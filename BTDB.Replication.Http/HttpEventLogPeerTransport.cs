using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Routing;

namespace BTDB.Replication.Http;

/// <summary>
/// Event-log peer calls over HTTP: POST {origin}/_btdb/eventlog/v1/submit, /bounds and /subscribe with a bearer API key
/// shared by the cluster. A subscription is one long response of length-prefixed messages, flushed per message with
/// response buffering disabled. Requests prefer HTTP/2 and fall back to HTTP/1.1.
/// </summary>
public sealed class HttpEventLogPeerTransport : IEventLogPeerTransport, IDisposable
{
    internal const string Path = "/_btdb/eventlog/v1";
    readonly HttpClient _client;
    readonly byte[] _apiKey;
    readonly string _apiKeyText;
    readonly int _maximumMessageBytes;

    public HttpEventLogPeerTransport(string apiKey, int maximumMessageBytes = 32 * 1024 * 1024)
    {
        ArgumentException.ThrowIfNullOrEmpty(apiKey);
        ArgumentOutOfRangeException.ThrowIfLessThan(maximumMessageBytes, 64 * 1024);
        _apiKeyText = apiKey;
        _apiKey = Encoding.UTF8.GetBytes(apiKey);
        _maximumMessageBytes = maximumMessageBytes;
        // Never forward a credential-bearing POST to a redirect target; callers supply deadlines.
        _client = new(new SocketsHttpHandler { AllowAutoRedirect = false, UseCookies = false })
        { Timeout = Timeout.InfiniteTimeSpan };
    }

    internal bool IsMapped { get; private set; }

    public IEndpointConventionBuilder Map(IEndpointRouteBuilder endpoints, IEventLogPeerHandler handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (IsMapped) throw new InvalidOperationException("The event log endpoint is already mapped.");
        var group = endpoints.MapGroup(Path);
        group.MapPost("/submit", context => HandleAsync(context, async (body, response) =>
        {
            var request = EventLogWire.DecodeSubmit(body);
            var result = await handler.SubmitAsync(request, context.RequestAborted).ConfigureAwait(false);
            await WriteAsync(response, EventLogWire.Encode(result), context.RequestAborted).ConfigureAwait(false);
        }));
        group.MapPost("/bounds", context => HandleAsync(context, async (body, response) =>
        {
            var (topic, _) = EventLogWire.DecodeTopic(body);
            var result = await handler.GetBoundsAsync(topic, context.RequestAborted).ConfigureAwait(false);
            await WriteAsync(response, EventLogWire.Encode(result), context.RequestAborted).ConfigureAwait(false);
        }));
        group.MapPost("/subscribe", context => HandleAsync(context, async (body, response) =>
        {
            var (topic, from) = EventLogWire.DecodeTopic(body);
            var messages = handler.SubscribeAsync(topic, from, context.RequestAborted);
            context.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();
            response.ContentType = "application/octet-stream";
            await response.StartAsync(context.RequestAborted).ConfigureAwait(false);
            await foreach (var message in messages.ConfigureAwait(false))
            {
                await response.Body.WriteAsync(EventLogWire.Encode(message), context.RequestAborted).ConfigureAwait(false);
                await response.Body.FlushAsync(context.RequestAborted).ConfigureAwait(false);
            }
        }));
        IsMapped = true;
        return group;
    }

    static async Task WriteAsync(HttpResponse response, byte[] bytes, CancellationToken cancellation)
    {
        response.ContentType = "application/octet-stream";
        response.ContentLength = bytes.Length;
        await response.Body.WriteAsync(bytes, cancellation).ConfigureAwait(false);
    }

    bool Authenticated(HttpContext context)
    {
        var authorization = context.Request.Headers.Authorization.ToString();
        if (!authorization.StartsWith("Bearer ", StringComparison.Ordinal)) return false;
        return CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(authorization[7..]), _apiKey);
    }

    async Task HandleAsync(HttpContext context, Func<byte[], HttpResponse, Task> handle)
    {
        context.Response.Headers.CacheControl = "no-store";
        try
        {
            if (!Authenticated(context))
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                return;
            }
            if (context.Request.ContentLength > _maximumMessageBytes)
            {
                context.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
                return;
            }
            var body = await ReadBoundedAsync(context.Request.Body, context.Request.ContentLength, _maximumMessageBytes,
                context.RequestAborted).ConfigureAwait(false);
            await handle(body, context.Response).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested) { context.Abort(); }
        catch (InvalidDataException) { Fail(StatusCodes.Status400BadRequest); }
        catch (ArgumentException) { Fail(StatusCodes.Status400BadRequest); }
        catch (EventLogRecordTooLargeException) { Fail(StatusCodes.Status413PayloadTooLarge); }
        catch (Exception) { Fail(StatusCodes.Status503ServiceUnavailable); }

        void Fail(int status)
        {
            if (context.Response.HasStarted) context.Abort();
            else
            {
                context.Response.Clear();
                context.Response.StatusCode = status;
            }
        }
    }

    static async Task<byte[]> ReadBoundedAsync(Stream stream, long? length, int maximum, CancellationToken cancellation)
    {
        if (length is { } declared)
        {
            if (declared < 0 || declared > maximum) throw new IOException("Event log message exceeds its size limit.");
            var exact = new byte[declared];
            await stream.ReadExactlyAsync(exact, cancellation).ConfigureAwait(false);
            return exact;
        }
        using var result = new MemoryStream();
        var buffer = new byte[81920];
        while (true)
        {
            var count = await stream.ReadAsync(buffer, cancellation).ConfigureAwait(false);
            if (count == 0) return result.ToArray();
            if (result.Length + count > maximum) throw new IOException("Event log message exceeds its size limit.");
            result.Write(buffer, 0, count);
        }
    }

    async Task<HttpResponseMessage> SendAsync(string endpoint, string operation, byte[] body, bool streaming,
        CancellationToken cancellation)
    {
        var target = new Uri(HttpReplicationPeerTransport.ValidateEndpoint(endpoint), Path + "/" + operation);
        using var request = new HttpRequestMessage(HttpMethod.Post, target)
        {
            Version = HttpVersion.Version20, VersionPolicy = HttpVersionPolicy.RequestVersionOrLower,
            Content = new ByteArrayContent(body)
        };
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _apiKeyText);
        request.Content.Headers.ContentType = new("application/octet-stream");
        HttpResponseMessage? response = null;
        try
        {
            response = await _client.SendAsync(request, streaming ? HttpCompletionOption.ResponseHeadersRead
                : HttpCompletionOption.ResponseContentRead, cancellation).ConfigureAwait(false);
            if (response.StatusCode != HttpStatusCode.OK)
                throw new IOException($"Event log peer request failed with HTTP {(int)response.StatusCode}.");
            return response;
        }
        catch (HttpRequestException error)
        {
            response?.Dispose();
            throw new IOException("Event log peer is unreachable.", error);
        }
        catch
        {
            response?.Dispose();
            throw;
        }
    }

    async Task<byte[]> CallAsync(string endpoint, string operation, byte[] body, CancellationToken cancellation)
    {
        using var response = await SendAsync(endpoint, operation, body, false, cancellation).ConfigureAwait(false);
        await using var stream = await response.Content.ReadAsStreamAsync(cancellation).ConfigureAwait(false);
        return await ReadBoundedAsync(stream, response.Content.Headers.ContentLength, _maximumMessageBytes, cancellation)
            .ConfigureAwait(false);
    }

    public async ValueTask<EventLogSubmitResponse> SubmitAsync(string endpoint, EventLogSubmitRequest request,
        CancellationToken cancellation)
    {
        var bytes = await CallAsync(endpoint, "submit", EventLogWire.Encode(request), cancellation).ConfigureAwait(false);
        return EventLogWire.DecodeSubmitResponse(bytes);
    }

    public async ValueTask<EventLogBoundsResponse> GetBoundsAsync(string endpoint, string topic,
        CancellationToken cancellation)
    {
        var bytes = await CallAsync(endpoint, "bounds", EventLogWire.EncodeTopic(topic), cancellation).ConfigureAwait(false);
        return EventLogWire.DecodeBounds(bytes);
    }

    public async IAsyncEnumerable<EventLogLiveMessage> SubscribeAsync(string endpoint, string topic, ulong from,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        using var response = await SendAsync(endpoint, "subscribe", EventLogWire.EncodeTopic(topic, from), true,
            cancellation).ConfigureAwait(false);
        await using var stream = await response.Content.ReadAsStreamAsync(cancellation).ConfigureAwait(false);
        var prefix = new byte[4];
        while (true)
        {
            var read = 0;
            while (read < 4)
            {
                var count = await stream.ReadAsync(prefix.AsMemory(read), cancellation).ConfigureAwait(false);
                if (count == 0)
                {
                    if (read == 0) yield break;
                    throw new IOException("Truncated event log live message.");
                }
                read += count;
            }
            var length = BinaryPrimitives.ReadUInt32LittleEndian(prefix);
            if (length > _maximumMessageBytes) throw new IOException("Event log live message exceeds its size limit.");
            var body = new byte[length];
            await stream.ReadExactlyAsync(body, cancellation).ConfigureAwait(false);
            EventLogLiveMessage message;
            try { message = EventLogWire.DecodeLive(body); }
            catch (InvalidDataException error) { throw new IOException("Invalid event log live message.", error); }
            yield return message;
        }
    }

    public void Dispose() => _client.Dispose();
}
