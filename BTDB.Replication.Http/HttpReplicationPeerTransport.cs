using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BTDB.Replication.Http;

/// <summary>HTTP boundary only: the coordinator still authenticates the selected identity and owns all authority,
/// comparison and handoff decisions. Each request reopens that exact leader session, without a server session table.</summary>
internal sealed class HttpReplicationPeerTransport : IReplicationPeerTransport, IDisposable
{
    internal const string Path = "/_btdb/replication";
    internal const int MaximumRange = 256 * 1024;
    internal const int MaximumControlBytes = 16 * 1024;
    readonly HttpClient _client;
    readonly SemaphoreSlim _requests;
    Registration? _listener;

    public HttpReplicationPeerTransport(int maximumConcurrentRequests = 32)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumConcurrentRequests);
        _requests = new(maximumConcurrentRequests);
        // Never forward a credential-bearing POST to a redirect target. The coordinator supplies the deadline.
        _client = new(new SocketsHttpHandler { AllowAutoRedirect = false, UseCookies = false })
        { Timeout = Timeout.InfiniteTimeSpan };
    }

    internal bool IsMapped { get; private set; }

    public IEndpointConventionBuilder Map(IEndpointRouteBuilder endpoints)
    {
        if (IsMapped) throw new InvalidOperationException("The replication endpoint is already mapped.");
        var mapping = endpoints.MapPost(Path, HandleAsync);
        IsMapped = true;
        return mapping;
    }

    public IDisposable Listen(string endpoint, Func<ReplicationPeerIdentity, IReplicationPeerSession> accept)
    {
        ValidateEndpoint(endpoint);
        var registration = new Registration(this, endpoint, accept);
        if (Interlocked.CompareExchange(ref _listener, registration, null) != null)
            throw new InvalidOperationException("A peer listener is already registered.");
        return registration;
    }

    public async ValueTask<IReplicationPeerSession> ConnectAsync(ReplicationPeerIdentity identity, CancellationToken cancellation)
    {
        ValidateEndpoint(identity.Endpoint);
        var session = new Session(_client, identity);
        try
        {
            await session.ConnectAsync(cancellation).ConfigureAwait(false);
            return session;
        }
        catch { session.Dispose(); throw; }
    }

    internal static Uri ValidateEndpoint(string endpoint)
    {
        if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) ||
            (uri.Scheme != "https" && (uri.Scheme != "http" || !uri.IsLoopback)) ||
            uri.UserInfo.Length != 0 || uri.Query.Length != 0 || uri.Fragment.Length != 0 || uri.AbsolutePath != "/")
            throw new ArgumentException("Peer endpoint must be an HTTPS origin (HTTP is allowed only on loopback).");
        return uri;
    }

    async Task HandleAsync(HttpContext context)
    {
        if (!await _requests.WaitAsync(0, context.RequestAborted).ConfigureAwait(false))
        { context.Response.StatusCode = StatusCodes.Status429TooManyRequests; return; }
        try
        {
            context.Response.Headers.CacheControl = "no-store";
            var listener = Volatile.Read(ref _listener);
            if (listener == null) { context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable; return; }
            var authorization = context.Request.Headers.Authorization.ToString();
            if (!authorization.StartsWith("Bearer ", StringComparison.Ordinal) || authorization.Length <= 7)
            { context.Response.StatusCode = StatusCodes.Status401Unauthorized; return; }
            if (context.Request.ContentLength > MaximumControlBytes)
            { context.Response.StatusCode = StatusCodes.Status413PayloadTooLarge; return; }
            var bytes = await ReadBoundedAsync(context.Request.Body, context.Request.ContentLength, MaximumControlBytes, context.RequestAborted).ConfigureAwait(false);
            PeerRequest request;
            try { request = ReplicationPeerWire.DecodeRequest(bytes); }
            catch (InvalidDataException) { context.Response.StatusCode = StatusCodes.Status400BadRequest; return; }
            if (request.Endpoint != listener.Endpoint || string.IsNullOrEmpty(request.ClusterId) ||
                string.IsNullOrEmpty(request.SessionId) || request.Term == 0)
            { context.Response.StatusCode = StatusCodes.Status400BadRequest; return; }
            var identity = new ReplicationPeerIdentity(request.ClusterId, request.Term, request.SessionId,
                request.Endpoint, authorization[7..]);
            IReplicationPeerSession session;
            try { session = listener.Accept(identity); }
            catch (IOException) { context.Response.StatusCode = StatusCodes.Status401Unauthorized; return; }
            using (session)
            {
                var cancellation = context.RequestAborted;
                switch (request.Operation)
                {
                    case PeerOperation.Connect:
                        // Accept just authenticated it; only async operations need to reauthenticate.
                        context.Response.StatusCode = StatusCodes.Status204NoContent;
                        break;
                    case PeerOperation.Poll:
                        if (request.DurationTicks <= 0) throw new ArgumentException("Invalid grant duration.");
                        if (request.Databases is not { } polled || Array.Exists(polled, d => string.IsNullOrEmpty(d.Database)))
                            throw new ArgumentException("Invalid polled databases.");
                        var poll = await session.PollAsync(polled, request.Challenge,
                            TimeSpan.FromTicks(request.DurationTicks), request.InlineBudget, cancellation).ConfigureAwait(false);
                        CheckListener(listener, identity, cancellation);
                        var segments = ReplicationPeerWire.EncodePollSegments(poll);
                        context.Response.ContentType = "application/octet-stream";
                        context.Response.ContentLength = segments.Sum(segment => (long)segment.Length);
                        foreach (var segment in segments)
                            await context.Response.Body.WriteAsync(segment, cancellation).ConfigureAwait(false);
                        break;
                    case PeerOperation.Read:
                        if (string.IsNullOrEmpty(request.Database) || request.FileId == 0 ||
                            request.Count is <= 0 or > MaximumRange)
                            throw new ArgumentException("Invalid TRL range.");
                        var buffer = ArrayPool<byte>.Shared.Rent(request.Count);
                        try
                        {
                            var count = await session.Reader(request.Database).ReadAsync(request.FileId, request.Offset,
                                buffer.AsMemory(0, request.Count), cancellation).ConfigureAwait(false);
                            if ((uint)count > (uint)request.Count) throw new IOException("Invalid TRL response length.");
                            CheckListener(listener, identity, cancellation);
                            context.Response.ContentType = "application/octet-stream";
                            context.Response.ContentLength = count;
                            await context.Response.Body.WriteAsync(buffer.AsMemory(0, count), cancellation).ConfigureAwait(false);
                        }
                        finally { ArrayPool<byte>.Shared.Return(buffer); }
                        break;
                    case PeerOperation.Handoff:
                        if (request.Handoff == null || !Guid.TryParse(request.Handoff.TransferId, out _))
                            throw new ArgumentException("Invalid prepared handoff.");
                        await session.OfferHandoffAsync(request.Handoff, cancellation).ConfigureAwait(false);
                        CheckListener(listener, identity, cancellation);
                        context.Response.StatusCode = StatusCodes.Status204NoContent;
                        break;
                    default:
                        context.Response.StatusCode = StatusCodes.Status400BadRequest;
                        break;
                }
            }
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested) { context.Abort(); }
        catch (FileNotFoundException) { Fail(StatusCodes.Status410Gone); }
        catch (ArgumentException) { Fail(StatusCodes.Status400BadRequest); }
        catch (IOException) { Fail(StatusCodes.Status503ServiceUnavailable); }
        finally { _requests.Release(); }

        void Fail(int status)
        {
            if (context.Response.HasStarted) context.Abort();
            else { context.Response.Clear(); context.Response.StatusCode = status; }
        }
    }

    // An async provider may finish after fencing or replacement. Reauthenticate before emitting its result.
    void CheckListener(Registration listener, ReplicationPeerIdentity identity, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        if (!ReferenceEquals(listener, Volatile.Read(ref _listener))) throw new IOException("Peer listener was replaced.");
        using var current = listener.Accept(identity);
    }

    // A declared length is read straight into an exact array; otherwise the body is buffered up to the limit.
    static async Task<byte[]> ReadBoundedAsync(Stream stream, long? length, int maximum, CancellationToken cancellation)
    {
        if (length is { } declared)
        {
            if (declared < 0 || declared > maximum) throw new IOException("Peer message exceeds its size limit.");
            var exact = new byte[declared];
            await stream.ReadExactlyAsync(exact, cancellation).ConfigureAwait(false);
            return exact;
        }
        using var result = new MemoryStream();
        var buffer = ArrayPool<byte>.Shared.Rent(Math.Min(maximum + 1, 4096));
        try
        {
            while (true)
            {
                var count = await stream.ReadAsync(buffer.AsMemory(0, Math.Min(buffer.Length, maximum + 1 - (int)result.Length)), cancellation)
                    .ConfigureAwait(false);
                if (count == 0) return result.ToArray();
                if (result.Length + count > maximum) throw new IOException("Peer message exceeds its size limit.");
                result.Write(buffer, 0, count);
            }
        }
        finally { ArrayPool<byte>.Shared.Return(buffer); }
    }


    sealed class Registration(HttpReplicationPeerTransport owner, string endpoint,
        Func<ReplicationPeerIdentity, IReplicationPeerSession> accept) : IDisposable
    {
        public string Endpoint => endpoint;
        public IReplicationPeerSession Accept(ReplicationPeerIdentity identity) => accept(identity);
        public void Dispose() => Interlocked.CompareExchange(ref owner._listener, null, this);
    }

    sealed class Session(HttpClient client, ReplicationPeerIdentity identity) : IReplicationPeerSession
    {
        readonly CancellationTokenSource _closed = new();
        readonly Uri _target = new(ValidateEndpoint(identity.Endpoint), Path);
        PeerRequest Message(PeerOperation operation) => new(identity.ClusterId, identity.Term, identity.SessionId, identity.Endpoint, operation);

        public async ValueTask ConnectAsync(CancellationToken cancellation)
        {
            using var response = await SendAsync(Message(PeerOperation.Connect), cancellation).ConfigureAwait(false);
            RequireStatus(response, HttpStatusCode.NoContent);
        }

        public async ValueTask<ReplicationPeerPoll> PollAsync(IReadOnlyList<ReplicationPeerPollRequest> databases,
            long challenge, TimeSpan duration, int inlineBudget, CancellationToken cancellation)
        {
            inlineBudget = Math.Clamp(inlineBudget, 0, ReplicationPeerPoll.MaximumInlineBytes);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, _closed.Token);
            using var response = await SendAsync(Message(PeerOperation.Poll) with
            {
                Databases = [.. databases], Challenge = challenge, DurationTicks = duration.Ticks, InlineBudget = inlineBudget
            }, linked.Token).ConfigureAwait(false);
            RequireStatus(response, HttpStatusCode.OK);
            // Progress stays within the control limit; inline TRL bytes add at most the requested budget.
            var bytes = await ReadBoundedAsync(await response.Content.ReadAsStreamAsync(linked.Token).ConfigureAwait(false),
                response.Content.Headers.ContentLength, MaximumControlBytes + inlineBudget, linked.Token).ConfigureAwait(false);
            linked.Token.ThrowIfCancellationRequested();
            ReplicationPeerPoll result;
            try { result = ReplicationPeerWire.DecodePoll(bytes, databases); }
            catch (InvalidDataException error) { throw new IOException("Invalid peer progress.", error); }
            result.Validate(challenge, databases, inlineBudget);
            return result;
        }

        public ILeaderTrlReader Reader(string database) => new ReaderProxy(this, database);

        public async ValueTask OfferHandoffAsync(PreparedHandoff offer, CancellationToken cancellation)
        {
            using var response = await SendAsync(Message(PeerOperation.Handoff) with { Handoff = offer }, cancellation).ConfigureAwait(false);
            RequireStatus(response, HttpStatusCode.NoContent);
        }

        async ValueTask<HttpResponseMessage> SendAsync(PeerRequest message, CancellationToken cancellation)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, _closed.Token);
            linked.Token.ThrowIfCancellationRequested();
            using var request = new HttpRequestMessage(HttpMethod.Post, _target);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", identity.ApiKey);
            request.Content = new ByteArrayContent(ReplicationPeerWire.EncodeRequest(message));
            request.Content.Headers.ContentType = new("application/octet-stream");
            HttpResponseMessage? response = null;
            try
            {
                response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, linked.Token).ConfigureAwait(false);
                linked.Token.ThrowIfCancellationRequested();
                if (response.StatusCode == HttpStatusCode.Gone) throw new FileNotFoundException("Leader no longer retains the requested TRL.");
                if (!response.IsSuccessStatusCode) throw new IOException($"Peer request failed with HTTP {(int)response.StatusCode}.");
                return response;
            }
            catch (HttpRequestException error)
            { response?.Dispose(); throw new IOException("Peer transport is unavailable.", error); }
            catch { response?.Dispose(); throw; }
        }

        static void RequireStatus(HttpResponseMessage response, HttpStatusCode expected)
        {
            if (response.StatusCode != expected) throw new IOException("Unexpected peer response status.");
        }

        public void Dispose() => _closed.Cancel();

        sealed class ReaderProxy(Session session, string database) : ILeaderTrlReader
        {
            public async ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination, CancellationToken cancellation)
            {
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, session._closed.Token);
                linked.Token.ThrowIfCancellationRequested();
                if (destination.IsEmpty) return 0;
                var count = Math.Min(destination.Length, MaximumRange);
                using var response = await session.SendAsync(session.Message(PeerOperation.Read) with
                { Database = database, FileId = fileId, Offset = offset, Count = count }, linked.Token).ConfigureAwait(false);
                RequireStatus(response, HttpStatusCode.OK);
                var length = response.Content.Headers.ContentLength;
                if (length == null || length < 0 || length > count) throw new IOException("Invalid TRL response length.");
                var stream = await response.Content.ReadAsStreamAsync(linked.Token).ConfigureAwait(false);
                await stream.ReadExactlyAsync(destination[..(int)length.Value], linked.Token).ConfigureAwait(false);
                linked.Token.ThrowIfCancellationRequested();
                return (int)length.Value;
            }
        }
    }

    public void Dispose()
    {
        Interlocked.Exchange(ref _listener, null);
        _client.Dispose();
    }
}
