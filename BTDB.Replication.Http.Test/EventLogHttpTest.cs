using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace BTDB.Replication.Http.Test;

public class EventLogHttpTest
{
    const string Topic = "events";
    const string ApiKey = "cluster-secret";
    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    static EventLogOptions Options => new()
    {
        SplitCap = 4096, SealFreeSpace = 512, HeartbeatInterval = TimeSpan.FromMilliseconds(50),
        OwnerTimeout = TimeSpan.FromMilliseconds(400), ContentionBackoff = TimeSpan.FromMilliseconds(20)
    };

    static byte[] Bytes(int value) => BitConverter.GetBytes(value);

    static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    sealed class Node : IAsyncDisposable
    {
        WebApplication _app = null!;
        public string Endpoint = null!;
        public IEventLog Log => _app.Services.GetRequiredService<IEventLog>();
        public EventLogService Service => _app.Services.GetRequiredService<EventLogService>();

        public static async Task<Node> Start(IEventLogStorage storage)
        {
            var node = new Node { Endpoint = $"http://127.0.0.1:{FreePort()}" };
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseKestrel().UseUrls(node.Endpoint);
            builder.Services.AddSingleton(storage);
            builder.Services.AddBTDBEventLog(node.Endpoint, ApiKey, Options);
            node._app = builder.Build();
            node._app.MapBTDBEventLog();
            await node._app.StartAsync();
            return node;
        }

        public async ValueTask DisposeAsync() => await _app.DisposeAsync();
    }

    [Fact]
    public void WireMessagesRoundTripAndRejectMalformedInput()
    {
        var request = new EventLogSubmitRequest("t", 7, 3, 2, true, 11, [new byte[] { 1 }, Array.Empty<byte>()]);
        var decoded = EventLogWire.DecodeSubmit(EventLogWire.Encode(request));
        Assert.Equal((request.Topic, request.Session, request.FirstSequence, request.OldestUnresolved,
            request.PreviouslyDispatched, request.ResumeFrom), (decoded.Topic, decoded.Session, decoded.FirstSequence,
            decoded.OldestUnresolved, decoded.PreviouslyDispatched, decoded.ResumeFrom));
        Assert.Equal(new byte[] { 1 }, decoded.Records[0].ToArray());
        Assert.Empty(decoded.Records[1].ToArray());
        var response = EventLogWire.DecodeSubmitResponse(EventLogWire.Encode(
            new EventLogSubmitResponse(EventLogSubmitStatus.Committed, [5, 6], 7, null)));
        Assert.Equal([5ul, 6], response.Offsets);
        var bounds = EventLogWire.DecodeBounds(EventLogWire.Encode(new EventLogBoundsResponse(false, new(0, 9), "http://x")));
        Assert.Equal(("http://x", 9ul), (bounds.OwnerEndpoint, bounds.Bounds.Next));
        var live = EventLogWire.Encode(new EventLogLiveMessage(EventLogLiveKind.Batch, 4, [new byte[] { 9 }], "v", null));
        var message = EventLogWire.DecodeLive(live.AsSpan(4));
        Assert.Equal((EventLogLiveKind.Batch, 4ul, "v"), (message.Kind, message.Offset, message.TailVersion));
        var bytes = EventLogWire.Encode(request);
        for (var length = 0; length < bytes.Length; length++)
            Assert.Throws<InvalidDataException>(() => EventLogWire.DecodeSubmit(bytes.AsSpan(0, length)));
        Assert.Throws<InvalidDataException>(() => EventLogWire.DecodeSubmit([.. bytes, 0]));
        bytes[0] = 2;
        Assert.Throws<InvalidDataException>(() => EventLogWire.DecodeSubmit(bytes));
    }

    [Fact]
    public async Task NodesPublishAndFollowOverHttp()
    {
        var storage = new InMemoryEventLogStorage();
        await using var a = await Node.Start(storage);
        await using var b = await Node.Start(storage);
        await using var c = await Node.Start(storage);
        Assert.Equal(0ul, await a.Log.GetTopic(Topic).PublishAsync(Bytes(0)).AsTask().WaitAsync(Timeout));
        using var cancel = new CancellationTokenSource(Timeout);
        var received = new List<(ulong, int)>();
        var reading = Task.Run(async () =>
        {
            await foreach (var record in c.Log.GetTopic(Topic).ReadAsync(0, 30, cancel.Token))
                received.Add((record.Offset, BitConverter.ToInt32(record.Payload.Span)));
        });
        var offsets = await Task.WhenAll(Enumerable.Range(1, 29)
            .Select(i => (i % 2 == 0 ? a : b).Log.GetTopic(Topic).PublishAsync(Bytes(i)).AsTask())).WaitAsync(Timeout);
        Assert.Equal(Enumerable.Range(1, 29).Select(i => (ulong)i), offsets.OrderBy(o => o));
        await reading.WaitAsync(Timeout);
        Assert.Equal(Enumerable.Range(0, 30).Select(i => (ulong)i), received.Select(r => r.Item1));
        Assert.True(a.Service.IsOwner(Topic));
        Assert.Equal(new EventLogBounds(0, 30), await b.Log.GetTopic(Topic).GetBoundsAsync());
    }

    [Fact]
    public async Task UnauthenticatedPeerRequestsAreRejected()
    {
        await using var a = await Node.Start(new InMemoryEventLogStorage());
        using var client = new HttpClient();
        using var response = await client.PostAsync(a.Endpoint + "/_btdb/eventlog/v1/bounds",
            new ByteArrayContent(EventLogWire.EncodeTopic(Topic)));
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        using var wrong = new HttpEventLogPeerTransport("wrong");
        await Assert.ThrowsAsync<IOException>(async () => await wrong.GetBoundsAsync(a.Endpoint, Topic, default));
    }

    [Fact]
    public async Task StoppedOwnerIsTakenOverOverHttp()
    {
        var storage = new InMemoryEventLogStorage();
        var a = await Node.Start(storage);
        await using var b = await Node.Start(storage);
        for (var i = 0; i < 3; i++) await a.Log.GetTopic(Topic).PublishAsync(Bytes(i)).AsTask().WaitAsync(Timeout);
        Assert.Equal(3ul, await b.Log.GetTopic(Topic).PublishAsync(Bytes(3)).AsTask().WaitAsync(Timeout));
        await a.DisposeAsync();
        Assert.Equal(4ul, await b.Log.GetTopic(Topic).PublishAsync(Bytes(4)).AsTask().WaitAsync(Timeout));
        Assert.True(b.Service.IsOwner(Topic));
        var records = new List<int>();
        await foreach (var record in b.Log.GetTopic(Topic).ReadAsync(0, 5))
            records.Add(BitConverter.ToInt32(record.Payload.Span));
        Assert.Equal([0, 1, 2, 3, 4], records);
    }
}
