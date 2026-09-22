using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using BTDB.KVDBLayer;
using BTDB.Replication.Azure;
using BTDB.Replication.Azure.Test;
using Xunit;
using ChildProcess = System.Diagnostics.Process;

namespace BTDB.Replication.ProcessTests;

public class ProcessFailoverTest(AzuriteFixture fixture) : IClassFixture<AzuriteFixture>
{
    sealed class Node : IAsyncDisposable
    {
        readonly ChildProcess _process;
        readonly Task _output, _error;
        readonly StringBuilder _diagnostics = new();
        readonly HttpClient _client = new() { Timeout = TimeSpan.FromSeconds(5) };
        readonly TaskCompletionSource<string> _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public string Endpoint = "";

        Node(ChildProcess process)
        {
            _process = process;
            _output = Drain(process.StandardOutput, true);
            _error = Drain(process.StandardError, false);
        }
        async Task Drain(StreamReader reader, bool output)
        {
            while (await reader.ReadLineAsync() is { } line)
            {
                if (output && line.StartsWith("READY ", StringComparison.Ordinal)) _ready.TrySetResult(line[6..]);
                // Keep process diagnostics bounded, including on unexpected worker termination.
                lock (_diagnostics)
                {
                    _diagnostics.AppendLine(line);
                    if (_diagnostics.Length > 16 * 1024) _diagnostics.Remove(0, _diagnostics.Length - 16 * 1024);
                }
            }
            if (output) _ready.TrySetException(new IOException("Node exited before startup: " + Diagnostics));
        }
        string Diagnostics { get { lock (_diagnostics) return _diagnostics.ToString(); } }

        public static async Task<Node> Start(BlobContainerClient container)
        {
            var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
            { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
            start.ArgumentList.Add(typeof(Program).Assembly.Location);
            start.ArgumentList.Add("--node");
            start.ArgumentList.Add(container.Uri.ToString());
            var node = new Node(ChildProcess.Start(start)!);
            try
            {
                node.Endpoint = await node._ready.Task.WaitAsync(TimeSpan.FromSeconds(20));
                return node;
            }
            catch { await node.DisposeAsync(); throw; }
        }

        public async Task<NodeStatus> Wait(Func<NodeStatus, bool> ready, string description, int seconds = 20)
        {
            var deadline = Stopwatch.StartNew();
            NodeStatus? last = null;
            while (deadline.Elapsed < TimeSpan.FromSeconds(seconds))
            {
                if (_process.HasExited) throw new IOException($"Node exited with {_process.ExitCode}: {Diagnostics}");
                try
                {
                    last = await _client.GetFromJsonAsync<NodeStatus>(Endpoint + "/test/state");
                    if (last != null && ready(last)) return last;
                }
                catch (HttpRequestException) { }
                await Task.Delay(50);
            }
            throw new TimeoutException($"Waiting for {description}; last state: {last}; {Diagnostics}");
        }
        public async Task Apply(ulong id, byte value)
        {
            using var reply = await _client.PostAsync($"{Endpoint}/test/apply/{id}/{value}", null);
            reply.EnsureSuccessStatusCode();
        }
        public async Task PausePublication()
        {
            using var reply = await _client.PostAsync(Endpoint + "/test/pause-publication", null);
            reply.EnsureSuccessStatusCode();
        }
        public async Task ExpectDivergenceExit()
        {
            await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            await Task.WhenAll(_output, _error);
            Assert.Equal(0, _process.ExitCode);
            Assert.Contains("RESTART Follower native history diverged", Diagnostics);
        }
        public async Task Kill()
        {
            if (!_process.HasExited) _process.Kill(true);
            await _process.WaitForExitAsync();
        }
        public async ValueTask DisposeAsync()
        {
            await Kill();
            await Task.WhenAll(_output, _error);
            _process.Dispose();
            _client.Dispose();
        }
    }

    static bool Compared(NodeStatus state, ulong id) => state.EventId == id && state.CompletedFile != 0 &&
        state.CompletedFile == state.AcknowledgedFile && state.CompletedOffset == state.AcknowledgedOffset;

    static async Task<ulong> PublishedEvent(BlobContainerClient container)
    {
        var storage = new AzureCanonicalTrlStorage(container, "main");
        var inventory = await CanonicalTrlInventory.DiscoverAsync(storage, TestNodeHost.Genesis);
        using var files = new InMemoryReplicationFileStorage();
        await using var collection = new ReplicationFileSet(files, inventory);
        await collection.InitializeAsync();
        using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        { FileCollection = collection, Compression = new NoCompressionStrategy(), CompactorScheduler = null });
        using var read = db.StartReadOnlyTransaction();
        return read.GetCommitUlong();
    }

    static async Task WaitPublished(BlobContainerClient container, ulong id)
    {
        var deadline = Stopwatch.StartNew();
        while (deadline.Elapsed < TimeSpan.FromSeconds(20))
        {
            try { if (await PublishedEvent(container) == id) return; }
            catch (IOException) { } // A racing version change requires a fresh canonical discovery.
            await Task.Delay(50);
        }
        throw new TimeoutException($"Canonical restore did not reach event {id}.");
    }

    [Fact]
    public async Task DivergentFollowerTerminatesItsProcessWithoutPublishingItsLocalOutcome()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Node.Start(container);
        await leader.Wait(s => s.Role == "Leader", "initial leader");
        await leader.Apply(1, 11);
        await WaitPublished(container, 1);
        await using var follower = await Node.Start(container);
        await follower.Wait(s => s.Role == "Follower" && s.EventId == 1, "restored follower");
        await leader.PausePublication();
        await leader.Apply(2, 22);
        await follower.Apply(2, 99);
        await follower.ExpectDivergenceExit();
        var surviving = await leader.Wait(s => s.Role == "Leader" && s.EventId == 2, "unaffected leader");
        Assert.Equal(22, surviving.Value);
        Assert.Equal(1ul, await PublishedEvent(container));
    }

    [Fact]
    public async Task KilledLeaderIsReplacedAndItsUnpublishedTailSurvivesWithoutReexecution()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Node.Start(container);
        await leader.Wait(s => s.Role == "Leader", "initial leader");
        await leader.Apply(1, 11);
        await WaitPublished(container, 1);

        await using var follower = await Node.Start(container);
        var restored = await follower.Wait(s => s.Role == "Follower" && s.EventId == 1, "restored follower");
        Assert.Equal(11, restored.Value);
        Assert.Equal(0, restored.Applied);

        await leader.PausePublication();
        await leader.Apply(2, 22);
        await follower.Apply(2, 22);
        var compared = await follower.Wait(s => s.Role == "Follower" && Compared(s, 2), "peer comparison of unpublished tail");
        Assert.Equal(1, compared.Applied);
        Assert.Equal(1ul, await PublishedEvent(container));

        // Real process death: no graceful lease release, test lease break or simulated timer advancement.
        await leader.Kill();
        var promoted = await follower.Wait(s => s.Role == "Leader" && s.EventId == 2, "lease expiry and takeover", 35);
        Assert.Equal(1, promoted.Applied);
        Assert.Equal(22, promoted.Value);
        await WaitPublished(container, 2);
        var record = await container.GetBlobClient("leader.json").DownloadContentAsync();
        var json = System.Text.Json.Nodes.JsonNode.Parse(record.Value.Content.ToString())!;
        Assert.True(json["term"]!.GetValue<ulong>() >= 2);
        Assert.Equal(follower.Endpoint, json["peerEndpoint"]!.GetValue<string>());

        // A third OS process has no prior local cache; verify takeover history by ordinary native restore.
        await using var replacement = await Node.Start(container);
        var cold = await replacement.Wait(s => s.Role == "Follower" && s.EventId == 2, "cold restored replacement");
        Assert.Equal(22, cold.Value);
        Assert.Equal(0, cold.Applied);
        await follower.Apply(3, 33);
        await replacement.Apply(3, 33);
        var continued = await replacement.Wait(s => Compared(s, 3), "comparison after takeover");
        Assert.Equal(33, continued.Value);
        Assert.Equal(1, continued.Applied);
        await WaitPublished(container, 3);
    }
}
