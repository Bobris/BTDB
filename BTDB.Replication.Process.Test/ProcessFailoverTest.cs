using System;
using System.Diagnostics;
using System.Collections.Generic;
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
        bool _suspended;
        int _nodePid;
        // Durable node-local files survive process death; the harness deletes them after the node exits.
        public readonly string DataDirectory;
        readonly bool _ownsDataDirectory;

        Node(ChildProcess process, string dataDirectory, bool ownsDataDirectory)
        {
            _process = process;
            DataDirectory = dataDirectory;
            _ownsDataDirectory = ownsDataDirectory;
            _nodePid = process.Id;
            _output = Drain(process.StandardOutput, true);
            _error = Drain(process.StandardError, false);
        }
        async Task Drain(StreamReader reader, bool output)
        {
            while (await reader.ReadLineAsync() is { } line)
            {
                if (output && line.StartsWith("PID ", StringComparison.Ordinal)) _nodePid = int.Parse(line[4..]);
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

        public static async Task<Node> Start(BlobContainerClient container, int? progressTimeoutMilliseconds = null,
            string? dataDirectory = null)
        {
            var ownsDataDirectory = dataDirectory == null;
            dataDirectory ??= Path.Combine(Path.GetTempPath(), "btdb-process-node-" + Guid.NewGuid().ToString("N"));
            var runtime = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet";
            var start = new ProcessStartInfo(OperatingSystem.IsWindows() ? runtime : "/bin/sh")
            { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
            if (!OperatingSystem.IsWindows())
            {
                // Keep STOP notifications away from .NET's direct-child reaper. On macOS the runtime
                // spins in waitid/waitpid on a stopped direct child and blocks other process waits.
                start.ArgumentList.Add("-c");
                start.ArgumentList.Add("\"$@\" & child=$!; echo PID $child; wait \"$child\"");
                start.ArgumentList.Add("btdb-node");
                start.ArgumentList.Add(runtime);
            }
            start.ArgumentList.Add(typeof(Program).Assembly.Location);
            start.ArgumentList.Add("--node");
            start.ArgumentList.Add(container.Uri.ToString());
            if (progressTimeoutMilliseconds is { } timeout)
                start.Environment["BTDB_TEST_PROGRESS_TIMEOUT_MILLISECONDS"] = timeout.ToString();
            else start.Environment.Remove("BTDB_TEST_PROGRESS_TIMEOUT_MILLISECONDS");
            start.Environment["BTDB_TEST_DATA_DIRECTORY"] = dataDirectory;
            var node = new Node(ChildProcess.Start(start)!, dataDirectory, ownsDataDirectory);
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
        public async Task ExpectDivergenceExit() => await ExpectRestartExit("Follower native history diverged");

        public async Task ExpectRestartExit(string reason)
        {
            await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            await Task.WhenAll(_output, _error);
            Assert.Equal(0, _process.ExitCode);
            Assert.Contains("RESTART " + reason, Diagnostics);
        }
        public async Task ExpectFatalExit()
        {
            await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
            await Task.WhenAll(_output, _error);
            Assert.Equal(75, _process.ExitCode);
            Assert.Contains("FATAL Replication publication made no forward progress.", Diagnostics);
        }

        public async Task Signal(string signal)
        {
            var start = new ProcessStartInfo("kill") { UseShellExecute = false };
            start.ArgumentList.Add(signal);
            start.ArgumentList.Add(_nodePid.ToString());
            using var command = ChildProcess.Start(start)!;
            await command.WaitForExitAsync();
            Assert.Equal(0, command.ExitCode);
            _suspended = signal == "-STOP";
        }

        public async Task Kill()
        {
            if (!_process.HasExited && _suspended) await Signal("-CONT");
            if (!_process.HasExited) _process.Kill(true);
            await _process.WaitForExitAsync();
        }
        public async ValueTask DisposeAsync()
        {
            await Kill();
            await Task.WhenAll(_output, _error);
            _process.Dispose();
            if (_ownsDataDirectory) await DeleteDirectory(DataDirectory);
            _client.Dispose();
        }
    }

    // Windows may release a killed process's memory-mapped file handles (or an antivirus scan) shortly after exit.
    static async Task DeleteDirectory(string directory)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                if (Directory.Exists(directory)) Directory.Delete(directory, true);
                return;
            }
            catch (Exception error) when (error is IOException or UnauthorizedAccessException && attempt < 50)
            {
                await Task.Delay(100);
            }
        }
    }

    static bool Compared(NodeStatus state, ulong id) => state.EventId == id && state.CompletedFile != 0 &&
        state.CompletedFile == state.ComparedFile && state.CompletedOffset == state.ComparedOffset;

    static async Task<ulong> PublishedEvent(BlobContainerClient container)
    {
        var storage = new AzureReplicationStorage(container, "main");
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
    public async Task CrashedFollowerRestartsFromItsOwnDiskStorage()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Node.Start(container);
        await leader.Wait(s => s.Role == "Leader", "initial leader");
        await leader.Apply(1, 11);
        await WaitPublished(container, 1);
        var directory = Path.Combine(Path.GetTempPath(), "btdb-process-node-" + Guid.NewGuid().ToString("N"));
        try
        {
            await using (var follower = await Node.Start(container, dataDirectory: directory))
            {
                await follower.Wait(s => s.Role == "Follower" && s.EventId == 1, "restored follower");
                await leader.Apply(2, 22);
                await follower.Apply(2, 22);
                await follower.Wait(s => Compared(s, 2), "comparison before the crash");
                await follower.Kill(); // No graceful shutdown: mapped files keep their preallocated length.
            }
            Assert.NotEmpty(Directory.EnumerateFiles(directory));
            await using var restarted = await Node.Start(container, dataDirectory: directory);
            var restored = await restarted.Wait(s => s.Role == "Follower" && s.EventId == 2, "restart from existing local files");
            Assert.Equal(22, restored.Value);
            Assert.Equal(0, restored.Applied); // Canonical restore, not replay of the crashed local tail.
            await leader.Apply(3, 33);
            await restarted.Apply(3, 33);
            var continued = await restarted.Wait(s => Compared(s, 3), "comparison after restart");
            Assert.Equal(33, continued.Value);
        }
        finally
        {
            await DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task StalledPublicationTerminatesTheLeaderAndFollowerRecoversItsOptimisticTail()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Node.Start(container, progressTimeoutMilliseconds: 2000);
        await leader.Wait(s => s.Role == "Leader", "initial leader");
        await leader.Apply(1, 11);
        await WaitPublished(container, 1);
        await using var follower = await Node.Start(container);
        await follower.Wait(s => s.Role == "Follower" && s.EventId == 1, "restored follower");
        await leader.PausePublication();
        await leader.Apply(2, 22);
        await follower.Apply(2, 22);
        await leader.ExpectFatalExit();
        var promoted = await follower.Wait(s => s.Role == "Leader" && s.EventId == 2, "takeover after fatal watchdog", 35);
        Assert.Equal(1, promoted.Applied);
        Assert.Equal(22, promoted.Value);
        await WaitPublished(container, 2);
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

    public static IEnumerable<object[]> UnavailableLeaderModes()
    {
        yield return new object[] { false };
        if (!OperatingSystem.IsWindows()) yield return new object[] { true };
    }

    [Theory]
    [MemberData(nameof(UnavailableLeaderModes))]
    public async Task UnavailableLeaderIsReplacedAndItsUnpublishedTailSurvivesWithoutReexecution(bool suspend)
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

        // No graceful release, test lease break or simulated timer advancement. STOP suspends all node threads.
        if (suspend) await leader.Signal("-STOP");
        else await leader.Kill();
        var promoted = await follower.Wait(s => s.Role == "Leader" && s.EventId == 2, "lease expiry and takeover", 35);
        Assert.Equal(1, promoted.Applied);
        Assert.Equal(22, promoted.Value);
        await WaitPublished(container, 2);
        var record = await container.GetBlobClient("leader.json").DownloadContentAsync();
        var json = System.Text.Json.Nodes.JsonNode.Parse(record.Value.Content.ToString())!;
        Assert.True(json["term"]!.GetValue<ulong>() >= 2);
        Assert.Equal(follower.Endpoint, json["peerEndpoint"]!.GetValue<string>());

        if (suspend)
        {
            await leader.Signal("-CONT");
            // This node created genesis rather than restoring a base; its first publication is its canonical base.
            // After losing authority it follows the new leader and confirms its own unpublished tail, without a restore.
            var resumed = await leader.Wait(s => s.Role == "Follower" && Compared(s, 2), "resumed old leader follows");
            Assert.Equal(22, resumed.Value);
        }

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
