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

public class ProcessFailoverTest(BlobStorageFixture fixture) : IClassFixture<BlobStorageFixture>
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
            string? dataDirectory = null, ulong generation = 1, int readDelayMilliseconds = 0, string databases = "main",
            bool objects = false, int? activationTimeoutMilliseconds = null)
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
            if (activationTimeoutMilliseconds is { } activation)
                start.Environment["BTDB_TEST_ACTIVATION_TIMEOUT_MILLISECONDS"] = activation.ToString();
            else start.Environment.Remove("BTDB_TEST_ACTIVATION_TIMEOUT_MILLISECONDS");
            start.Environment["BTDB_TEST_DATA_DIRECTORY"] = dataDirectory;
            start.Environment["BTDB_TEST_GENERATION"] = generation.ToString();
            start.Environment["BTDB_TEST_READ_DELAY_MILLISECONDS"] = readDelayMilliseconds.ToString();
            start.Environment["BTDB_TEST_DATABASES"] = databases;
            start.Environment["BTDB_TEST_APPLICATION"] = objects ? "objectdb" : "kv";
            var node = new Node(ChildProcess.Start(start)!, dataDirectory, ownsDataDirectory);
            try
            {
                node.Endpoint = await node._ready.Task.WaitAsync(TimeSpan.FromSeconds(20));
                return node;
            }
            catch { await node.DisposeAsync(); throw; }
        }

        public async Task<NodeStatus> Wait(Func<NodeStatus, bool> ready, string description, int seconds = 20,
            string database = "main")
        {
            var deadline = Stopwatch.StartNew();
            NodeStatus? last = null;
            while (deadline.Elapsed < TimeSpan.FromSeconds(seconds))
            {
                if (_process.HasExited) throw new IOException($"Node exited with {_process.ExitCode}: {Diagnostics}");
                try
                {
                    last = await _client.GetFromJsonAsync<NodeStatus>($"{Endpoint}/test/state?db={database}");
                    if (last != null && ready(last)) return last;
                }
                catch (HttpRequestException) { }
                await Task.Delay(50);
            }
            throw new TimeoutException($"Waiting for {description}; last state: {last}; {Diagnostics}");
        }
        public async Task Apply(ulong id, byte value, int kilobytes = 0, string database = "main")
        {
            using var reply = await _client.PostAsync($"{Endpoint}/test/apply/{id}/{value}?kb={kilobytes}&db={database}", null);
            reply.EnsureSuccessStatusCode();
        }
        public async Task Orders(ulong from, int count)
        {
            using var reply = await _client.PostAsync($"{Endpoint}/test/orders/{from}/{count}", null);
            reply.EnsureSuccessStatusCode();
        }
        public async Task<OrderSummary> OrderSummary() =>
            (await _client.GetFromJsonAsync<OrderSummary>(Endpoint + "/test/orders"))!;
        public async Task Partition(string target, bool enabled)
        {
            using var reply = await _client.PostAsync($"{Endpoint}/test/partition/{target}/{enabled}", null);
            reply.EnsureSuccessStatusCode();
        }
        public async Task PrepareUpgrade()
        {
            using var reply = await _client.PostAsync(Endpoint + "/test/prepare-upgrade", null);
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
        public async Task ExpectFatalExit(string reason = "Replication publication made no forward progress.", int seconds = 15)
        {
            await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(seconds));
            await Task.WhenAll(_output, _error);
            Assert.Equal(75, _process.ExitCode);
            Assert.Contains("FATAL " + reason, Diagnostics);
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

    static async Task<ulong> PublishedEvent(BlobContainerClient container, string database = "main")
    {
        var storage = new AzureReplicationStorage(container, database);
        var inventory = await CanonicalTrlInventory.DiscoverAsync(storage, TestNodeHost.Genesis);
        using var files = new InMemoryReplicationFileStorage();
        await using var collection = new ReplicationFileSet(files, inventory);
        await collection.InitializeAsync();
        using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        { FileCollection = collection, Compression = new NoCompressionStrategy(), CompactorScheduler = null });
        using var read = db.StartReadOnlyTransaction();
        return read.GetCommitUlong();
    }

    static async Task WaitPublished(BlobContainerClient container, ulong id, string database = "main")
    {
        var deadline = Stopwatch.StartNew();
        while (deadline.Elapsed < TimeSpan.FromSeconds(20))
        {
            try { if (await PublishedEvent(container, database) == id) return; }
            catch (IOException) { } // A racing version change requires a fresh canonical discovery.
            await Task.Delay(50);
        }
        throw new TimeoutException($"Canonical restore did not reach event {id}.");
    }

    [Fact]
    public async Task PreparedHigherGenerationTakesOverWithoutWaitingForLeaseExpiryAndTheOldLeaderFollows()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Node.Start(container);
        await leader.Wait(s => s.Role == "Leader", "initial leader");
        await leader.Apply(1, 11);
        await WaitPublished(container, 1);
        await using var upgraded = await Node.Start(container, generation: 2);
        await upgraded.Wait(s => s.Role == "Follower" && s.EventId == 1, "restored higher generation");
        await leader.Apply(2, 22);
        await upgraded.Apply(2, 22);
        await upgraded.Wait(s => Compared(s, 2), "comparison before handoff");
        var handoff = Stopwatch.StartNew();
        await upgraded.PrepareUpgrade();
        var promoted = await upgraded.Wait(s => s.Role == "Leader" && s.EventId == 2, "planned handoff");
        handoff.Stop();
        Assert.Equal(22, promoted.Value);
        // The drain waits out issued grants (1 s here); lease expiry would take 15 s.
        Assert.True(handoff.Elapsed < TimeSpan.FromSeconds(10), $"Handoff took {handoff.Elapsed}.");
        Console.WriteLine($"Planned handoff took {handoff.Elapsed.TotalMilliseconds:0} ms.");
        var record = System.Text.Json.Nodes.JsonNode.Parse(
            (await container.GetBlobClient("leader.json").DownloadContentAsync()).Value.Content.ToString())!;
        Assert.Equal(2ul, record["applicationGeneration"]!.GetValue<ulong>());
        Assert.Equal(upgraded.Endpoint, record["peerEndpoint"]!.GetValue<string>());
        // The lower generation never contends again; it follows and confirms the new leader's history.
        await upgraded.Apply(3, 33);
        await leader.Apply(3, 33);
        var follower = await leader.Wait(s => s.Role == "Follower" && Compared(s, 3), "old generation follows");
        Assert.Equal(33, follower.Value);
        await WaitPublished(container, 3);
    }

    [Fact]
    public async Task IsolatedLeaderLosesLeadershipAndFollowsTheNewLeaderAfterHealing()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Node.Start(container);
        await leader.Wait(s => s.Role == "Leader", "initial leader");
        await leader.Apply(1, 11);
        await WaitPublished(container, 1);
        await using var follower = await Node.Start(container);
        await follower.Wait(s => s.Role == "Follower" && s.EventId == 1, "restored follower");
        await leader.Apply(2, 22);
        await follower.Apply(2, 22);
        await follower.Wait(s => Compared(s, 2), "comparison before the partition");
        await WaitPublished(container, 2);
        // The process keeps running: it cannot renew its lease or answer peers, but local input continues.
        await leader.Partition("storage", true);
        await leader.Partition("peers", true);
        await leader.Wait(s => s.Role != "Leader", "isolated leader gives up leadership", 20);
        var promoted = await follower.Wait(s => s.Role == "Leader", "takeover after lease expiry", 35);
        Assert.Equal(2ul, promoted.EventId);
        await follower.Apply(3, 33);
        await leader.Apply(3, 33);
        await WaitPublished(container, 3);
        await leader.Partition("storage", false);
        await leader.Partition("peers", false);
        var healed = await leader.Wait(s => s.Role == "Follower" && Compared(s, 3), "healed old leader follows", 30);
        Assert.Equal(33, healed.Value);
    }

    [Fact]
    public async Task FollowerCutOffFromItsLeaderNeverTakesOverAndCatchesUpAfterHealing()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Node.Start(container);
        await leader.Wait(s => s.Role == "Leader", "initial leader");
        await leader.Apply(1, 11);
        await WaitPublished(container, 1);
        await using var follower = await Node.Start(container);
        await follower.Wait(s => s.Role == "Follower" && s.EventId == 1, "restored follower");
        await leader.Partition("peers", true);
        await leader.Apply(2, 22);
        await follower.Apply(2, 22);
        await WaitPublished(container, 2);
        // Longer than the 15 s lease: missing grants never let a follower contend while the leader renews.
        await Task.Delay(TimeSpan.FromSeconds(17));
        var cut = await follower.Wait(s => s.EventId == 2, "follower applied its input locally");
        Assert.Equal("Follower", cut.Role);
        Assert.NotEqual((cut.CompletedFile, cut.CompletedOffset), (cut.ComparedFile, cut.ComparedOffset));
        Assert.Equal("Leader", (await leader.Wait(_ => true, "leader state")).Role);
        await leader.Partition("peers", false);
        await follower.Wait(s => s.Role == "Follower" && Compared(s, 2), "comparison after healing");
        await leader.Apply(3, 33);
        await follower.Apply(3, 33);
        await follower.Wait(s => Compared(s, 3), "continued comparison");
    }

    [Fact]
    public async Task NodeKilledDuringRestoreRestoresAgainFromItsPartialLocalFiles()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Node.Start(container);
        await leader.Wait(s => s.Role == "Leader", "initial leader");
        for (ulong id = 1; id <= 24; id++) await leader.Apply(id, (byte)id, 1024); // 24 MiB of TRL
        await WaitPublished(container, 24);
        var directory = Path.Combine(Path.GetTempPath(), "btdb-process-node-" + Guid.NewGuid().ToString("N"));
        try
        {
            await using (var slow = await Node.Start(container, dataDirectory: directory, readDelayMilliseconds: 400))
            {
                await Task.Delay(TimeSpan.FromSeconds(2));
                Assert.Equal("Restoring", (await slow.Wait(_ => true, "restore in progress")).Role);
                await slow.Kill(); // In the middle of downloading: partial files stay behind.
            }
            Assert.NotEmpty(Directory.EnumerateFiles(directory));
            await using var restarted = await Node.Start(container, dataDirectory: directory);
            var restored = await restarted.Wait(s => s.Role == "Follower" && s.EventId == 24, "restore after the kill", 60);
            Assert.Equal(24, restored.Value);
            Assert.Equal(0, restored.Applied);
            await leader.Apply(25, 25);
            await restarted.Apply(25, 25);
            await restarted.Wait(s => Compared(s, 25), "comparison after the interrupted restore");
        }
        finally
        {
            await DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task TakeoverActivatesNoDatabaseUntilEveryDatabaseCaughtUp()
    {
        const string both = "main,second";
        var container = await fixture.ContainerAsync();
        await using var leader = await Node.Start(container, databases: both);
        await leader.Wait(s => s.Role == "Leader", "initial leader");
        await leader.Apply(1, 11);
        await leader.Apply(1, 12, database: "second");
        await WaitPublished(container, 1);
        await WaitPublished(container, 1, "second");
        await using var follower = await Node.Start(container, databases: both);
        await follower.Wait(s => s.Role == "Follower" && s.EventId == 1, "restored main");
        await follower.Wait(s => s.EventId == 1, "restored second", database: "second");
        await leader.Apply(2, 21);
        await leader.Apply(2, 22, database: "second");
        await follower.Apply(2, 21); // Its input for the second database lags behind.
        await WaitPublished(container, 2);
        await WaitPublished(container, 2, "second");
        await leader.Kill();
        await follower.Wait(s => s.Role == "Activating", "lease won, second database still lagging", 35);
        await Task.Delay(TimeSpan.FromSeconds(2));
        var waiting = await follower.Wait(_ => true, "still activating");
        Assert.Equal("Activating", waiting.Role);
        // Nothing is adopted or published while one database lags: main stays at the old leader's history.
        await follower.Apply(3, 31);
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.Equal(2ul, await PublishedEvent(container));
        await follower.Apply(2, 22, database: "second");
        var promoted = await follower.Wait(s => s.Role == "Leader", "activation after catching up");
        Assert.Equal(3ul, promoted.EventId);
        await WaitPublished(container, 3);
        await WaitPublished(container, 2, "second");
        Assert.Equal(22, (await follower.Wait(s => s.EventId == 2, "second database", database: "second")).Value);
    }

    [Fact]
    public async Task ObjectDbRollbacksInsideVirtualBatchesReplicateAndSurviveFailover()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Node.Start(container, objects: true);
        await leader.Wait(s => s.Role == "Leader", "initial leader");
        await leader.Orders(1, 20);
        await WaitPublished(container, 20);
        await using var follower = await Node.Start(container, objects: true);
        await follower.Wait(s => s.Role == "Follower" && s.EventId == 20, "restored follower");
        for (ulong from = 21; from <= 40; from += 10)
        {
            await leader.Orders(from, 10);
            await follower.Orders(from, 10);
        }
        // Byte equality includes every rolled-back order and its rejection inside the virtual batches.
        await follower.Wait(s => Compared(s, 40), "comparison of batched ObjectDB events");
        var expected = await leader.OrderSummary();
        Assert.Equal(expected, await follower.OrderSummary());
        Assert.Equal(5, expected.Rejections); // Events 7, 14, 21, 28 and 35.
        await leader.Kill();
        await follower.Wait(s => s.Role == "Leader" && s.EventId == 40, "takeover", 35);
        await follower.Orders(41, 10);
        await WaitPublished(container, 50);
        await using var replacement = await Node.Start(container, objects: true);
        await replacement.Wait(s => s.Role == "Follower" && s.EventId == 50, "cold restore");
        var summary = await follower.OrderSummary();
        Assert.Equal(summary, await replacement.OrderSummary());
        Assert.Equal(7, summary.Rejections);
        Assert.Equal(43, summary.Orders);
    }

    [Fact]
    public async Task UpgradedLeaderPublishesItsSchemaDetachingOldFollowersWhileUpgradedReplacementsRestoreIt()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Node.Start(container, objects: true);
        await leader.Wait(s => s.Role == "Leader", "initial leader");
        await using var old = await Node.Start(container, objects: true);
        await old.Wait(s => s.Role == "Follower", "old follower");
        await leader.Orders(1, 10);
        await old.Orders(1, 10);
        await old.Wait(s => Compared(s, 10), "old follower comparison");
        await WaitPublished(container, 10);
        // The upgraded build restores the old schema and executes nothing until its own schema is published.
        await using var upgraded = await Node.Start(container, generation: 2, objects: true);
        await upgraded.Wait(s => s.Role == "Follower" && s.EventId == 10, "restored upgraded node");
        await upgraded.PrepareUpgrade();
        await upgraded.Wait(s => s.Role == "Leader", "handoff to the upgraded build");
        await leader.Wait(s => s.Detached, "old leader detaches on the schema commit", 30);
        await old.Wait(s => s.Detached, "old follower detaches on the schema commit", 30);
        Assert.Equal(2, (await upgraded.OrderSummary()).IndexedFirstCustomer); // The new index covers orders 1 and 6.
        await upgraded.Orders(11, 10);
        await WaitPublished(container, 20);
        // Old nodes keep running locally; an upgraded replacement restores the schema commit by ordinary replay.
        await old.Orders(11, 5);
        await using var replacement = await Node.Start(container, generation: 2, objects: true);
        await replacement.Wait(s => s.Role == "Follower" && s.EventId == 20, "upgraded replacement");
        await upgraded.Orders(21, 10);
        await replacement.Orders(21, 10);
        await replacement.Wait(s => Compared(s, 30), "comparison with the upgraded schema");
        var summary = await upgraded.OrderSummary();
        Assert.Equal(summary, await replacement.OrderSummary());
        Assert.Equal(5, summary.IndexedFirstCustomer); // Orders 1, 6, 11, 16 and 26; 21 was rejected.
    }

    [Fact]
    public async Task RollingSchemaUpgradeUnderInputRestoresTheLaggingUpgradedLeaderOntoPublishedHistory()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Node.Start(container, objects: true);
        await leader.Wait(s => s.Role == "Leader", "initial leader");
        await using var old = await Node.Start(container, objects: true);
        await old.Wait(s => s.Role == "Follower", "old follower");
        await leader.Orders(1, 10);
        await old.Orders(1, 10);
        await WaitPublished(container, 10);
        var directory = Path.Combine(Path.GetTempPath(), "btdb-process-node-" + Guid.NewGuid().ToString("N"));
        try
        {
            await using (var upgraded = await Node.Start(container, generation: 2, objects: true, dataDirectory: directory,
                             activationTimeoutMilliseconds: 3000))
            {
                await upgraded.Wait(s => s.Role == "Follower" && s.EventId == 10, "restored upgraded node");
                // Input continues on the old build; the upgraded node cannot execute it before its schema exists.
                await leader.Orders(11, 10);
                await old.Orders(11, 10);
                await WaitPublished(container, 20);
                await upgraded.PrepareUpgrade();
                // After the handoff it lags behind the now frozen published history and cannot catch up by executing:
                // the activation deadline fences and restarts it.
                await upgraded.ExpectFatalExit("Replication activation made no forward progress.", 40);
            }
            // The restarted node restores the published history, and the selected generation keeps old nodes out.
            await using var restarted = await Node.Start(container, generation: 2, objects: true, dataDirectory: directory,
                activationTimeoutMilliseconds: 3000);
            await restarted.Wait(s => s.Role == "Leader" && s.EventId == 20, "upgraded leader on published history", 40);
            await leader.Wait(s => s.Detached, "old leader detaches", 30);
            await old.Wait(s => s.Detached, "old follower detaches", 30);
            var record = System.Text.Json.Nodes.JsonNode.Parse(
                (await container.GetBlobClient("leader.json").DownloadContentAsync()).Value.Content.ToString())!;
            Assert.Equal(2ul, record["applicationGeneration"]!.GetValue<ulong>());
            // Its event loop resumes from retained input after the published history.
            await restarted.Orders(21, 10);
            await WaitPublished(container, 30);
            await using var replacement = await Node.Start(container, generation: 2, objects: true);
            await replacement.Wait(s => s.Role == "Follower" && s.EventId == 30, "upgraded replacement");
            Assert.Equal(await restarted.OrderSummary(), await replacement.OrderSummary());
        }
        finally
        {
            await DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task UpgradedFollowerExecutingBeforeItsSchemaIsPublishedRestartsWithoutAffectingHistory()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Node.Start(container, objects: true);
        await leader.Wait(s => s.Role == "Leader", "initial leader");
        await leader.Orders(1, 5);
        await WaitPublished(container, 5);
        await using var upgraded = await Node.Start(container, generation: 2, objects: true);
        await upgraded.Wait(s => s.Role == "Follower" && s.EventId == 5, "restored upgraded follower");
        // Executing with the new relation persists its index upgrade locally, which the leader never wrote.
        await leader.Orders(6, 5);
        await upgraded.Orders(6, 5);
        await upgraded.ExpectRestartExit("Follower native history diverged");
        Assert.Equal("Leader", (await leader.Wait(_ => true, "unaffected leader")).Role);
        await leader.Orders(11, 5);
        await WaitPublished(container, 15);
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
