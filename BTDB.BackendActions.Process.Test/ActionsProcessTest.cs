using System.Diagnostics;
using System.Net.Http.Json;
using System.Text;
using Azure.Storage.Blobs;
using BTDB.Replication.Azure.Test;
using Xunit;
using ChildProcess = System.Diagnostics.Process;

namespace BTDB.BackendActions.ProcessTests;

/// <summary>Separate node processes over Azurite: real HTTP replication and leader channel, EventLog topics,
/// EventSourcing and BackendActions. Every action must finish exactly once on every replica, whichever node dies.</summary>
public class ActionsProcessTest(BlobStorageFixture fixture) : IClassFixture<BlobStorageFixture>
{
    sealed class Node : IAsyncDisposable
    {
        readonly ChildProcess _process;
        readonly Task _output, _error;
        readonly StringBuilder _diagnostics = new();
        readonly HttpClient _client = new() { Timeout = TimeSpan.FromSeconds(30) };
        readonly TaskCompletionSource<string> _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly string _directory;
        public readonly string Name;
        public string Endpoint = "";

        Node(ChildProcess process, string name, string directory)
        {
            _process = process;
            Name = name;
            _directory = directory;
            _output = Drain(process.StandardOutput, true);
            _error = Drain(process.StandardError, false);
        }

        async Task Drain(StreamReader reader, bool output)
        {
            while (await reader.ReadLineAsync() is { } line)
            {
                if (output && line.StartsWith("READY ", StringComparison.Ordinal)) _ready.TrySetResult(line[6..]);
                lock (_diagnostics)
                {
                    _diagnostics.AppendLine(line);
                    if (_diagnostics.Length > 32 * 1024) _diagnostics.Remove(0, _diagnostics.Length - 32 * 1024);
                }
            }
            if (output) _ready.TrySetException(new IOException($"Node {Name} exited before startup: {Diagnostics}"));
        }

        public string Diagnostics { get { lock (_diagnostics) return _diagnostics.ToString(); } }
        public bool Exited => _process.HasExited;

        public static async Task<Node> Start(BlobContainerClient container, string name)
        {
            var directory = Path.Combine(Path.GetTempPath(), "btdb-actions-node-" + Guid.NewGuid().ToString("N"));
            var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
            { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
            start.ArgumentList.Add(typeof(Program).Assembly.Location);
            start.ArgumentList.Add("--node");
            start.ArgumentList.Add(container.Uri.ToString());
            start.Environment["BTDB_TEST_NODE_ID"] = name;
            start.Environment["BTDB_TEST_DATA_DIRECTORY"] = directory;
            var node = new Node(ChildProcess.Start(start)!, name, directory);
            try
            {
                node.Endpoint = await node._ready.Task.WaitAsync(TimeSpan.FromSeconds(30));
                return node;
            }
            catch { await node.DisposeAsync(); throw; }
        }

        public async Task<NodeState> Wait(Func<NodeState, bool> ready, string description, int seconds = 30)
        {
            var deadline = Stopwatch.StartNew();
            NodeState? last = null;
            while (deadline.Elapsed < TimeSpan.FromSeconds(seconds))
            {
                if (_process.HasExited) throw new IOException($"Node {Name} exited with {_process.ExitCode}: {Diagnostics}");
                try
                {
                    last = await _client.GetFromJsonAsync<NodeState>(Endpoint + "/test/state");
                    if (last != null && ready(last)) return last;
                }
                catch (HttpRequestException) { }
                await Task.Delay(100);
            }
            throw new TimeoutException($"Waiting for {Name} {description}; last state: {last}; {Diagnostics}");
        }

        public async Task<ulong[]> Enqueue(int count, int milliseconds)
        {
            using var reply = await _client.PostAsync($"{Endpoint}/test/enqueue/{count}/{milliseconds}", null);
            reply.EnsureSuccessStatusCode();
            return (await reply.Content.ReadFromJsonAsync<ulong[]>())!;
        }

        public async Task<Finish[]> Finishes() =>
            (await _client.GetFromJsonAsync<Finish[]>(Endpoint + "/test/finishes"))!;

        public async Task<Execution[]> Executions() =>
            (await _client.GetFromJsonAsync<Execution[]>(Endpoint + "/test/executions"))!;

        // Each replica's OnFinished handled every action exactly once, as Completed.
        public async Task<Finish[]> WaitFinished(IReadOnlyCollection<ulong> ids, int seconds = 90)
        {
            var deadline = Stopwatch.StartNew();
            Finish[] finishes = [];
            while (deadline.Elapsed < TimeSpan.FromSeconds(seconds))
            {
                if (_process.HasExited) throw new IOException($"Node {Name} exited with {_process.ExitCode}: {Diagnostics}");
                try
                {
                    finishes = await Finishes();
                    if (finishes.Length >= ids.Count) break;
                }
                catch (HttpRequestException) { }
                await Task.Delay(200);
            }
            Assert.True(finishes.Length == ids.Count,
                $"{Name} finished {finishes.Length} of {ids.Count} actions; {Diagnostics}");
            Assert.Equal(ids.Order(), finishes.Select(f => f.ActionId).Order());
            Assert.All(finishes, f => Assert.Equal((ActionOutcomeKind.Completed, 1), (f.Kind, f.Count)));
            return finishes.OrderBy(f => f.ActionId).ToArray();
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
            for (var attempt = 1; attempt <= 50; attempt++)
            {
                try
                {
                    if (Directory.Exists(_directory)) Directory.Delete(_directory, true);
                    break;
                }
                catch (IOException) { await Task.Delay(100); }
            }
        }
    }

    // A leader with its schema published, then followers that restore it.
    async Task<Node[]> StartCluster(BlobContainerClient container, params string[] names)
    {
        var nodes = new List<Node>();
        try
        {
            var first = await Node.Start(container, names[0]);
            nodes.Add(first);
            await first.Wait(s => s is { Role: "Leader", Running: true, Published: true }, "to lead with its schema published");
            foreach (var name in names[1..]) nodes.Add(await Node.Start(container, name));
            foreach (var node in nodes) await node.Wait(s => s.Running, "to run its application");
            return nodes.ToArray();
        }
        catch (Exception error)
        {
            var diagnostics = string.Join("\n", nodes.Select(n => $"--- {n.Name}:\n{n.Diagnostics}"));
            foreach (var node in nodes) await node.DisposeAsync();
            throw new IOException(error.Message + "\n" + diagnostics, error);
        }
    }

    static IEnumerable<(ulong, ulong, ActionOutcomeKind, string, int)> Fields(Finish[] finishes) =>
        finishes.Select(f => (f.ActionId, f.Attempt, f.Kind, f.Node, f.Count));

    static async Task UntilExecuting(Node node)
    {
        var deadline = Stopwatch.StartNew();
        while ((await node.Executions()).Length == 0)
        {
            if (deadline.Elapsed > TimeSpan.FromSeconds(30)) throw new TimeoutException($"{node.Name} executed nothing.");
            await Task.Delay(50);
        }
    }

    [Fact]
    public async Task ActionsRunOnceAcrossTheClusterAndFinishOnEveryReplica()
    {
        var container = await fixture.ContainerAsync();
        var nodes = await StartCluster(container, "a", "b", "c");
        try
        {
            var ids = await nodes[1].Enqueue(12, 300);
            foreach (var node in nodes) await node.WaitFinished(ids);
            var executions = new List<Execution>();
            foreach (var node in nodes) executions.AddRange(await node.Executions());
            Assert.Equal(ids.Order(), executions.Select(e => e.ActionId).Order()); // no attempt ran twice
            Assert.All(executions, e => Assert.Equal(1ul, e.Attempt));
        }
        finally
        {
            foreach (var node in nodes) await node.DisposeAsync();
        }
    }

    [Fact]
    public async Task AKilledWorkersActionsRunAgainOnTheOtherNodes()
    {
        var container = await fixture.ContainerAsync();
        var nodes = await StartCluster(container, "a", "b", "c");
        try
        {
            var ids = await nodes[0].Enqueue(9, 4000);
            await UntilExecuting(nodes[2]);
            var lost = (await nodes[2].Executions()).Select(e => e.ActionId).ToHashSet();
            await nodes[2].Kill();
            var finishes = await nodes[0].WaitFinished(ids);
            Assert.Equal(Fields(finishes), Fields(await nodes[1].WaitFinished(ids)));
            Assert.All(finishes.Where(f => lost.Contains(f.ActionId)),
                f => Assert.True(f.Attempt == 2 && f.Node != "c", $"Action {f.ActionId}: attempt {f.Attempt} on {f.Node}."));
        }
        finally
        {
            foreach (var node in nodes) await node.DisposeAsync();
        }
    }

    [Fact]
    public async Task AKilledLeadersActionsRunAgainAndTheRunningOnesAreAdopted()
    {
        var container = await fixture.ContainerAsync();
        var nodes = await StartCluster(container, "a", "b", "c");
        try
        {
            var ids = await nodes[1].Enqueue(9, 8000);
            await UntilExecuting(nodes[0]);
            await UntilExecuting(nodes[1]);
            var lost = (await nodes[0].Executions()).Select(e => e.ActionId).ToHashSet();
            var adopted = (await nodes[1].Executions()).Select(e => e.ActionId).ToHashSet();
            await nodes[0].Kill(); // the leader, its coordinator and its worker
            var finishes = await nodes[1].WaitFinished(ids, 120);
            Assert.Equal(Fields(finishes), Fields(await nodes[2].WaitFinished(ids, 120)));
            Assert.All(finishes.Where(f => lost.Contains(f.ActionId)),
                f => Assert.True(f.Attempt >= 2 && f.Node != "a", $"Action {f.ActionId}: attempt {f.Attempt} on {f.Node}."));
            // Survivors' attempts outlive the 15 s replication lease and finish under the new coordinator.
            Assert.All(finishes.Where(f => adopted.Contains(f.ActionId)),
                f => Assert.True(f.Attempt == 1 && f.Node == "b", $"Action {f.ActionId}: attempt {f.Attempt} on {f.Node}."));
        }
        finally
        {
            foreach (var node in nodes) await node.DisposeAsync();
        }
    }
}
