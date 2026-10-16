using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Xunit;

namespace BTDB.LSM.ProcessTests;

/// Writers and readers in separate processes on one FileSystemLsmStorage directory (ImplementationPlan.md, section
/// 19.6): a reader follows a writer, a waiting writer takes over from a killed one without losing a durable commit,
/// and a gracefully disposed writer hands the lease over at once.
public sealed class LsmProcessTest : IDisposable
{
    readonly string _directory =
        Path.Combine(Path.GetTempPath(), "btdb-lsm-process-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_directory)) Directory.Delete(_directory, true);
        }
        catch (IOException)
        {
        }
    }

    sealed class Node : IAsyncDisposable
    {
        readonly Process _process;
        readonly Channel<string> _lines = Channel.CreateUnbounded<string>();
        readonly StringBuilder _log = new();
        readonly Task _pump;

        Node(Process process)
        {
            _process = process;
            _pump = Task.Run(async () =>
            {
                while (await process.StandardOutput.ReadLineAsync() is { } line)
                {
                    lock (_log) _log.AppendLine(line);
                    await _lines.Writer.WriteAsync(line);
                }

                _lines.Writer.TryComplete();
            });
        }

        public string Log
        {
            get
            {
                lock (_log) return _log.ToString();
            }
        }

        public bool HasExited => _process.HasExited;
        public int ExitCode => _process.ExitCode;

        public static Node Start(params string[] arguments)
        {
            var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
            {
                UseShellExecute = false, RedirectStandardInput = true, RedirectStandardOutput = true,
                RedirectStandardError = false
            };
            start.ArgumentList.Add(typeof(Program).Assembly.Location);
            foreach (var argument in arguments) start.ArgumentList.Add(argument);
            return new(Process.Start(start)!);
        }

        public void Send(string line) => _process.StandardInput.WriteLine(line);

        /// Reads lines until one satisfies match; fails on ERROR, exit or timeout.
        public async Task<string> Expect(Func<string, bool> match, string description, int seconds = 30)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
            try
            {
                while (true)
                {
                    var line = await _lines.Reader.ReadAsync(timeout.Token);
                    if (line.StartsWith("ERROR", StringComparison.Ordinal))
                        throw new InvalidOperationException($"{line}{Environment.NewLine}{Log}");
                    if (match(line)) return line;
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or ChannelClosedException)
            {
                throw new TimeoutException($"No {description} within {seconds} s.{Environment.NewLine}{Log}");
            }
        }

        /// Whether a matching line arrives within the time, without failing.
        public async Task<bool> Sees(Func<string, bool> match, TimeSpan time)
        {
            using var timeout = new CancellationTokenSource(time);
            try
            {
                while (true)
                    if (match(await _lines.Reader.ReadAsync(timeout.Token)))
                        return true;
            }
            catch (Exception ex) when (ex is OperationCanceledException or ChannelClosedException)
            {
                return false;
            }
        }

        public async Task<ulong> ExpectDurable(ulong atLeast, int seconds = 30)
        {
            var line = await Expect(l => l.StartsWith("DURABLE ") && ulong.Parse(l[8..]) >= atLeast,
                $"DURABLE {atLeast}", seconds);
            return ulong.Parse(line[8..]);
        }

        /// The last DURABLE number this node printed.
        public ulong LastDurable()
        {
            ulong last = 0;
            foreach (var line in Log.Split(Environment.NewLine))
                if (line.StartsWith("DURABLE ")) last = ulong.Parse(line[8..]);
            return last;
        }

        public void Kill()
        {
            if (!_process.HasExited) _process.Kill(true);
            _process.WaitForExit();
        }

        public async Task WaitForExit(int seconds = 30)
        {
            await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(seconds));
            await _pump;
        }

        public async ValueTask DisposeAsync()
        {
            Kill();
            await _pump;
            _process.Dispose();
        }
    }

    Node Writer(string name, int leaseMilliseconds = 2000, int maxCommit = 100_000) =>
        Node.Start("--writer", _directory, name, leaseMilliseconds.ToString(), maxCommit.ToString());

    Node Reader() => Node.Start("--reader", _directory);

    static async Task<ulong> SeenConsistent(Node reader, ulong atLeast)
    {
        reader.Send("WAIT " + atLeast);
        var line = await reader.Expect(l => l.StartsWith("SEEN "), "SEEN " + atLeast);
        var parts = line.Split(' ', 3);
        Assert.True(parts[2] == "OK", line);
        var seen = ulong.Parse(parts[1]);
        Assert.True(seen >= atLeast, line);
        return seen;
    }

    static async Task<string> Owner(Node reader, ulong key)
    {
        reader.Send("OWNER " + key);
        var line = await reader.Expect(l => l.StartsWith($"OWNER {key} "), "OWNER " + key);
        return line.Split(' ')[2];
    }

    [Fact]
    public async Task ReaderProcessFollowsWriterProcess()
    {
        await using var writer = Writer("a", maxCommit: 300);
        await writer.Expect(l => l == "WRITER", "WRITER");
        await using var reader = Reader();
        await writer.ExpectDurable(50);
        await SeenConsistent(reader, 50);
        await writer.ExpectDurable(300);
        Assert.Equal(300ul, await SeenConsistent(reader, 300));
        Assert.True(Directory.GetFiles(Path.Combine(_directory, "tables")).Length > 1);
        writer.Send("STOP");
        await writer.WaitForExit();
        Assert.Equal(0, writer.ExitCode);
        reader.Send("STOP");
        await reader.WaitForExit();
    }

    [Fact]
    public async Task WaitingWriterTakesOverFromKilledWriterWithoutLosingDurableCommits()
    {
        await using var first = Writer("a");
        await first.Expect(l => l == "WRITER", "WRITER from the first writer");
        await first.ExpectDurable(40);
        await using var second = Writer("b");
        await second.Expect(l => l == "STARTED", "STARTED from the second writer");
        await using var reader = Reader();
        Assert.False(await second.Sees(l => l == "WRITER", TimeSpan.FromSeconds(1)));
        first.Kill();
        var lastDurable = first.LastDurable();
        Assert.True(lastDurable >= 40);
        await second.Expect(l => l == "WRITER", "WRITER from the second writer after the lease expired", 20);
        var taken = await second.ExpectDurable(lastDurable + 1);
        // The first durable commit of the second writer follows every durable commit of the first one.
        Assert.True(taken > lastDurable);
        await second.ExpectDurable(lastDurable + 30);
        await SeenConsistent(reader, lastDurable + 30);
        Assert.Equal("a", await Owner(reader, lastDurable));
        Assert.Equal("b", await Owner(reader, lastDurable + 30));
        second.Send("STOP");
        await second.WaitForExit();
        Assert.Equal(0, second.ExitCode);
    }

    [Fact]
    public async Task DisposedWriterHandsTheLeaseOverAtOnce()
    {
        // A long lease: the handover is fast only because the first writer releases it.
        await using var first = Writer("a", leaseMilliseconds: 30_000);
        await first.Expect(l => l == "WRITER", "WRITER from the first writer");
        await first.ExpectDurable(20);
        await using var second = Writer("b", leaseMilliseconds: 30_000);
        await second.Expect(l => l == "STARTED", "STARTED from the second writer");
        first.Send("STOP");
        await first.WaitForExit();
        Assert.Equal(0, first.ExitCode);
        var stopwatch = Stopwatch.StartNew();
        await second.Expect(l => l == "WRITER", "WRITER from the second writer", 10);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5), stopwatch.Elapsed.ToString());
        await second.ExpectDurable(first.LastDurable() + 10);
        await using var reader = Reader();
        await SeenConsistent(reader, first.LastDurable() + 10);
    }
}
