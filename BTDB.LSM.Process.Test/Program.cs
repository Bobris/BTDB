using System;
using System.Buffers.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;

namespace BTDB.LSM.ProcessTests;

/// A writer or reader node on a FileSystemLsmStorage directory, driven by LsmProcessTest through stdin and stdout.
///
/// Writer: `--writer {directory} {name} {leaseMilliseconds} {maxCommit}` waits for the writer lease, then commits
/// CommitUlong + 1 with key CommitUlong (4 bytes, big endian) and value "{name}:{padding}:{CommitUlong}", durably, printing
/// "WRITER" once it holds the lease and "DURABLE {n}" after each commit. "STOP" disposes it gracefully.
///
/// Reader: `--reader {directory}` answers "WAIT {n}" with "SEEN {c} OK" once its snapshot has CommitUlong c >= n and
/// holds exactly keys 1..c with matching values, or "SEEN {c} BAD {reason}"; "OWNER {k}" prints "OWNER {k} {name}".
public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        Console.WriteLine("PID " + Environment.ProcessId);
        try
        {
            return args[0] switch
            {
                "--writer" => await Writer(args[1], args[2], int.Parse(args[3]), int.Parse(args[4])),
                "--reader" => await Reader(args[1]),
                _ => 2
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine("ERROR " + ex.GetType().Name + " " + ex.Message.ReplaceLineEndings(" "));
            return 1;
        }
    }

    static LsmKeyValueDBOptions Options(string directory, int leaseMilliseconds) => new()
    {
        Storage = new FileSystemLsmStorage(directory), MemtableSize = 16 * 1024, BlockSize = 1024,
        Level0CompactionTrigger = 2, LevelBaseBytes = 64 * 1024, LevelMultiplier = 4, LevelCount = 4,
        TargetFileSize = 32 * 1024, WalFlushInterval = TimeSpan.FromMilliseconds(2),
        LeaseDuration = TimeSpan.FromMilliseconds(leaseMilliseconds),
        LeaseSafetyMargin = TimeSpan.FromMilliseconds(leaseMilliseconds / 4),
        LeaseRetryInterval = TimeSpan.FromMilliseconds(50), DeletionGracePeriod = TimeSpan.FromSeconds(30),
        RefreshRetryInterval = TimeSpan.FromMilliseconds(5), ManifestListInterval = TimeSpan.FromMilliseconds(200)
    };

    static readonly string Padding = new('x', 300);

    internal static byte[] Key(ulong commit)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(key, (uint)commit);
        return key;
    }

    static async Task<int> Writer(string directory, string name, int leaseMilliseconds, int maxCommit)
    {
        using var db = await LsmKeyValueDB.OpenAsync(Options(directory, leaseMilliseconds));
        db.DurableTransactions = true;
        using var stop = new CancellationTokenSource();
        var commands = Task.Run(() =>
        {
            while (Console.ReadLine() is { } line)
                if (line == "STOP")
                    break;
            stop.Cancel();
        });
        Console.WriteLine("STARTED");
        var announced = false;
        while (!stop.IsCancellationRequested)
        {
            ulong commit;
            IKeyValueDBTransaction transaction;
            try
            {
                transaction = await db.StartWritingTransaction().AsTask().WaitAsync(stop.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            using (var tr = transaction)
            {
                if (!announced)
                {
                    Console.WriteLine("WRITER");
                    announced = true;
                }

                commit = tr.GetCommitUlong() + 1;
                if (commit > (ulong)maxCommit) break;
                using var cursor = tr.CreateCursor();
                // The padding makes the small memtable flush and compact every few dozen commits.
                cursor.CreateOrUpdateKeyValue(Key(commit), Encoding.UTF8.GetBytes($"{name}:{Padding}:{commit}"));
                tr.SetCommitUlong(commit);
                tr.Commit();
            }

            Console.WriteLine("DURABLE " + commit);
            await Task.Delay(2);
        }

        await commands;
        return 0;
    }

    static async Task<int> Reader(string directory)
    {
        using var db = await LsmKeyValueDB.OpenAsync(Options(directory, 15_000) with { RegisterReader = true });
        while (Console.ReadLine() is { } line)
        {
            var parts = line.Split(' ');
            if (parts[0] == "STOP") break;
            if (parts[0] == "WAIT")
            {
                await db.WaitForCommitUlongAsync(ulong.Parse(parts[1])).AsTask().WaitAsync(TimeSpan.FromSeconds(30));
                using var tr = db.StartReadOnlyTransaction();
                var commit = tr.GetCommitUlong();
                Console.WriteLine($"SEEN {commit} {Check(tr, commit)}");
            }
            else if (parts[0] == "OWNER")
            {
                await db.RefreshAsync();
                using var tr = db.StartReadOnlyTransaction();
                using var cursor = tr.CreateCursor();
                var key = ulong.Parse(parts[1]);
                var owner = cursor.FindExactKey(Key(key))
                    ? Encoding.UTF8.GetString(cursor.SlowGetValue()).Split(':')[0]
                    : "-";
                Console.WriteLine($"OWNER {key} {owner}");
            }
        }

        return 0;
    }

    /// Keys 1..commit with values "{name}:{key}" and nothing else.
    static string Check(IKeyValueDBTransaction tr, ulong commit)
    {
        if (tr.GetKeyValueCount() != (long)commit) return $"BAD count {tr.GetKeyValueCount()}";
        using var cursor = tr.CreateCursor();
        ulong expected = 1;
        if (cursor.FindFirstKey([]))
            do
            {
                var key = BinaryPrimitives.ReadUInt32BigEndian(cursor.SlowGetKey());
                if (key != expected) return $"BAD key {key} expected {expected}";
                var value = Encoding.UTF8.GetString(cursor.SlowGetValue());
                if (!value.EndsWith(":" + key, StringComparison.Ordinal)) return $"BAD value {value} for {key}";
                expected++;
            } while (cursor.FindNextKey([]));

        return expected == commit + 1 ? "OK" : $"BAD ended at {expected}";
    }
}
