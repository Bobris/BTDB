using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Format;
using BTDB.LSM.Storage;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Api;

/// Runs the same random operations against BTreeKeyValueDB and LsmKeyValueDB and compares every observable result:
/// return values, cursor validity, keys, values, key indexes, counts, ulongs and an older snapshot.
public class LsmKeyValueDBOracleTest(ITestOutputHelper output)
{
    [Theory]
    [InlineData(1, 64 * 1024 * 1024, 32 * 1024, 64, 20_000)]
    [InlineData(2, 1024, 128, 2, 20_000)]
    [InlineData(3, 4096, 256, 0, 20_000)]
    [InlineData(4, 512, 64, 1_000_000, 20_000)]
    [InlineData(5, 2048, 256, 4, 100_000)]
    [InlineData(6, 1024, 128, 2, 30_000, 0)]
    [InlineData(7, 2048, 256, 64, 30_000, 2)]
    public async Task MatchesBTreeKeyValueDB(int seed, long memtableSize, int blockSize, int pointDeleteRangeLimit,
        int operations, int valueSeparationThreshold = 1024)
    {
        var random = new Random(seed);
        var storage = new InMemoryLsmStorage();
        using var expectedDb = new BTreeKeyValueDB(new InMemoryFileCollection());
        var actualDb = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, MemtableSize = memtableSize, BlockSize = blockSize, RestartInterval = 2, IndexPartitionSize = 48,
            PointDeleteRangeLimit = pointDeleteRangeLimit, Level0CompactionTrigger = 3, LevelBaseBytes = 4096,
            LevelMultiplier = 3, LevelCount = 5, TargetFileSize = 2048,
            ValueSeparationThreshold = valueSeparationThreshold, ValueGcThreshold = 0.6, BlockCache = new LsmBlockCache(32 * 1024)
        });
        try
        {
            Run(random, expectedDb, actualDb, operations);
        }
        finally
        {
            actualDb.Dispose();
        }

        output.WriteLine(actualDb.CalcStats());
    }

    readonly Queue<string> _log = new();

    void Log(string message)
    {
        _log.Enqueue(message);
        if (_log.Count > 15) _log.Dequeue();
    }

    Func<string>? _dump;

    string History => string.Join(Environment.NewLine, _log) + Environment.NewLine + _dump?.Invoke();

    void Run(Random random, IKeyValueDB expectedDb, IKeyValueDB actualDb, int operations)
    {
        var pair = new Pair(expectedDb.StartTransaction(), actualDb.StartTransaction());
        var snapshot = new Pair(expectedDb.StartReadOnlyTransaction(), actualDb.StartReadOnlyTransaction());
        var cursors = NewCursors(pair, 3);
        var snapshotCursors = NewCursors(snapshot, 1);
        _dump = () => "E keys: " + Keys(pair.E) + Environment.NewLine + "A keys: " + Keys(pair.A) + Environment.NewLine +
                      string.Join(Environment.NewLine, cursors.Select((c, i) =>
            $"cursor {i}: E valid {c.E.IsValid()} index {TryIndex(c.E)}; A valid {c.A.IsValid()} index {TryIndex(c.A)} fresh {FreshIndex(c.A)} expected fresh {FreshExpected(c)}" +
                (((LsmKeyValueDBCursor)c.A).DebugKey is { } k ? Environment.NewLine + Components(pair.A, k) : "")));
        try
        {
            for (var op = 0; op < operations; op++)
            {
                var cursor = cursors[random.Next(cursors.Count)];
                var choice = random.Next(100);
                var step = $"op {op} choice {choice} cursor {cursors.IndexOf(cursor)}";
                if (choice < 25)
                {
                    var key = RandomKey(random);
                    var value = RandomValue(random);
                    step += $" create {Hex(key)}={Hex(value)}";
                    Same(step, () => cursor.E.CreateOrUpdateKeyValue(key, value),
                        () => cursor.A.CreateOrUpdateKeyValue(key, value));
                    // BTreeKeyValueDB keeps a stale "removed current" flag after CreateOrUpdateKeyValue, so
                    // GetKeyIndex would be one lower; finding the key again clears it.
                    Same(step + " refind", () => cursor.E.Find(key, 0), () => cursor.A.Find(key, 0));
                }
                else if (choice < 33)
                {
                    var prefix = RandomPrefix(random);
                    if (random.Next(2) == 0) Same(step, () => cursor.E.FindFirstKey(prefix), () => cursor.A.FindFirstKey(prefix));
                    else Same(step, () => cursor.E.FindLastKey(prefix), () => cursor.A.FindLastKey(prefix));
                }
                else if (choice < 45)
                {
                    var prefix = RandomPrefix(random);
                    if (random.Next(2) == 0) Same(step, () => cursor.E.FindNextKey(prefix), () => cursor.A.FindNextKey(prefix));
                    else Same(step, () => cursor.E.FindPreviousKey(prefix), () => cursor.A.FindPreviousKey(prefix));
                }
                else if (choice < 52)
                {
                    var key = RandomKey(random);
                    var prefixLen = (uint)random.Next(key.Length + 1);
                    step += $" find {Hex(key)}/{prefixLen}";
                    Same(step, () => cursor.E.Find(key, prefixLen), () => cursor.A.Find(key, prefixLen));
                }
                else if (choice < 57)
                {
                    var count = pair.E.GetKeyValueCount();
                    var index = random.NextInt64(-1, count + 2);
                    Same(step, () => cursor.E.FindKeyIndex(index), () => cursor.A.FindKeyIndex(index));
                }
                else if (choice < 60)
                {
                    var prefix = RandomPrefix(random);
                    var index = random.Next(-1, 6);
                    Same(step, () => cursor.E.FindKeyIndex(prefix, index), () => cursor.A.FindKeyIndex(prefix, index));
                }
                else if (choice < 68)
                {
                    step += " erase";
                    if (cursor.E.IsValid()) Same(step, () => { cursor.E.EraseCurrent(); return 0; }, () => { cursor.A.EraseCurrent(); return 0; });
                }
                else if (choice < 71)
                {
                    var other = cursors[random.Next(cursors.Count)];
                    step += $" erase up to cursor {cursors.IndexOf(other)}";
                    if (cursor.E.IsValid() && other.E.IsValid() && other != cursor)
                        Same(step, () => cursor.E.EraseUpTo(other.E), () => cursor.A.EraseUpTo(other.A));
                }
                else if (choice < 75)
                {
                    var value = RandomValue(random);
                    step += $" set value {Hex(value)} at {(cursor.E.IsValid() ? Hex(GetKey(cursor.E)) : "-")}";
                    if (cursor.E.IsValid())
                    {
                        var target = GetKey(cursor.E);
                        Same(step, () => { cursor.E.SetValue(value); return 0; }, () => { cursor.A.SetValue(value); return 0; });
                        using var freshE = pair.E.CreateCursor();
                        using var freshA = pair.A.CreateCursor();
                        freshE.Find(target, 0);
                        freshA.Find(target, 0);
                        if (!GetValue(freshA).AsSpan().SequenceEqual(value))
                            Assert.Fail($"{step}: LSM lost SetValue{Environment.NewLine}{History}");
                        if (!GetValue(freshE).AsSpan().SequenceEqual(value))
                            Assert.Fail($"{step}: BTreeKeyValueDB lost SetValue{Environment.NewLine}{History}");
                        // BTreeKeyValueDB loses this SetValue when another cursor positioned before it later erases
                        // a key in the same leaf; re-finding the other oracle cursors avoids that bug.
                        foreach (var other in cursors)
                            if (other != cursor && other.E.IsValid())
                                other.E.Find(GetKey(other.E), 0);
                    }
                }
                else if (choice < 78)
                {
                    var key = RandomKey(random);
                    var prefixLen = (uint)random.Next(key.Length + 1);
                    step += $" suffix {Hex(key)}/{prefixLen}";
                    Same(step, () => cursor.E.UpdateKeySuffix(key, prefixLen), () => cursor.A.UpdateKeySuffix(key, prefixLen));
                    // BTreeKeyValueDB leaves a stale key index on the cursor after these results; ignore its position.
                    if (!cursor.A.IsValid() || cursor.E.GetKeyIndex() != cursor.A.GetKeyIndex())
                    {
                        cursor.E.Invalidate();
                        cursor.A.Invalidate();
                    }
                }
                else if (choice < 80)
                {
                    var index = (uint)random.Next(3);
                    var value = (ulong)random.Next(4);
                    Log($"{step}: set ulong {index} and commit ulong to {value}");
                    pair.E.SetUlong(index, value);
                    pair.A.SetUlong(index, value);
                    pair.E.SetCommitUlong(value);
                    pair.A.SetCommitUlong(value);
                }
                else if (choice < 84)
                {
                    var commit = random.Next(3) != 0;
                    Log($"{step}: {(commit ? "commit" : "rollback")}");
                    foreach (var c in cursors) c.Dispose();
                    if (commit)
                    {
                        pair.E.Commit();
                        pair.A.Commit();
                    }

                    pair.Dispose();
                    if (random.Next(4) == 0)
                    {
                        foreach (var c in snapshotCursors) c.Dispose();
                        snapshot.Dispose();
                        snapshot = new(expectedDb.StartReadOnlyTransaction(), actualDb.StartReadOnlyTransaction());
                        snapshotCursors = NewCursors(snapshot, 1);
                    }

                    pair = new(expectedDb.StartTransaction(), actualDb.StartTransaction());
                    cursors = NewCursors(pair, 1 + random.Next(3));
                    CompareAll(pair, step + " after commit");
                }
                else
                {
                    var c = snapshotCursors[0];
                    var prefix = RandomPrefix(random);
                    Same(step, () => c.E.FindNextKey(prefix), () => c.A.FindNextKey(prefix));
                    CompareCursor(step, c);
                }

                Assert.Equal(pair.E.GetKeyValueCount(), pair.A.GetKeyValueCount());
                Assert.Equal(pair.E.GetUlongCount() == 0 ? 0 : pair.E.GetUlong(0), pair.A.GetUlong(0));
                Assert.Equal(pair.E.GetCommitUlong(), pair.A.GetCommitUlong());
                for (var i = 0; i < cursors.Count; i++) CompareCursor($"{step} -> cursor {i}", cursors[i]);
                if (op % 997 == 0) CompareAll(pair);
            }

            CompareAll(pair);
            CompareAll(snapshot);
        }
        finally
        {
            foreach (var c in cursors) c.Dispose();
            foreach (var c in snapshotCursors) c.Dispose();
            pair.Dispose();
            snapshot.Dispose();
        }
    }

    static List<CursorPair> NewCursors(Pair pair, int count) =>
        Enumerable.Range(0, count).Select(_ => new CursorPair(pair.E.CreateCursor(), pair.A.CreateCursor())).ToList();

    void Same<T>(string step, Func<T> expected, Func<T> actual, bool allowActualSuccess = false)
    {
        T? e = default, a = default;
        Exception? ee = null, ae = null;
        try
        {
            e = expected();
        }
        catch (Exception ex)
        {
            ee = ex;
        }

        try
        {
            a = actual();
        }
        catch (Exception ex)
        {
            ae = ex;
        }

        if (!step.Contains(" key") && !step.Contains(" value of") && !step.Contains("-> cursor")) Log($"{step}: {e}{(ee != null ? " threw " + ee.GetType().Name : "")}");
        if (allowActualSuccess && ee != null) return;
        if (ee == null != (ae == null))
            Assert.Fail($"{step}: expected {ee?.GetType().Name ?? "no exception"}, actual {ae?.GetType().Name ?? "no exception"} {ae}{Environment.NewLine}{History}");
        if (!Equals(e, a)) Assert.Fail($"{step}: expected {e}, actual {a}{Environment.NewLine}{History}");
    }

    // A BTreeKeyValueDB cursor disturbed by another cursor's write reports itself invalid, refuses to read and can
    // track a wrong key index. So where the oracle cursor is valid both must be on the same key; the LSM cursor's key
    // index is always checked against the rank computed from scratch in the oracle; and a disturbed oracle cursor is
    // re-synchronized to the LSM cursor's key when that key still exists.
    void CompareCursor(string step, CursorPair cursor)
    {
        var actualKey = ((LsmKeyValueDBCursor)cursor.A).DebugKey;
        var expectedIndex = actualKey == null ? -1 : OracleIndex(cursor.E.Transaction, actualKey);
        Same(step + " key index", () => expectedIndex, () => cursor.A.GetKeyIndex());
        if (!cursor.E.IsValid())
        {
            if (actualKey != null && cursor.A.IsValid() && cursor.E.Transaction.CreateCursor() is var probe)
            {
                using (probe)
                    if (probe.Find(actualKey, 0) == FindResult.Exact)
                        cursor.E.Find(actualKey, 0);
            }

            return;
        }

        if (!cursor.A.IsValid()) Assert.Fail($"{step}: IsValid expected True{Environment.NewLine}{History}");
        var key = GetKey(cursor.E);
        Same(step + " key", () => Hex(key), () => Hex(GetKey(cursor.A)));
        // A BTreeKeyValueDB cursor can show a stale value after another cursor's SetValue; read it fresh.
        using var fresh = cursor.E.Transaction.CreateCursor();
        fresh.Find(key, 0);
        Same(step + " value", () => Hex(GetValue(fresh)), () => Hex(GetValue(cursor.A)));
    }

    /// Index of key, or for an erased key the index of the key before it, computed by a fresh oracle cursor.
    static long OracleIndex(IKeyValueDBTransaction transaction, byte[] key)
    {
        using var cursor = transaction.CreateCursor();
        return cursor.Find(key, 0) switch
        {
            FindResult.Exact => cursor.GetKeyIndex(),
            FindResult.Previous => cursor.GetKeyIndex(),
            FindResult.Next => cursor.GetKeyIndex() - 1,
            _ => -1
        };
    }

    static string Hex(byte[] bytes) => Convert.ToHexString(bytes);

    static string FreshIndex(IKeyValueDBCursor cursor)
    {
        try
        {
            var key = ((LsmKeyValueDBCursor)cursor).DebugKey;
            if (key == null) return "-";
            using var fresh = cursor.Transaction.CreateCursor();
            var result = fresh.Find(key, 0);
            return $"{Hex(key)} {result} {fresh.GetKeyIndex()} count {cursor.Transaction.GetKeyValueCount()}";
        }
        catch (Exception ex)
        {
            return ex.GetType().Name;
        }
    }

    static string Keys(IKeyValueDBTransaction transaction)
    {
        using var cursor = transaction.CreateCursor();
        var keys = new List<string>();
        if (cursor.FindFirstKey([]))
            do keys.Add(Hex(GetKey(cursor))); while (cursor.FindNextKey([]));
        return string.Join(",", keys);
    }

    static string Components(IKeyValueDBTransaction transaction, byte[] key)
    {
        var view = ((LsmKeyValueDBTransaction)transaction).View;
        var parts = new List<string>();
        foreach (var component in view.Components)
        {
            var found = component.TryGet(key, out var entry);
            var seek = component.SeekGE(key, true, out var seekEntry);
            parts.Add($"[{component.GetType().Name} get {found} {(found ? $"{entry.IsSet}/{entry.ExistsBelow}/{Hex(entry.Value.ToArray())}" : "")} seek {(seek ? Hex(seekEntry.Key) + "=" + Hex(seekEntry.Value.ToArray()) : "-")} covers {component.Tombstones.Covers(key)}]");
        }

        return string.Join(" ", parts);
    }

    static string FreshExpected(CursorPair c)
    {
        var key = ((LsmKeyValueDBCursor)c.A).DebugKey;
        if (key == null) return "-";
        using var fresh = c.E.Transaction.CreateCursor();
        var result = fresh.Find(key, 0);
        return $"{result} {fresh.GetKeyIndex()}";
    }

    static string TryIndex(IKeyValueDBCursor cursor)
    {
        try
        {
            return cursor.GetKeyIndex().ToString();
        }
        catch (Exception ex)
        {
            return ex.GetType().Name;
        }
    }

    static byte[] GetKey(IKeyValueDBCursor cursor)
    {
        Memory<byte> buffer = default;
        return cursor.GetKeyMemory(ref buffer, true).ToArray();
    }

    static byte[] GetValue(IKeyValueDBCursor cursor)
    {
        Memory<byte> buffer = default;
        return cursor.GetValueMemory(ref buffer, true).ToArray();
    }

    void CompareAll(Pair pair, string step = "compare all")
    {
        using var expected = pair.E.CreateCursor();
        using var actual = pair.A.CreateCursor();
        var e = expected.FindFirstKey([]);
        var a = actual.FindFirstKey([]);
        long index = 0;
        while (true)
        {
            Assert.Equal(e, a);
            if (!e) break;
            Same(step + " key", () => Hex(GetKey(expected)), () => Hex(GetKey(actual)));
            Same(step + " value of " + Hex(GetKey(expected)), () => Hex(GetValue(expected)), () => Hex(GetValue(actual)));
            Assert.Equal(index, actual.GetKeyIndex());
            index++;
            e = expected.FindNextKey([]);
            a = actual.FindNextKey([]);
        }

        Assert.Equal(pair.E.GetKeyValueCount(), index);
    }

    static byte[] RandomKey(Random random)
    {
        var key = new byte[random.Next(4)];
        for (var i = 0; i < key.Length; i++) key[i] = (byte)(random.Next(4) switch { 0 => 0, 1 => 1, 2 => 7, _ => 255 });
        return key;
    }

    static byte[] RandomPrefix(Random random)
    {
        var key = RandomKey(random);
        return key[..random.Next(Math.Min(key.Length, 2) + 1)];
    }

    static byte[] RandomValue(Random random)
    {
        var value = new byte[random.Next(3) == 0 ? random.Next(200) : random.Next(4)];
        random.NextBytes(value);
        return value;
    }

    sealed record Pair(IKeyValueDBTransaction E, IKeyValueDBTransaction A) : IDisposable
    {
        public void Dispose()
        {
            E.Dispose();
            A.Dispose();
        }
    }

    sealed record CursorPair(IKeyValueDBCursor E, IKeyValueDBCursor A) : IDisposable
    {
        public void Dispose()
        {
            E.Dispose();
            A.Dispose();
        }
    }
}
