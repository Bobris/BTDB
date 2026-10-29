using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using BTDB.LSM.Engine;
using Xunit;

namespace BTDB.LSM.Test.Counting;

/// Drives NativeMemtable and the managed reference Memtable with the same random operations and compares every query,
/// including over older versions kept as snapshots, with trees deep enough to split leaves and branches.
public class NativeMemtableTest
{
    [Theory]
    [InlineData(1, 50_000, 200_000, 50)]
    [InlineData(2, 2_000, 100_000, 5)]
    [InlineData(3, 200_000, 100_000, 1000)]
    public void MatchesManagedMemtable(int seed, int keySpace, int operations, int operationsPerVersion)
    {
        var random = new Random(seed);
        IMemtable native = NativeMemtable.CreateEmpty(64 * 1024 * 1024);
        IMemtable managed = Memtable.CreateEmpty();
        var owner = 1000L;
        var snapshots = new List<(IMemtable Native, IMemtable Managed, List<(byte[], bool, bool, byte[])> Content)>();
        try
        {
            for (var op = 0; op < operations; op++)
            {
                if (op % operationsPerVersion == 0)
                {
                    if (random.Next(20) == 0)
                        snapshots.Add((native, managed, Content(managed)));
                    owner++;
                    native = native.CloneForWriting(owner);
                    managed = managed.CloneForWriting(owner);
                }

                var key = Key(random.Next(keySpace));
                var choice = random.Next(100);
                Assert.Equal(managed.TryGetNode(key, out var isSet, out var existsBelow), native.TryGetNode(key, out var nIsSet, out var nExistsBelow));
                Assert.Equal(isSet, nIsSet);
                Assert.Equal(existsBelow, nExistsBelow);
                var value = new byte[random.Next(3) == 0 ? random.Next(300) : random.Next(8)];
                random.NextBytes(value);
                if (choice < 55)
                {
                    if (managed.TryGetNode(key, out _, out _))
                    {
                        var set = random.Next(4) != 0;
                        Assert.True(managed.UpdateExisting(key, set, value));
                        Assert.True(native.UpdateExisting(key, set, value));
                    }
                    else
                    {
                        var set = random.Next(4) != 0;
                        var below = random.Next(2) == 0;
                        managed.Insert(key, set, below, value);
                        native.Insert(key, set, below, value);
                    }
                }
                else if (choice < 80)
                {
                    managed.Remove(key);
                    native.Remove(key);
                }
                else if (choice < 83)
                {
                    var end = Key(random.Next(keySpace));
                    managed.RemoveRange(key, end);
                    native.RemoveRange(key, end);
                }

                Assert.Equal(managed.EntryCount, native.EntryCount);
                Assert.Equal(managed.DeltaSum, native.DeltaSum);
                var probe = random.Next(4) == 0 ? (byte[])[] : Key(random.Next(keySpace));
                Assert.Equal(managed.PointDeltaBefore(probe), native.PointDeltaBefore(probe));
                Assert.Equal(managed.CountBlocksBefore(probe, true), native.CountBlocksBefore(probe, true));
                Assert.Equal(managed.CountBlocksBefore(probe, false), native.CountBlocksBefore(probe, false));
                var inclusive = random.Next(2) == 0;
                SameEntry(managed.SeekGE(probe, inclusive, out var m), m, native.SeekGE(probe, inclusive, out var n), n);
                SameEntry(managed.SeekLE(probe, inclusive, out m), m, native.SeekLE(probe, inclusive, out n), n);
                SameEntry(managed.SeekLast(out m), m, native.SeekLast(out n), n);
                if (managed.BlockCount > 0)
                {
                    var block = random.Next(managed.BlockCount);
                    Assert.Equal(managed.BlockFirstKey(block), native.BlockFirstKey(block));
                }
            }

            Assert.Equal(Content(managed), Content(native), new ContentComparer());
            foreach (var (oldNative, oldManaged, content) in snapshots)
            {
                Assert.Equal(content, Content(oldManaged), new ContentComparer());
                Assert.Equal(content, Content(oldNative), new ContentComparer());
            }
        }
        finally
        {
            native.Arena!.Dispose();
        }
    }

    static void SameEntry(bool expectedFound, LsmEntry expected, bool actualFound, LsmEntry actual)
    {
        Assert.Equal(expectedFound, actualFound);
        if (!expectedFound) return;
        Assert.Equal(expected.Key, actual.Key);
        Assert.Equal(expected.IsSet, actual.IsSet);
        Assert.Equal(expected.ExistsBelow, actual.ExistsBelow);
        Assert.Equal(expected.Value.ToArray(), actual.Value.ToArray());
    }

    static List<(byte[], bool, bool, byte[])> Content(IMemtable memtable) =>
        memtable.Enumerate().Select(e => (e.Key, e.IsSet, e.ExistsBelow, e.Value.ToArray())).ToList();

    sealed class ContentComparer : IEqualityComparer<List<(byte[], bool, bool, byte[])>>
    {
        public bool Equals(List<(byte[], bool, bool, byte[])>? x, List<(byte[], bool, bool, byte[])>? y) =>
            x!.Count == y!.Count && x.Zip(y).All(p => p.First.Item1.AsSpan().SequenceEqual(p.Second.Item1) &&
                                                       p.First.Item2 == p.Second.Item2 &&
                                                       p.First.Item3 == p.Second.Item3 &&
                                                       p.First.Item4.AsSpan().SequenceEqual(p.Second.Item4));

        public int GetHashCode(List<(byte[], bool, bool, byte[])> obj) => obj.Count;
    }

    static byte[] Key(int value)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, value);
        return key;
    }

    // Before the fix a size near int.MaxValue rounded up to a negative one, so the arena returned its current pointer
    // and moved it backwards instead of failing.
    [Theory]
    [InlineData(int.MaxValue)]
    [InlineData(int.MaxValue - 6)]
    [InlineData(-1)]
    public void ArenaRejectsSizesItCannotRound(int size)
    {
        using var arena = new BTDB.LSM.Memory.NativeArena(4096);
        // Allocate returns a pointer, which this project may not touch outside unsafe code.
        var allocate = typeof(BTDB.LSM.Memory.NativeArena).GetMethod(nameof(BTDB.LSM.Memory.NativeArena.Allocate))!;
        var error = Assert.Throws<System.Reflection.TargetInvocationException>(() => allocate.Invoke(arena, [size]));
        Assert.IsType<ArgumentOutOfRangeException>(error.InnerException);
        Assert.Equal(0, arena.UsedBytes);
    }
}
