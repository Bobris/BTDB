using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using BTDB.LSM.Counting;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Counting;

public class CountedStackOracleTest(ITestOutputHelper output)
{
    [Theory]
    [InlineData(1, 512, 20_000, 2, 1, 16, 0)]
    [InlineData(2, 512, 20_000, 4, 2, 1, 4)]
    [InlineData(3, 4096, 50_000, 16, 4, 64, 64)]
    [InlineData(4, 4096, 50_000, 64, 8, 8, 1_000_000)]
    [InlineData(5, 64, 20_000, 2, 2, 4, 0)]
    [InlineData(6, 100_000, 50_000, 16, 4, 256, 16)]
    public void MatchesOracle(int seed, int keySpace, int operations, int blockSize, int restartInterval,
        int memtableLimit, int pointDeleteRangeLimit)
    {
        Run(seed, keySpace, operations, blockSize, restartInterval, memtableLimit, pointDeleteRangeLimit,
            fullCheckInterval: 997);
    }

    [Fact]
    public void MatchesOracleForMillionOperations()
    {
        Run(42, 20_000, 1_000_000, 16, 4, 64, 64, fullCheckInterval: 100_003);
    }

    [Fact]
    public void SelectCostIsLogarithmic()
    {
        const int keyCount = 1_000_000;
        var stack = new CountedStack(new() { BlockSize = 64, RestartInterval = 16, PointDeleteRangeLimit = 64 });
        var random = new Random(7);
        // Bulk load every other key into the bottom component.
        for (var i = 0; i < keyCount; i += 2)
        {
            stack.Set(Key(i));
            if (stack.MemtableEntryCount >= 4096) stack.Flush();
        }

        stack.Flush();
        stack.Compact(0, stack.Immutables.Count - 1);
        // Leveled-like shape: each newer component is about ten times smaller, with updates, deletes and ranges.
        var size = keyCount / 20;
        for (var level = 0; level < 5; level++, size /= 10)
        {
            for (var i = 0; i < size; i++)
            {
                var key = random.Next(keyCount);
                if (random.Next(4) == 0) stack.Delete(Key(key));
                else stack.Set(Key(key));
                if (stack.MemtableEntryCount >= 4096)
                {
                    stack.Flush();
                    stack.Compact(0, 1);
                }
            }

            var rangeStart = random.Next(keyCount - 2000);
            stack.DeleteRange(Key(rangeStart), Key(rangeStart + 1000 + random.Next(1000)));
            stack.Flush();
        }

        var before = stack.Stats.Clone();
        const int selects = 2000;
        var count = stack.Count;
        for (var i = 0; i < selects; i++) stack.Select(random.NextInt64(count));
        var after = stack.Stats;
        var components = stack.Immutables.Count;
        double Per(long a, long b) => (double)(a - b) / selects;
        output.WriteLine(
            $"keys={count} components={components} pivots/select={Per(after.SelectPivots, before.SelectPivots):F1} " +
            $"exactPivots/select={Per(after.SelectExactPivots, before.SelectExactPivots):F1} " +
            $"ranks/select={Per(after.RankCalls, before.RankCalls):F1} " +
            $"indexReads/select={Per(after.IndexReads, before.IndexReads):F1} " +
            $"blockReads/select={Per(after.BlockReads, before.BlockReads):F1} " +
            $"steps/select={Per(after.SelectSteps, before.SelectSteps):F1}");
        Assert.True(Per(after.SelectPivots, before.SelectPivots) <= components * Math.Log2(count));

        before = stack.Stats.Clone();
        for (var i = 0; i < selects; i++) stack.Rank(Key(random.Next(keyCount)));
        output.WriteLine($"blockReads/rank={(double)(stack.Stats.BlockReads - before.BlockReads) / selects:F1}");
    }

    void Run(int seed, int keySpace, int operations, int blockSize, int restartInterval, int memtableLimit,
        int pointDeleteRangeLimit, int fullCheckInterval)
    {
        var random = new Random(seed);
        var stack = new CountedStack(new()
        {
            BlockSize = blockSize, RestartInterval = restartInterval, PointDeleteRangeLimit = pointDeleteRangeLimit
        });
        var oracle = new Fenwick(keySpace);
        const int maxComponents = 10;
        for (var op = 0; op < operations; op++)
        {
            var choice = random.Next(100);
            if (choice < 55)
            {
                var key = random.Next(keySpace);
                Assert.Equal(oracle.Set(key, true), stack.Set(Key(key)));
            }
            else if (choice < 70)
            {
                if (oracle.Count > 0 && random.Next(10) != 0)
                {
                    var key = oracle.Select(random.Next(oracle.Count));
                    Assert.True(stack.Delete(Key(key)));
                    oracle.Set(key, false);
                }
                else
                {
                    var key = random.Next(keySpace);
                    Assert.Equal(oracle.Set(key, false), stack.Delete(Key(key)));
                }
            }
            else if (choice < 74)
            {
                var start = random.Next(keySpace);
                // Mostly small and medium ranges; rarely a large one, so the population does not collapse.
                var length = random.Next(1000) switch
                {
                    < 450 => random.Next(4), < 900 => random.Next(16), < 999 => random.Next(keySpace / 256 + 1),
                    _ => random.Next(keySpace / 4 + 1)
                };
                var last = Math.Min(keySpace - 1, start + length);
                long expected = 0;
                for (var key = start; key <= last; key++)
                {
                    if (oracle.Set(key, false)) expected++;
                }

                // Inclusive [start, last] as [Key(start), Key(last) + 0x00).
                Assert.Equal(expected, stack.DeleteRange(Key(start), AfterKey(last)));
            }
            else if (choice < 76)
            {
                stack.Flush();
            }
            else if (choice < 78 && stack.Immutables.Count >= 2)
            {
                var first = random.Next(stack.Immutables.Count - 1);
                stack.Compact(first, random.Next(first + 1, stack.Immutables.Count));
            }

            if (stack.MemtableEntryCount >= memtableLimit) stack.Flush();
            if (stack.Immutables.Count > maxComponents)
            {
                var first = random.Next(stack.Immutables.Count - 1);
                stack.Compact(first, Math.Min(stack.Immutables.Count - 1, first + 1 + random.Next(3)));
            }

            Assert.Equal(oracle.Count, stack.Count);
            CheckRandom(random, stack, oracle, keySpace);
            if (op % fullCheckInterval == fullCheckInterval - 1) CheckFull(stack, oracle, keySpace);
        }

        CheckFull(stack, oracle, keySpace);
        output.WriteLine($"components={stack.Immutables.Count} live={stack.Count} ranks={stack.Stats.RankCalls} " +
                         $"selects={stack.Stats.SelectCalls} pivots={stack.Stats.SelectPivots}");
    }

    static void CheckRandom(Random random, CountedStack stack, Fenwick oracle, int keySpace)
    {
        var key = random.Next(keySpace);
        Assert.Equal(oracle.Get(key), stack.Exists(Key(key)));
        switch (random.Next(3))
        {
            case 0:
                Assert.Equal(oracle.Prefix(key), stack.Rank(Key(key)));
                var (min, max) = stack.RankBounds(Key(key));
                Assert.InRange(oracle.Prefix(key), min, max);
                break;
            case 1:
                Assert.Equal(oracle.Prefix(key + 1), stack.Rank(AfterKey(key)));
                break;
            default:
                Assert.Equal(0, stack.Rank([]));
                break;
        }

        if (oracle.Count > 0)
        {
            var index = random.Next(oracle.Count);
            Assert.Equal(Key(oracle.Select(index)), stack.Select(index));
        }
    }

    static void CheckFull(CountedStack stack, Fenwick oracle, int keySpace)
    {
        var expected = new List<int>();
        for (var key = 0; key < keySpace; key++)
        {
            if (oracle.Get(key)) expected.Add(key);
        }

        var index = 0;
        foreach (var key in stack.LiveKeysFrom(null))
        {
            Assert.True(index < expected.Count);
            Assert.Equal(Key(expected[index]), key);
            Assert.Equal(index, stack.Rank(key));
            index++;
        }

        Assert.Equal(expected.Count, index);
    }

    static byte[] Key(int value)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, value);
        return key;
    }

    static byte[] AfterKey(int value)
    {
        var key = new byte[5];
        BinaryPrimitives.WriteInt32BigEndian(key, value);
        return key;
    }

    /// Oracle set over [0, size) with rank and select through a Fenwick tree.
    sealed class Fenwick(int size)
    {
        readonly int[] _tree = new int[size + 1];
        readonly bool[] _present = new bool[size];

        public int Count { get; private set; }

        public bool Get(int key) => _present[key];

        /// Returns true when the presence changed.
        public bool Set(int key, bool present)
        {
            if (_present[key] == present) return false;
            _present[key] = present;
            var delta = present ? 1 : -1;
            Count += delta;
            for (var i = key + 1; i <= size; i += i & -i) _tree[i] += delta;
            return true;
        }

        /// Number of present keys lower than key.
        public int Prefix(int key)
        {
            var sum = 0;
            for (var i = Math.Min(key, size); i > 0; i -= i & -i) sum += _tree[i];
            return sum;
        }

        public int Select(int index)
        {
            var position = 0;
            var step = 1;
            while (step * 2 <= size) step *= 2;
            for (; step > 0; step /= 2)
            {
                if (position + step <= size && _tree[position + step] <= index)
                {
                    position += step;
                    index -= _tree[position];
                }
            }

            return position;
        }
    }
}
