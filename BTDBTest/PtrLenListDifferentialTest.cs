using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

// Compares PtrLenList with a set of positions under random includes, excludes, searches and merges. The list keeps
// sorted, disjoint, non-adjacent ranges; including returns whether no position was included before and excluding
// whether all were.
public class PtrLenListDifferentialTest
{
    const int Size = 128;

    static List<(ulong Pos, ulong Len)> Runs(bool[] set)
    {
        var res = new List<(ulong, ulong)>();
        for (var i = 0; i < Size; i++)
        {
            if (!set[i]) continue;
            var start = i;
            while (i < Size && set[i]) i++;
            res.Add(((ulong)start, (ulong)(i - start)));
        }

        return res;
    }

    static void AssertSame(bool[] set, PtrLenList list)
    {
        Assert.Equal(Runs(set), Ranges(list));
        Assert.Equal(!set.Any(b => b), list.Empty);
    }

    static List<(ulong, ulong)> Ranges(PtrLenList list)
    {
        var ranges = new List<(ulong, ulong)>();
        foreach (var range in list) ranges.Add((range.Key, range.Value));
        return ranges;
    }

    // Before the fix excluding a range whose end is in a gap moved the start of the next range back to that end, so
    // excluding [5, 8) turned [20, 24) into [8, 24).
    [Theory]
    [InlineData(5, 3, 10, 2)]
    [InlineData(14, 3, 10, 2)]
    [InlineData(11, 4, 10, 1)]
    public void ExcludeEndingInGapKeepsNextRange(int pos, int len, int firstPos, int firstLen)
    {
        var list = new PtrLenList();
        list.TryInclude(10, 2);
        list.TryInclude(20, 4);
        Assert.False(list.TryExclude((ulong)pos, (ulong)len));
        Assert.Equal([((ulong)firstPos, (ulong)firstLen), (20UL, 4UL)], Ranges(list));
    }

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 60).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void MatchesSetOfPositions(int seed)
    {
        var random = new Random(seed);
        var list = new PtrLenList();
        var set = new bool[Size];
        for (var step = 0; step < 3000; step++)
        {
            var before = string.Join(" ", Runs(set));
            var lastOp = "";
            var pos = random.Next(64);
            var len = random.Next(random.Next(2) == 0 ? 4 : 12);
            switch (random.Next(8))
            {
                case 0:
                case 1:
                {
                    var expected = Enumerable.Range(pos, len).All(i => !set[i]);
                    lastOp = $"include {pos} {len}";
                    Assert.Equal(expected, list.TryInclude((ulong)pos, (ulong)len));
                    for (var i = pos; i < pos + len; i++) set[i] = true;
                    break;
                }
                case 2:
                case 3:
                {
                    var expected = Enumerable.Range(pos, len).All(i => set[i]);
                    lastOp = $"exclude {pos} {len}";
                    Assert.Equal(expected, list.TryExclude((ulong)pos, (ulong)len));
                    for (var i = pos; i < pos + len; i++) set[i] = false;
                    break;
                }
                case 4:
                {
                    var expected = pos;
                    while (Enumerable.Range(expected, len).Any(i => i < Size && set[i])) expected++;
                    Assert.Equal((ulong)expected, list.FindFreeSizeAfter((ulong)pos, (ulong)len));
                    break;
                }
                case 5:
                {
                    if (len == 0) break;
                    lastOp = $"findremove {len}";
                    var run = Runs(set).FirstOrDefault(r => r.Len >= (ulong)len);
                    var found = list.TryFindLenAndRemove((ulong)len, out var at);
                    Assert.Equal(run.Len != 0, found);
                    if (found)
                    {
                        Assert.Equal(run.Pos, at);
                        for (var i = (int)at; i < (int)at + len; i++) set[i] = false;
                    }

                    break;
                }
                case 6:
                {
                    var probe = random.Next(Size);
                    Assert.Equal(set[probe], list.Contains((ulong)probe));
                    break;
                }
                default:
                {
                    var other = new PtrLenList();
                    var otherSet = new bool[Size];
                    for (var k = random.Next(4); k > 0; k--)
                    {
                        var p = random.Next(64);
                        var l = random.Next(1, 8);
                        other.TryInclude((ulong)p, (ulong)l);
                        for (var i = p; i < p + l; i++) otherSet[i] = true;
                    }

                    lastOp = "merge/unmerge";
                    var merged = list.MergeIntoNew(other);
                    var mergedSet = set.Zip(otherSet, (a, b) => a || b).ToArray();
                    AssertSame(mergedSet, merged);
                    if (random.Next(2) == 0)
                    {
                        list.MergeInPlace(other);
                        set = mergedSet;
                    }
                    else
                    {
                        list.UnmergeInPlace(other);
                        set = set.Zip(otherSet, (a, b) => a && !b).ToArray();
                    }

                    break;
                }
            }

            try
            {
                AssertSame(set, list);
            }
            catch (Exception e)
            {
                throw new Exception($"step {step} before [{before}] op {lastOp}", e);
            }
        }
    }
}
