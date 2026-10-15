using System;
using System.Collections.Generic;
using BTDB.LSM.Format;

namespace BTDB.LSM.Engine;

/// Merges a contiguous run of components into one, preserving every key's delta sum (ImplementationPlan.md, sections
/// 6.2, 6.3 and 13). The run is newest first; below is the view of all components older than the run.
static class LsmMerger
{
    public static (List<LsmEntry> Entries, TombstoneSet Tombstones) Merge(ReadOnlySpan<ILsmComponent> run,
        LsmView below, int belowFirst)
    {
        // Union of all fragments of the run masks the state below the run.
        var fragments = new List<LsmTombstone>();
        foreach (var component in run) fragments.AddRange(component.Tombstones.Items);
        fragments.Sort((a, b) => LsmKeys.Compare(a.Start, b.Start));
        var spans = new List<(byte[] Start, byte[] End)>();
        foreach (var fragment in fragments)
        {
            if (spans.Count > 0 && LsmKeys.Compare(fragment.Start, spans[^1].End) <= 0)
            {
                if (LsmKeys.Compare(fragment.End, spans[^1].End) > 0) spans[^1] = (spans[^1].Start, fragment.End);
            }
            else spans.Add((fragment.Start, fragment.End));
        }

        var enumerators = new IEnumerator<LsmEntry>[run.Length];
        var has = new bool[run.Length];
        for (var i = 0; i < run.Length; i++)
        {
            enumerators[i] = run[i].Enumerate().GetEnumerator();
            has[i] = enumerators[i].MoveNext();
        }

        var entries = new List<LsmEntry>();
        var spanIndex = 0;
        try
        {
            while (true)
            {
                byte[]? key = null;
                for (var i = 0; i < run.Length; i++)
                {
                    if (has[i] && (key == null || LsmKeys.Compare(enumerators[i].Current.Key, key) < 0))
                        key = enumerators[i].Current.Key;
                }

                if (key == null) break;
                while (spanIndex < spans.Count && LsmKeys.Compare(spans[spanIndex].End, key) <= 0) spanIndex++;
                var covered = spanIndex < spans.Count && LsmKeys.Compare(spans[spanIndex].Start, key) <= 0;

                // The newest decision wins: a point, or a fragment masking everything older.
                var decided = false;
                var isSet = false;
                ReadOnlyMemory<byte> value = default;
                var existsBelow = false;
                for (var i = 0; i < run.Length; i++)
                {
                    var atKey = has[i] && LsmKeys.Compare(enumerators[i].Current.Key, key) == 0;
                    if (!decided)
                    {
                        if (atKey)
                        {
                            decided = true;
                            isSet = enumerators[i].Current.IsSet;
                            value = enumerators[i].Current.Value;
                        }
                        else if (run[i].Tombstones.Covers(key)) decided = true;
                    }

                    // Uncovered keys: the oldest point's ExistsBelow describes the state below the run.
                    if (atKey && !covered) existsBelow = enumerators[i].Current.ExistsBelow;
                }

                if (covered) existsBelow = false;
                if (isSet || existsBelow) entries.Add(new(key, isSet, existsBelow, isSet ? value : default));

                for (var i = 0; i < run.Length; i++)
                {
                    if (has[i] && LsmKeys.Compare(enumerators[i].Current.Key, key) == 0)
                        has[i] = enumerators[i].MoveNext();
                }
            }
        }
        finally
        {
            foreach (var enumerator in enumerators) enumerator.Dispose();
        }

        var tombstones = new List<LsmTombstone>();
        foreach (var (start, end) in spans)
        {
            var coveredBelow = below.RankFrom(belowFirst, end) - below.RankFrom(belowFirst, start);
            if (coveredBelow > 0) tombstones.Add(new(start, end, coveredBelow));
        }

        return (entries, new([.. tombstones]));
    }
}
