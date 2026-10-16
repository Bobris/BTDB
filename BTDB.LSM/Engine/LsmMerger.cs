using System;
using System.Collections.Generic;
using BTDB.LSM.Format;

namespace BTDB.LSM.Engine;

/// Merges a contiguous run of components into one, preserving every key's delta sum (ImplementationPlan.md, sections
/// 6.2, 6.3 and 13). The run is newest first; below is the view of all components older than the run. Entries are
/// produced lazily so compaction output can be streamed into tables.
sealed class LsmMerger
{
    readonly ILsmComponent[] _run;
    readonly List<(byte[] Start, byte[] End)> _spans = [];

    public LsmMerger(ILsmComponent[] run, LsmView below)
    {
        _run = run;
        // Union of all fragments of the run masks the state below the run.
        var fragments = new List<LsmTombstone>();
        foreach (var component in run) fragments.AddRange(component.Tombstones.Items);
        fragments.Sort((a, b) => LsmKeys.Compare(a.Start, b.Start));
        foreach (var fragment in fragments)
        {
            if (_spans.Count > 0 && LsmKeys.Compare(fragment.Start, _spans[^1].End) <= 0)
            {
                if (LsmKeys.Compare(fragment.End, _spans[^1].End) > 0) _spans[^1] = (_spans[^1].Start, fragment.End);
            }
            else _spans.Add((fragment.Start, fragment.End));
        }

        var tombstones = new List<LsmTombstone>();
        foreach (var (start, end) in _spans)
        {
            var coveredBelow = below.Rank(end) - below.Rank(start);
            if (coveredBelow > 0) tombstones.Add(new(start, end, coveredBelow));
        }

        Tombstones = new([.. tombstones]);
    }

    /// Merged fragments; fragments masking nothing below the run are dropped.
    public TombstoneSet Tombstones { get; }

    public IEnumerable<LsmEntry> Entries()
    {
        var run = _run;
        var spans = _spans;
        var enumerators = new IEnumerator<LsmEntry>[run.Length];
        var has = new bool[run.Length];
        for (var i = 0; i < run.Length; i++)
        {
            enumerators[i] = run[i].Enumerate().GetEnumerator();
            has[i] = enumerators[i].MoveNext();
        }

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

                if (key == null) yield break;
                while (spanIndex < spans.Count && LsmKeys.Compare(spans[spanIndex].End, key) <= 0) spanIndex++;
                var covered = spanIndex < spans.Count && LsmKeys.Compare(spans[spanIndex].Start, key) <= 0;

                // The newest decision wins: a point, or a fragment masking everything older.
                var decided = false;
                var isSet = false;
                ReadOnlyMemory<byte> value = default;
                var separated = false;
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
                            separated = enumerators[i].Current.IsSeparated;
                        }
                        else if (run[i].Tombstones.Covers(key)) decided = true;
                    }

                    // Uncovered keys: the oldest point's ExistsBelow describes the state below the run.
                    if (atKey && !covered) existsBelow = enumerators[i].Current.ExistsBelow;
                }

                if (covered) existsBelow = false;
                var result = isSet || existsBelow
                    ? new LsmEntry(key, isSet, existsBelow, isSet ? value : default, isSet && separated)
                    : default;

                for (var i = 0; i < run.Length; i++)
                {
                    if (has[i] && LsmKeys.Compare(enumerators[i].Current.Key, key) == 0)
                        has[i] = enumerators[i].MoveNext();
                }

                if (result.Key != null) yield return result;
            }
        }
        finally
        {
            foreach (var enumerator in enumerators) enumerator.Dispose();
        }
    }
}
