using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.LSM.Format;

namespace BTDB.LSM.Engine;

sealed class LsmCompactionOptions
{
    public int Level0Trigger { get; init; } = 4;
    public long LevelBaseBytes { get; init; } = 256L * 1024 * 1024;
    public int LevelMultiplier { get; init; } = 10;
    public long TargetFileSize { get; init; } = 64L * 1024 * 1024;

    /// Value files whose live ratio drops below this are rewritten by compactions touching their values.
    public double ValueGcThreshold { get; init; } = 0.5;
}

/// Moves values out of tables (ImplementationPlan.md, section 10): inline values above the threshold go to a value
/// file, and separated values in mostly dead value files are copied to a new one.
sealed class ValueSeparation(int threshold, ValueFileWriter writer, Func<ulong, bool> rewriteFile,
    Func<ValueHandle, byte[]> readValue)
{
    public LsmEntry Apply(in LsmEntry entry)
    {
        if (!entry.IsSet) return entry;
        if (entry.IsSeparated)
        {
            var handle = ValueHandle.Decode(entry.Value.Span);
            if (!rewriteFile(handle.FileNumber)) return entry;
            return Separate(entry, readValue(handle));
        }

        return entry.Value.Length > threshold ? Separate(entry, entry.Value.Span) : entry;
    }

    LsmEntry Separate(in LsmEntry entry, ReadOnlySpan<byte> value) =>
        new(entry.Key, true, entry.ExistsBelow, writer.Add(value).Encode(), true);

    public void Finish() => writer.Finish();
}

/// One compaction: all inputs of Level merge with the overlapping tables of Level + 1. Level 0 always takes every L0
/// table, so no newer L0 table is left above an older merged one. A rewrite compacts one table into its own level to
/// drop references to mostly dead value files.
sealed record LsmCompactionPlan(int Level, TableReader[] Inputs, TableReader[] Overlaps, bool Rewrite = false)
{
    public int OutputLevel => Rewrite ? Level : Level + 1;

    /// A single input without overlaps moves down unchanged: the state below its span is the same at both levels.
    public bool IsTrivialMove => !Rewrite && Overlaps.Length == 0 && Inputs.Length == 1;
}

/// Leveled compaction (ImplementationPlan.md, section 13): scores, input selection, the count-preserving merge and
/// output split into tables of TargetFileSize with tombstones clipped to each table's span.
static class LsmCompaction
{
    public static long LevelTarget(LsmCompactionOptions options, int level)
    {
        var target = options.LevelBaseBytes;
        for (var i = 1; i < level; i++) target *= options.LevelMultiplier;
        return target;
    }

    public static LsmCompactionPlan? Pick(LsmVersion version, LsmCompactionOptions options, byte[]?[] pointers)
    {
        var bestLevel = -1;
        var bestScore = 1.0;
        if (version.Level0.Length >= options.Level0Trigger)
        {
            bestLevel = 0;
            bestScore = (double)version.Level0.Length / options.Level0Trigger;
        }

        // The bottom level is never compacted further down.
        for (var level = 1; level < version.LevelCount - 1; level++)
        {
            var score = (double)version.LevelBytes(level) / LevelTarget(options, level);
            if (score > bestScore)
            {
                bestLevel = level;
                bestScore = score;
            }
        }

        if (bestLevel < 0) return PickValueRewrite(version, options);
        if (bestLevel == 0)
        {
            var inputs = version.Level0;
            var lower = inputs.Select(t => t.Lower).Min(LsmKeys.Comparer)!;
            var upper = inputs.Select(t => t.Upper).Max(LsmKeys.Comparer)!;
            return new(0, inputs, version.Overlapping(1, lower, upper));
        }

        var tables = version.Levels[bestLevel];
        var pointer = pointers[bestLevel];
        var chosen = pointer == null ? tables[0] : tables.FirstOrDefault(t => LsmKeys.Compare(t.Lower, pointer) > 0) ?? tables[0];
        pointers[bestLevel] = chosen.Lower;
        return new(bestLevel, [chosen], version.Overlapping(bestLevel + 1, chosen.Lower, chosen.Upper));
    }

    static LsmCompactionPlan? PickValueRewrite(LsmVersion version, LsmCompactionOptions options)
    {
        for (var level = 1; level < version.LevelCount; level++)
        foreach (var table in version.Levels[level])
        {
            if (table.ValueReferences.Keys.Any(f => version.LiveValueRatio(f) < options.ValueGcThreshold))
                return new(level, [table], [], true);
        }

        return null;
    }

    /// Runs plan; returns the new version and the tables it made obsolete.
    public static (LsmVersion Version, TableReader[] Obsolete) Execute(LsmVersion version, LsmCompactionPlan plan,
        LsmCompactionOptions options, TableBuildOptions tableOptions, Func<byte[], TableReader> writeTable,
        ValueSeparation values)
    {
        if (plan.IsTrivialMove)
        {
            var moved = plan.Level == 0
                ? version.WithLevel0([]).Replace(1, [], plan.Inputs)
                : version.Replace(plan.Level, plan.Inputs, []).Replace(plan.OutputLevel, [], plan.Inputs);
            return (moved, []);
        }

        var run = new List<ILsmComponent>();
        if (plan.Level == 0) run.AddRange(plan.Inputs);
        else run.Add(new LevelComponent(plan.Inputs));
        if (plan.Overlaps.Length > 0) run.Add(new LevelComponent(plan.Overlaps));
        var below = new LsmView(version.ComponentsBelow(plan.OutputLevel));
        var outputs = WriteOutputs(new LsmMerger([.. run], below), below, options, tableOptions, writeTable, values);
        var next = plan.Level == 0
            ? version.WithLevel0([])
            : version.Replace(plan.Level, plan.Inputs, []);
        next = next.Replace(plan.OutputLevel, plan.Overlaps, outputs);
        return (next, [.. plan.Inputs, .. plan.Overlaps]);
    }

    /// Merges every table into the bottom level.
    public static (LsmVersion Version, TableReader[] Obsolete) CompactAll(LsmVersion version,
        LsmCompactionOptions options, TableBuildOptions tableOptions, Func<byte[], TableReader> writeTable,
        ValueSeparation values)
    {
        var obsolete = version.AllTables().Select(t => t.Table).ToArray();
        var empty = LsmVersion.Empty(version.LevelCount);
        if (obsolete.Length == 0) return (version, []);
        var outputs = WriteOutputs(new LsmMerger(version.Components, new LsmView([])), new LsmView([]), options,
            tableOptions, writeTable, values);
        return (new LsmVersion(empty.Level0, empty.Levels, version.ValueFiles)
            .Replace(version.LevelCount - 1, [], outputs), obsolete);
    }

    /// Streams merged entries into tables of about TargetFileSize. Table i spans [first key of table i, first key of
    /// table i + 1); fragments crossing a boundary are split and each part gets its own CoveredBelow from below.
    static List<TableReader> WriteOutputs(LsmMerger merger, LsmView below, LsmCompactionOptions options,
        TableBuildOptions tableOptions, Func<byte[], TableReader> writeTable, ValueSeparation values)
    {
        var outputs = new List<TableReader>();
        var tombstones = merger.Tombstones;
        TableBuilder? builder = null;
        byte[]? lower = null;
        foreach (var merged in merger.Entries())
        {
            var entry = values.Apply(merged);
            if (builder != null && builder.EstimatedSize >= options.TargetFileSize)
            {
                outputs.Add(writeTable(builder.Finish(Clip(tombstones, lower, entry.Key, below))));
                builder = null;
                lower = entry.Key;
            }

            builder ??= new(tableOptions);
            builder.Add(entry);
        }

        if (builder != null || tombstones.Count > 0)
        {
            var last = Clip(tombstones, lower, null, below);
            if (builder != null || last.Count > 0)
                outputs.Add(writeTable((builder ?? new TableBuilder(tableOptions)).Finish(last)));
        }

        values.Finish();
        return outputs;
    }

    /// Fragments clipped to [lower, upper); null bounds are unbounded.
    static TombstoneSet Clip(TombstoneSet tombstones, byte[]? lower, byte[]? upper, LsmView below)
    {
        var result = new List<LsmTombstone>();
        foreach (var fragment in tombstones.Items)
        {
            var start = lower != null && LsmKeys.Compare(fragment.Start, lower) < 0 ? lower : fragment.Start;
            var end = upper != null && LsmKeys.Compare(fragment.End, upper) > 0 ? upper : fragment.End;
            if (LsmKeys.Compare(start, end) >= 0) continue;
            if (start == fragment.Start && end == fragment.End)
            {
                result.Add(fragment);
                continue;
            }

            var coveredBelow = below.Rank(end) - below.Rank(start);
            if (coveredBelow > 0) result.Add(new(start, end, coveredBelow));
        }

        return new([.. result]);
    }
}
