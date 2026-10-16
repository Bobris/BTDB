using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.LSM.Format;

namespace BTDB.LSM.Engine;

/// Immutable table set (ImplementationPlan.md, section 11): L0 tables newest first, each its own component because L0
/// tables may overlap, and levels 1..n of non-overlapping tables sorted by key, each presented as one component.
sealed class LsmVersion
{
    public LsmVersion(TableReader[] level0, TableReader[][] levels, IReadOnlyDictionary<ulong, long> valueFiles)
    {
        Level0 = level0;
        Levels = levels;
        ValueFiles = valueFiles;
        var live = new Dictionary<ulong, long>();
        foreach (var (_, table) in AllTables())
        foreach (var (fileNumber, bytes) in table.ValueReferences)
            live[fileNumber] = live.GetValueOrDefault(fileNumber) + bytes;
        LiveValueBytes = live;
        var components = new List<ILsmComponent>(level0);
        LevelComponents = new LevelComponent?[levels.Length];
        for (var level = 1; level < levels.Length; level++)
        {
            if (levels[level].Length == 0) continue;
            LevelComponents[level] = new(levels[level]);
            components.Add(LevelComponents[level]!);
        }

        Components = [.. components];
    }

    public static LsmVersion Empty(int levelCount) =>
        new([], Enumerable.Range(0, levelCount).Select(_ => Array.Empty<TableReader>()).ToArray(),
            new Dictionary<ulong, long>());

    /// Value files of this version and their total stored bytes.
    public IReadOnlyDictionary<ulong, long> ValueFiles { get; }

    /// Stored bytes still referenced by the tables of this version, per value file.
    public IReadOnlyDictionary<ulong, long> LiveValueBytes { get; }

    public double LiveValueRatio(ulong fileNumber) =>
        ValueFiles.TryGetValue(fileNumber, out var total) && total > 0
            ? (double)LiveValueBytes.GetValueOrDefault(fileNumber) / total
            : 1;

    /// Adds value files and drops those no table references any more; returns the dropped ones.
    public LsmVersion WithValueFiles(IEnumerable<(ulong FileNumber, long Bytes)> added, out ulong[] dropped)
    {
        var files = new Dictionary<ulong, long>(ValueFiles);
        foreach (var (fileNumber, bytes) in added) files.Add(fileNumber, bytes);
        dropped = files.Keys.Where(f => !LiveValueBytes.ContainsKey(f)).ToArray();
        foreach (var fileNumber in dropped) files.Remove(fileNumber);
        return new(Level0, Levels, files);
    }

    public TableReader[] Level0 { get; }

    /// Index 0 is unused; Levels[n] holds level n sorted by key.
    public TableReader[][] Levels { get; }

    public LevelComponent?[] LevelComponents { get; }

    /// All table components, newest first.
    public ILsmComponent[] Components { get; }

    public int LevelCount => Levels.Length;

    public IEnumerable<(int Level, TableReader Table)> AllTables()
    {
        foreach (var table in Level0) yield return (0, table);
        for (var level = 1; level < Levels.Length; level++)
        foreach (var table in Levels[level])
            yield return (level, table);
    }

    public long LevelBytes(int level) => level == 0 ? Level0.Sum(t => t.Length) : Levels[level].Sum(t => t.Length);

    /// Components of all levels strictly below level (level >= 1).
    public ILsmComponent[] ComponentsBelow(int level)
    {
        var result = new List<ILsmComponent>();
        for (var i = level + 1; i < Levels.Length; i++)
            if (LevelComponents[i] is { } component)
                result.Add(component);
        return [.. result];
    }

    /// Tables of level (>= 1) whose span overlaps [lower, upper).
    public TableReader[] Overlapping(int level, byte[] lower, byte[] upper) => Levels[level]
        .Where(t => LsmKeys.Compare(t.Upper, lower) > 0 && LsmKeys.Compare(t.Lower, upper) < 0).ToArray();

    public LsmVersion WithLevel0(TableReader[] level0) => new(level0, Levels, ValueFiles);

    /// Replaces tables of level by others; the result is kept sorted by key.
    public LsmVersion Replace(int level, IReadOnlyCollection<TableReader> removed, IEnumerable<TableReader> added)
    {
        if (level == 0) throw new ArgumentOutOfRangeException(nameof(level));
        var levels = (TableReader[][])Levels.Clone();
        levels[level] = Levels[level].Where(t => !removed.Contains(t)).Concat(added)
            .OrderBy(t => t.Lower, LsmKeys.Comparer).ToArray();
        return new(Level0, levels, ValueFiles);
    }
}
