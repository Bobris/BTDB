using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication.EventLog;

/// <summary>
/// Background merging of sealed splits into immutable objects named by level and first offset. Level L groups are the
/// splits <c>k·F^L+1 .. (k+1)·F^L</c> for fan-out F; a group is eligible once a later split exists, so the tail is never
/// merged. The content is a pure function of the group's records, so concurrent or repeated merges create identical
/// bytes. An object is garbage once a verified object of a higher level covers it; garbage is marked with a deletion
/// deadline and deleted only when the deadline passed and it is still covered.
/// </summary>
internal sealed class EventLogMerger(IEventLogStorage storage, string topic, EventLogOptions options,
    IReplicationScheduler scheduler)
{
    sealed record Merged(string Key, string Version, byte Level, ulong FirstSplit, ulong LastSplit, ulong FirstOffset);

    readonly Dictionary<string, Merged> _known = new(StringComparer.Ordinal);
    readonly HashSet<(string Key, string Version)> _verified = new();

    public sealed record PassResult(int Created, int Marked, int Deleted);

    public async ValueTask<PassResult> RunPassAsync(CancellationToken cancellation)
    {
        if (options.MergeFanOut == 0 || options.MergeLevels == 0) return new(0, 0, 0);
        var splits = new SortedList<ulong, EventLogObjectInfo>();
        var mergedInfos = new List<(EventLogObjectInfo Info, int Level)>();
        await foreach (var info in storage.ListAsync(topic + "/", cancellation).ConfigureAwait(false))
        {
            if (!EventLogFormat.TryParseKey(topic, info.Key, out var level, out var number)) continue;
            if (level == 0) splits[number] = info;
            else if (level <= options.MergeLevels) mergedInfos.Add((info, level));
        }
        if (splits.Count == 0) return new(0, 0, 0);
        var maxSplit = splits.Keys[^1];
        var merged = new List<(Merged Object, EventLogObjectInfo Info)>();
        foreach (var (info, _) in mergedInfos)
            merged.Add((await LearnAsync(info, cancellation).ConfigureAwait(false), info));

        var created = 0;
        for (var level = 1; level <= options.MergeLevels; level++)
        {
            var span = Span(level);
            var groups = new SortedSet<ulong>();
            foreach (var id in splits.Keys) groups.Add((id - 1) / span);
            foreach (var (m, _) in merged)
                if (m.Level < level) groups.Add((m.FirstSplit - 1) / span);
            foreach (var group in groups)
            {
                var first = group * span + 1;
                var last = first + span - 1;
                if (last >= maxSplit) break;
                if (merged.Any(m => m.Object.Level >= level && m.Object.FirstSplit <= first && m.Object.LastSplit >= last))
                    continue;
                var result = await MergeGroupAsync(level, first, last, splits, merged, cancellation).ConfigureAwait(false);
                if (result == null) continue;
                merged.Add(result.Value);
                created++;
            }
        }

        var marked = 0;
        var deleted = 0;
        var now = options.TimeProvider.GetUtcNow();
        foreach (var info in splits.Values.Concat(merged.Select(m => m.Info)))
        {
            if (!await IsGarbageAsync(info, merged, cancellation).ConfigureAwait(false)) continue;
            if (info.DeleteAfter is not { } due)
            {
                if (await storage.ScheduleDeletionAsync(info.Key, info.Version, options.DeletionDelay, cancellation)
                        .ConfigureAwait(false) != null) marked++;
            }
            else if (due <= now && await storage.DeleteAsync(info.Key, info.Version, cancellation).ConfigureAwait(false))
                deleted++;
        }
        return new(created, marked, deleted);
    }

    ulong Span(int level)
    {
        ulong span = 1;
        for (var i = 0; i < level; i++) span = checked(span * (ulong)options.MergeFanOut);
        return span;
    }

    async ValueTask<Merged> LearnAsync(EventLogObjectInfo info, CancellationToken cancellation)
    {
        if (_known.TryGetValue(info.Key, out var known) && known.Version == info.Version) return known;
        var prefix = new byte[EventLogFormat.MergedFixedLength(topic)];
        await storage.ReadRangeAsync(info.Key, info.Version, 0, prefix, cancellation).ConfigureAwait(false);
        var header = EventLogFormat.ParseMergedFixed(prefix, topic);
        EventLogFormat.TryParseKey(topic, info.Key, out var level, out var firstOffset);
        if (header.Level != level || header.FirstOffset != firstOffset)
            throw new EventLogCorruptedException($"Merged object '{info.Key}' does not match its name.");
        var merged = new Merged(info.Key, info.Version, header.Level, header.SplitId, header.LastSplitId, firstOffset);
        _known[info.Key] = merged;
        return merged;
    }

    async ValueTask<bool> IsGarbageAsync(EventLogObjectInfo info, List<(Merged Object, EventLogObjectInfo Info)> merged,
        CancellationToken cancellation)
    {
        EventLogFormat.TryParseKey(topic, info.Key, out var level, out var number);
        ulong first, last;
        if (level == 0) first = last = number;
        else
        {
            var self = merged.First(m => m.Info.Key == info.Key).Object;
            (first, last) = (self.FirstSplit, self.LastSplit);
        }
        foreach (var (cover, coverInfo) in merged)
        {
            if (cover.Level <= level || cover.FirstSplit > first || cover.LastSplit < last) continue;
            if (await VerifyAsync(coverInfo, cancellation).ConfigureAwait(false)) return true;
        }
        return false;
    }

    /// <summary>A covering object must be complete and valid before anything it covers is marked or deleted.</summary>
    async ValueTask<bool> VerifyAsync(EventLogObjectInfo info, CancellationToken cancellation)
    {
        if (_verified.Contains((info.Key, info.Version))) return true;
        var blob = await storage.ReadAsync(info.Key, cancellation).ConfigureAwait(false);
        if (blob == null || blob.Version != info.Version) return false;
        EventLogFormat.Parse(blob.Content.Span, topic);
        _verified.Add((info.Key, info.Version));
        return true;
    }

    async ValueTask<(Merged, EventLogObjectInfo)?> MergeGroupAsync(int level, ulong first, ulong last,
        SortedList<ulong, EventLogObjectInfo> splits, List<(Merged Object, EventLogObjectInfo Info)> merged,
        CancellationToken cancellation)
    {
        var sources = new List<(ReadOnlyMemory<byte> Content, EventLogObject Object)>();
        long length = 0;
        for (var id = first; id <= last;)
        {
            var lower = merged.Where(m => m.Object.Level < level && m.Object.FirstSplit == id && m.Object.LastSplit <= last)
                .OrderByDescending(m => m.Object.Level).Select(m => m.Info).FirstOrDefault();
            var key = lower?.Key ?? (splits.TryGetValue(id, out var split) ? split.Key : null);
            if (key == null) return null;
            var blob = await storage.ReadAsync(key, cancellation).ConfigureAwait(false);
            if (blob == null) return null;
            var parsed = EventLogFormat.Parse(blob.Content.Span, topic);
            if (!parsed.IsSealed || parsed.SplitId != id || sources.Count > 0 && parsed.FirstOffset != sources[^1].Object.NextOffset)
                throw new EventLogCorruptedException($"Split {id} of topic '{topic}' does not continue the history.");
            sources.Add((blob.Content, parsed));
            length += blob.Content.Length;
            if (length > options.MaxMergedObjectLength) return null;
            id = parsed.LastSplitId + 1;
        }
        var content = EventLogFormat.CreateMerged(topic, level, first, last, sources);
        var firstOffset = sources[0].Object.FirstOffset;
        var mergedKey = EventLogFormat.MergedKey(topic, level, firstOffset);
        var sha = Convert.ToHexString(SHA256.HashData(content));
        while (true)
        {
            var result = await storage.WriteAsync(mergedKey, null, content, null, sha, cancellation).ConfigureAwait(false);
            if (result.Outcome != EventLogWriteOutcome.Ambiguous) break;
            await scheduler.DelayAsync(options.AmbiguousRetryDelay, "event log merge retry", cancellation)
                .ConfigureAwait(false);
        }
        var stored = await storage.ReadAsync(mergedKey, cancellation).ConfigureAwait(false);
        if (stored == null) return null;
        if (!stored.Content.Span.SequenceEqual(content))
            throw new EventLogCorruptedException($"Merged object '{mergedKey}' differs from its deterministic content.");
        _verified.Add((mergedKey, stored.Version));
        var info = new EventLogObjectInfo(mergedKey, stored.Version, content.Length, null);
        var entry = new Merged(mergedKey, stored.Version, (byte)level, first, last, firstOffset);
        _known[mergedKey] = entry;
        return (entry, info);
    }
}
