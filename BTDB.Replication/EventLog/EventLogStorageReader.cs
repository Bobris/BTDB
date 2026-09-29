using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication.EventLog;

/// <summary>The latest existing split. When it is sealed, the writable tail is its successor, which does not exist yet.</summary>
internal sealed record EventLogTail(ulong SplitId, string Version, byte[] Content, EventLogObject Object, EventLogOwner? Owner)
{
    public bool Sealed => Object.Seal != null;
    public ulong NextOffset => Object.NextOffset;
}

internal sealed record EventLogLocated(string Key, string Version, ReadOnlyMemory<byte> Content, EventLogObject Object);

/// <summary>Reads committed history from storage: tail discovery and frames from an offset. Sealed and merged objects
/// are immutable, so a located object is cached by key and version.</summary>
internal sealed class EventLogStorageReader(IEventLogStorage storage, string topic, int mergeLevels = 2)
{
    sealed class Listing
    {
        public readonly SortedList<ulong, EventLogObjectInfo> Splits = new();
        public readonly SortedList<ulong, EventLogObjectInfo>[] Merged;
        public Listing(int levels)
        {
            Merged = new SortedList<ulong, EventLogObjectInfo>[levels + 1];
            for (var i = 0; i < Merged.Length; i++) Merged[i] = new();
        }
    }

    readonly object _lock = new();
    Listing? _listing;
    readonly Dictionary<(string Key, string Version), EventLogLocated> _cache = new();
    readonly Queue<(string, string)> _cacheOrder = new();
    const int CacheEntries = 64;

    public string Topic => topic;

    async ValueTask<Listing> ListAsync(bool refresh, CancellationToken cancellation)
    {
        lock (_lock)
            if (!refresh && _listing != null) return _listing;
        var listing = new Listing(mergeLevels);
        await foreach (var info in storage.ListAsync(topic + "/", cancellation).ConfigureAwait(false))
        {
            if (!EventLogFormat.TryParseKey(topic, info.Key, out var level, out var number)) continue;
            if (level == 0) listing.Splits[number] = info;
            else if (level < listing.Merged.Length) listing.Merged[level][number] = info;
        }
        lock (_lock) _listing = listing;
        return listing;
    }

    /// <summary>Find the latest split by listing and then following seals to successors that already exist.</summary>
    public async ValueTask<EventLogTail?> FindTailAsync(CancellationToken cancellation)
    {
        for (var attempt = 0; ; attempt++)
        {
            var listing = await ListAsync(true, cancellation).ConfigureAwait(false);
            if (listing.Splits.Count == 0)
            {
                for (var level = 1; level < listing.Merged.Length; level++)
                    if (listing.Merged[level].Count != 0)
                        throw new EventLogCorruptedException($"Topic '{topic}' has merged objects but no splits.");
                return null;
            }
            var id = listing.Splits.Keys[^1];
            EventLogTail? tail = null;
            while (true)
            {
                var blob = await storage.ReadAsync(EventLogFormat.SplitKey(topic, id), cancellation).ConfigureAwait(false);
                if (blob == null) break;
                var content = blob.Content.ToArray();
                var parsed = EventLogFormat.Parse(content, topic);
                if (parsed.SplitId != id) throw new EventLogCorruptedException($"Split {id} has a wrong header.");
                if (tail != null && parsed.FirstOffset != tail.NextOffset)
                    throw new EventLogCorruptedException($"Split {id} does not continue its predecessor.");
                tail = new(id, blob.Version, content, parsed, blob.Owner);
                if (!tail.Sealed) return tail;
                id++;
            }
            if (tail != null) return tail;
            if (attempt == 3) throw new EventLogCorruptedException($"The latest split of topic '{topic}' keeps disappearing.");
        }
    }

    async ValueTask<EventLogLocated?> LoadAsync(string key, EventLogObjectInfo? info, CancellationToken cancellation)
    {
        if (info != null)
            lock (_lock)
                if (_cache.TryGetValue((key, info.Version), out var cached)) return cached;
        var blob = await storage.ReadAsync(key, cancellation).ConfigureAwait(false);
        if (blob == null) return null;
        var located = new EventLogLocated(key, blob.Version, blob.Content, EventLogFormat.Parse(blob.Content.Span, topic));
        if (located.Object.IsSealed)
            lock (_lock)
                if (_cache.TryAdd((key, blob.Version), located))
                {
                    _cacheOrder.Enqueue((key, blob.Version));
                    if (_cacheOrder.Count > CacheEntries) _cache.Remove(_cacheOrder.Dequeue());
                }
        return located;
    }

    /// <summary>The part of a merged object from the frame containing <paramref name="offset"/>, found through its
    /// header index, or null with the object's last split ID when the offset lies beyond it.</summary>
    async ValueTask<(EventLogLocated? Located, ulong LastSplitId)> LoadMergedAsync(EventLogObjectInfo info, ulong offset,
        CancellationToken cancellation)
    {
        lock (_lock)
            if (_cache.TryGetValue((info.Key, info.Version), out var cached))
                return offset < cached.Object.NextOffset ? (cached, cached.Object.LastSplitId) : (null, cached.Object.LastSplitId);
        var prefix = new byte[EventLogFormat.MergedFixedLength(topic)];
        await storage.ReadRangeAsync(info.Key, info.Version, 0, prefix, cancellation).ConfigureAwait(false);
        var merged = EventLogFormat.ParseMergedFixed(prefix, topic);
        if (offset >= merged.FirstOffset + merged.RecordCount) return (null, merged.LastSplitId);
        if (offset == merged.FirstOffset)
            return (await LoadAsync(info.Key, info, cancellation).ConfigureAwait(false)
                    ?? throw new EventLogVersionChangedException(info.Key), merged.LastSplitId);
        var header = new byte[merged.HeaderLength];
        await storage.ReadRangeAsync(info.Key, info.Version, 0, header, cancellation).ConfigureAwait(false);
        var frameStart = EventLogFormat.MergedFrameStart(header, merged, offset);
        var range = new byte[info.Length - frameStart];
        await storage.ReadRangeAsync(info.Key, info.Version, frameStart, range, cancellation).ConfigureAwait(false);
        var firstFrameOffset = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(range.AsSpan(5));
        var parsed = EventLogFormat.ParseMergedRange(range, merged, firstFrameOffset);
        return (new(info.Key, info.Version, range, parsed), merged.LastSplitId);
    }

    static int GreatestAtMost(IList<ulong> keys, ulong value)
    {
        int low = 0, high = keys.Count - 1, result = -1;
        while (low <= high)
        {
            var middle = (low + high) / 2;
            if (keys[middle] <= value)
            {
                result = middle;
                low = middle + 1;
            }
            else high = middle - 1;
        }
        return result;
    }

    async ValueTask<ulong> SplitFirstOffsetAsync(EventLogObjectInfo info, CancellationToken cancellation)
    {
        var header = new byte[EventLogFormat.SplitHeaderLength(topic)];
        await storage.ReadRangeAsync(info.Key, info.Version, 0, header, cancellation).ConfigureAwait(false);
        return EventLogFormat.ParseSplitHeader(header, topic).FirstOffset;
    }

    /// <summary>The object containing <paramref name="offset"/>, preferring the highest merged level, or null when the
    /// offset is at or beyond the end of the existing history.</summary>
    public async ValueTask<EventLogLocated?> LocateAsync(ulong offset, CancellationToken cancellation)
    {
        for (var attempt = 0; attempt < 4; attempt++)
        {
            try
            {
                var result = await TryLocateAsync(await ListAsync(attempt > 0, cancellation).ConfigureAwait(false),
                    offset, cancellation).ConfigureAwait(false);
                if (result.Found != null || result.AtEnd) return result.Found;
            }
            catch (EventLogVersionChangedException) { }
        }
        throw new EventLogOffsetNotAvailableException($"Offset {offset} of topic '{topic}' is not available.");
    }

    async ValueTask<(EventLogLocated? Found, bool AtEnd)> TryLocateAsync(Listing listing, ulong offset,
        CancellationToken cancellation)
    {
        ulong afterSplit = 0;
        for (var level = listing.Merged.Length - 1; level >= 1; level--)
        {
            var merged = listing.Merged[level];
            var index = GreatestAtMost(merged.Keys, offset);
            if (index < 0) continue;
            var info = merged.Values[index];
            var (located, lastSplitId) = await LoadMergedAsync(info, offset, cancellation).ConfigureAwait(false);
            if (located != null) return (located, false);
            afterSplit = Math.Max(afterSplit, lastSplitId);
        }
        var splits = listing.Splits;
        int low = 0, high = splits.Count - 1, candidate = -1;
        while (low <= high && splits.Keys[low] <= afterSplit) low++;
        var first = low;
        while (low <= high)
        {
            var middle = (low + high) / 2;
            if (await SplitFirstOffsetAsync(splits.Values[middle], cancellation).ConfigureAwait(false) <= offset)
            {
                candidate = middle;
                low = middle + 1;
            }
            else high = middle - 1;
        }
        if (candidate < 0) return (null, first == splits.Count);
        for (var id = splits.Keys[candidate]; ; id++)
        {
            var located = await LoadAsync(EventLogFormat.SplitKey(topic, id), splits.GetValueOrDefault(id), cancellation)
                .ConfigureAwait(false);
            if (located == null) return (null, id > splits.Keys[^1]);
            if (offset < located.Object.NextOffset) return (located, false);
            if (located.Object.Seal == null) return (null, offset == located.Object.NextOffset);
        }
    }

    /// <summary>Committed frames that contain offsets from <paramref name="from"/> up to the end of the existing
    /// history or to <paramref name="to"/>. The first frame may start before <paramref name="from"/>.</summary>
    public async IAsyncEnumerable<(ReadOnlyMemory<byte> Content, EventLogFrame Frame)> ReadFramesAsync(ulong from,
        ulong? to, [EnumeratorCancellation] CancellationToken cancellation)
    {
        var offset = from;
        while (to == null || offset < to)
        {
            var located = await LocateAsync(offset, cancellation).ConfigureAwait(false);
            if (located == null) yield break;
            var advanced = false;
            foreach (var frame in located.Object.Frames)
            {
                if (frame.NextOffset <= offset) continue;
                if (to != null && frame.FirstOffset >= to) yield break;
                yield return (located.Content, frame);
                offset = frame.NextOffset;
                advanced = true;
            }
            if (!advanced || located.Object is { Level: 0, Seal: null }) yield break;
        }
    }

    public async IAsyncEnumerable<EventLogRecord> ReadRecordsAsync(ulong from, ulong? to,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        await foreach (var (content, frame) in ReadFramesAsync(from, to, cancellation).ConfigureAwait(false))
        foreach (var record in EventLogFormat.Records(content, frame))
        {
            if (record.Offset < from) continue;
            if (to != null && record.Offset >= to) yield break;
            yield return record;
        }
    }
}
