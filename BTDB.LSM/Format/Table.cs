using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
using BTDB.LSM.Storage;

namespace BTDB.LSM.Format;

/// Reads of immutable objects that report missing and truncated objects as BTDBException (invariant L7).
static class LsmObjects
{
    public static void ReadRange(ILsmStorage storage, string key, long offset, Span<byte> destination, string kind)
    {
        try
        {
            storage.ReadRange(key, offset, destination);
        }
        catch (System.IO.FileNotFoundException ex)
        {
            throw new BTDBException(
                $"{kind} {key} no longer exists: the snapshot is older than the deletion grace period.", ex);
        }
        catch (System.IO.EndOfStreamException ex)
        {
            throw new BTDBException($"{kind} {key} is truncated.", ex);
        }
    }
}

sealed class TableBuildOptions
{
    public int BlockSize { get; init; } = 32 * 1024;
    public int RestartInterval { get; init; } = 16;
    public int BloomBitsPerKey { get; init; } = 16;
    public bool Compress { get; init; } = true;

    /// Target encoded size of one index partition; each partition has its own bloom filter partition.
    public int IndexPartitionSize { get; init; } = 16 * 1024;
}

/// Writes a table (ImplementationPlan.md, section 9): data blocks, index partitions, filter partitions, the top-level
/// index, the tombstone fragments, properties and a fixed footer. Only the top-level index, tombstones and properties
/// are pinned by readers; partitions are read through the cache.
sealed class TableBuilder(TableBuildOptions options)
{
    public const ulong Magic = 0x314D534C42445442; // "BTDBLSM1"
    public const int FormatVersion = 4;
    public const int FooterSize = 8 + 4 + 4 + 3 * 16 + 8;

    sealed class Partition(int firstBlock)
    {
        public readonly int FirstBlock = firstBlock;
        public readonly LsmEncoder Index = new();
        public readonly List<ulong> Hashes = [];
        public int BlockCount;
        public byte[]? FirstKey;
    }

    readonly LsmEncoder _output = new(64 * 1024);
    readonly List<Partition> _partitions = [];
    readonly List<ulong> _blockHashes = [];
    DataBlockBuilder _block = new(options.RestartInterval);
    long _deltaSum;
    int _blockCount;
    long _indexBytes;
    byte[]? _smallest;
    byte[]? _largest;
    readonly SortedDictionary<ulong, long> _valueReferences = [];

    public long EntryCount { get; private set; }

    public long EstimatedSize => _output.Length + _block.EstimatedSize + _indexBytes + EntryCount * 2;

    public static byte[] Build(IEnumerable<LsmEntry> entries, TombstoneSet tombstones, TableBuildOptions options)
    {
        var builder = new TableBuilder(options);
        foreach (var entry in entries) builder.Add(entry);
        return builder.Finish(tombstones);
    }

    void FlushBlock()
    {
        if (_block.EntryCount == 0) return;
        if (_partitions.Count == 0 || _partitions[^1].Index.Length >= options.IndexPartitionSize)
            _partitions.Add(new(_blockCount));
        var partition = _partitions[^1];
        partition.FirstKey ??= _block.FirstKey;
        var offset = _output.Length;
        var index = partition.Index;
        var indexStart = index.Length;
        index.WriteByteArray(_block.FirstKey!);
        index.WriteVUInt64((ulong)offset);
        index.WriteVUInt64((ulong)_block.EntryCount);
        index.WriteVInt64(_deltaSum);
        index.WriteVInt64(_block.MinPrefix);
        index.WriteVInt64(_block.MaxPrefix);
        _block.Finish(_output, options.Compress);
        index.WriteVUInt64((ulong)(_output.Length - offset));
        _indexBytes += index.Length - indexStart;
        partition.Hashes.AddRange(_blockHashes);
        partition.BlockCount++;
        _blockHashes.Clear();
        _deltaSum += _block.DeltaSum;
        _blockCount++;
        _block = new(options.RestartInterval);
    }

    public void Add(in LsmEntry entry)
    {
        if (_largest != null && LsmKeys.Compare(_largest, entry.Key) >= 0)
            throw new InvalidOperationException("Table entries must be strictly ascending.");
        _smallest ??= entry.Key;
        _largest = entry.Key;
        _block.Add(entry);
        if (entry.IsSeparated)
        {
            var handle = ValueHandle.Decode(entry.Value.Span);
            _valueReferences[handle.FileNumber] = _valueReferences.GetValueOrDefault(handle.FileNumber) +
                                                  handle.StoredLength;
        }

        _blockHashes.Add(BloomFilter.Hash(entry.Key));
        EntryCount++;
        if (_block.EstimatedSize >= options.BlockSize) FlushBlock();
    }

    public byte[] Finish(TombstoneSet tombstones)
    {
        FlushBlock();
        var output = _output;
        var handles = new List<(long IndexOffset, int IndexLength, long FilterOffset, int FilterLength)>();
        foreach (var partition in _partitions)
        {
            var indexOffset = output.Length;
            output.WriteVUInt64((ulong)partition.BlockCount);
            output.WriteBytes(partition.Index.WrittenSpan);
            output.WriteChecksum(indexOffset);
            var filterOffset = output.Length;
            BloomFilter.Build(output, partition.Hashes.ToArray(), options.BloomBitsPerKey);
            output.WriteChecksum(filterOffset);
            handles.Add((indexOffset, filterOffset - indexOffset, filterOffset, output.Length - filterOffset));
        }

        var topOffset = output.Length;
        output.WriteVUInt64((ulong)_blockCount);
        output.WriteVUInt64((ulong)_partitions.Count);
        for (var i = 0; i < _partitions.Count; i++)
        {
            output.WriteByteArray(_partitions[i].FirstKey!);
            output.WriteVUInt64((ulong)_partitions[i].FirstBlock);
            output.WriteVUInt64((ulong)handles[i].IndexOffset);
            output.WriteVUInt64((ulong)handles[i].IndexLength);
            output.WriteVUInt64((ulong)handles[i].FilterOffset);
            output.WriteVUInt64((ulong)handles[i].FilterLength);
        }

        output.WriteChecksum(topOffset);
        var tombstoneOffset = output.Length;
        output.WriteVUInt64((ulong)tombstones.Count);
        foreach (var tombstone in tombstones.Items)
        {
            output.WriteByteArray(tombstone.Start);
            output.WriteByteArray(tombstone.End);
            output.WriteVInt64(tombstone.CoveredBelow);
        }

        output.WriteChecksum(tombstoneOffset);
        var propertiesOffset = output.Length;
        output.WriteVUInt64((ulong)EntryCount);
        output.WriteVInt64(_deltaSum);
        output.WriteByteArray(_smallest ?? []);
        output.WriteByteArray(_largest ?? []);
        output.WriteVUInt64((ulong)_valueReferences.Count);
        foreach (var (fileNumber, bytes) in _valueReferences)
        {
            output.WriteVUInt64(fileNumber);
            output.WriteVUInt64((ulong)bytes);
        }

        output.WriteChecksum(propertiesOffset);
        var footerStart = output.Length;
        output.WriteUInt64(Magic);
        output.WriteInt32(FormatVersion);
        output.WriteInt32(0);
        foreach (var (start, end) in new[]
                 {
                     (topOffset, tombstoneOffset), (tombstoneOffset, propertiesOffset), (propertiesOffset, footerStart)
                 })
        {
            output.WriteInt64(start);
            output.WriteInt64(end - start);
        }

        output.WriteChecksum(footerStart);
        return output.WrittenSpan.ToArray();
    }
}

/// One decoded index partition: per data block its first key, handle, entry count, cumulative delta sum before it
/// and the extremes of the running delta inside it.
sealed class IndexPartition : ICachedBlock
{
    public IndexPartition(ReadOnlySpan<byte> data)
    {
        var decoder = new LsmDecoder(data);
        var count = decoder.ReadVUInt32();
        FirstKeys = new byte[count][];
        Offsets = new long[count];
        Lengths = new int[count];
        DeltaBefore = new long[count];
        MinPrefix = new int[count];
        MaxPrefix = new int[count];
        long size = 64;
        for (var i = 0; i < count; i++)
        {
            FirstKeys[i] = decoder.ReadByteArray();
            Offsets[i] = (long)decoder.ReadVUInt64();
            decoder.ReadVUInt32();
            DeltaBefore[i] = decoder.ReadVInt64();
            MinPrefix[i] = decoder.ReadVInt32();
            MaxPrefix[i] = decoder.ReadVInt32();
            Lengths[i] = decoder.ReadVUInt32();
            size += FirstKeys[i].Length + 64;
        }

        MemorySize = size;
    }

    public byte[][] FirstKeys { get; }
    public long[] Offsets { get; }
    public int[] Lengths { get; }
    public long[] DeltaBefore { get; }
    public int[] MinPrefix { get; }
    public int[] MaxPrefix { get; }
    public long MemorySize { get; }

    /// Number of blocks of this partition whose first key is lower than (or equal to, when inclusive) key.
    public int CountBlocksBefore(ReadOnlySpan<byte> key, bool inclusive)
    {
        int lo = 0, hi = FirstKeys.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            var cmp = LsmKeys.Compare(FirstKeys[mid], key);
            if (cmp < 0 || inclusive && cmp == 0) lo = mid + 1;
            else hi = mid;
        }

        return lo;
    }
}

sealed class CachedFilter(BloomFilter filter) : ICachedBlock
{
    public BloomFilter Filter { get; } = filter;
    public long MemorySize => Filter.SizeInBytes + 64;
}

/// Reads a table through ranged storage reads and the cache. The top-level index, tombstones and properties are
/// pinned for the reader's lifetime; index and filter partitions and data blocks are cached.
sealed class TableReader : ILsmComponent
{
    readonly ILsmStorage _storage;
    readonly LsmBlockCache _cache;
    readonly byte[][] _partitionFirstKeys;
    readonly int[] _partitionFirstBlocks;
    readonly (long Offset, int Length)[] _indexHandles;
    readonly (long Offset, int Length)[] _filterHandles;

    TableReader(ILsmStorage storage, LsmBlockCache cache, ulong fileNumber, string objectKey, long length,
        int blockCount, byte[][] partitionFirstKeys, int[] partitionFirstBlocks, (long, int)[] indexHandles,
        (long, int)[] filterHandles, TombstoneSet tombstones, long entryCount, long deltaSum, byte[] smallest,
        byte[] largest, IReadOnlyDictionary<ulong, long> valueReferences)
    {
        _storage = storage;
        _cache = cache;
        FileNumber = fileNumber;
        ObjectKey = objectKey;
        Length = length;
        BlockCount = blockCount;
        _partitionFirstKeys = partitionFirstKeys;
        _partitionFirstBlocks = partitionFirstBlocks;
        _indexHandles = indexHandles;
        _filterHandles = filterHandles;
        Tombstones = tombstones;
        EntryCount = entryCount;
        DeltaSum = deltaSum;
        Smallest = smallest;
        Largest = largest;
        ValueReferences = valueReferences;
    }

    public ulong FileNumber { get; }
    public string ObjectKey { get; }
    public long Length { get; }
    public long EntryCount { get; }
    public long DeltaSum { get; }
    public TombstoneSet Tombstones { get; }
    public byte[] Smallest { get; }
    public byte[] Largest { get; }
    public int BlockCount { get; }
    public int PartitionCount => _partitionFirstKeys.Length;

    /// Stored bytes this table references in each value file.
    public IReadOnlyDictionary<ulong, long> ValueReferences { get; }

    public bool HasPoints => BlockCount > 0;

    byte[]? _lower;
    byte[]? _upper;

    /// Inclusive lower bound of everything the table contains: its first point or first tombstone start.
    public byte[] Lower => _lower ??= !HasPoints ? Tombstones[0].Start :
        Tombstones.Count == 0 || LsmKeys.Compare(Smallest, Tombstones[0].Start) <= 0 ? Smallest : Tombstones[0].Start;

    /// Exclusive upper bound of everything the table contains: after its last point or its last tombstone end.
    public byte[] Upper => _upper ??= ComputeUpper();

    byte[] ComputeUpper()
    {
        if (!HasPoints) return Tombstones[^1].End;
        var afterLargest = LsmKeys.Successor(Largest);
        return Tombstones.Count == 0 || LsmKeys.Compare(afterLargest, Tombstones[^1].End) >= 0
            ? afterLargest
            : Tombstones[^1].End;
    }

    public static TableReader Open(ILsmStorage storage, LsmBlockCache cache, ulong fileNumber, string objectKey,
        long length, int tailReadSize = 64 * 1024)
    {
        if (length < TableBuilder.FooterSize) throw new BTDBException($"Table {objectKey} is truncated.");
        var tailLength = (int)Math.Min(length, Math.Max(tailReadSize, TableBuilder.FooterSize));
        var tail = new byte[tailLength];
        LsmObjects.ReadRange(storage, objectKey, length - tailLength, tail, "Table");
        var footer = LsmChecksum.Verify(tail.AsSpan(tailLength - TableBuilder.FooterSize), objectKey);
        if (BinaryPrimitives.ReadUInt64LittleEndian(footer) != TableBuilder.Magic)
            throw new BTDBException($"Table {objectKey} has an unknown format.");
        if (BinaryPrimitives.ReadInt32LittleEndian(footer[8..]) != TableBuilder.FormatVersion)
            throw new BTDBException($"Table {objectKey} has an unsupported format version.");
        var sections = new (long Offset, long Length)[3];
        for (var i = 0; i < sections.Length; i++)
            sections[i] = (BinaryPrimitives.ReadInt64LittleEndian(footer[(16 + i * 16)..]),
                BinaryPrimitives.ReadInt64LittleEndian(footer[(24 + i * 16)..]));
        var metaStart = sections[0].Offset;
        var tailStart = length - tailLength;
        byte[] meta;
        if (metaStart >= tailStart)
        {
            meta = tail;
        }
        else
        {
            meta = new byte[length - metaStart];
            LsmObjects.ReadRange(storage, objectKey, metaStart, meta, "Table");
            tailStart = metaStart;
        }

        ReadOnlySpan<byte> Section(int i) => LsmChecksum.Verify(
            meta.AsSpan((int)(sections[i].Offset - tailStart), (int)sections[i].Length), objectKey);

        var top = new LsmDecoder(Section(0));
        var blockCount = top.ReadVUInt32();
        var partitionCount = top.ReadVUInt32();
        var firstKeys = new byte[partitionCount][];
        var firstBlocks = new int[partitionCount];
        var indexHandles = new (long, int)[partitionCount];
        var filterHandles = new (long, int)[partitionCount];
        for (var i = 0; i < partitionCount; i++)
        {
            firstKeys[i] = top.ReadByteArray();
            firstBlocks[i] = top.ReadVUInt32();
            indexHandles[i] = ((long)top.ReadVUInt64(), top.ReadVUInt32());
            filterHandles[i] = ((long)top.ReadVUInt64(), top.ReadVUInt32());
        }

        var tombstoneDecoder = new LsmDecoder(Section(1));
        var tombstones = new LsmTombstone[tombstoneDecoder.ReadVUInt32()];
        for (var i = 0; i < tombstones.Length; i++)
            tombstones[i] = new(tombstoneDecoder.ReadByteArray(), tombstoneDecoder.ReadByteArray(),
                tombstoneDecoder.ReadVInt64());
        var properties = new LsmDecoder(Section(2));
        var entryCount = (long)properties.ReadVUInt64();
        var deltaSum = properties.ReadVInt64();
        var smallest = properties.ReadByteArray();
        var largest = properties.ReadByteArray();
        var valueReferences = new Dictionary<ulong, long>();
        for (var count = properties.ReadVUInt32(); count > 0; count--)
            valueReferences.Add(properties.ReadVUInt64(), (long)properties.ReadVUInt64());
        return new(storage, cache, fileNumber, objectKey, length, blockCount, firstKeys, firstBlocks, indexHandles,
            filterHandles, new(tombstones), entryCount, deltaSum, smallest, largest, valueReferences);
    }

    public IComponentCursor CreateCursor() => new TableCursor(this);

    byte[] ReadChecked(long offset, int length)
    {
        var data = new byte[length];
        LsmObjects.ReadRange(_storage, ObjectKey, offset, data, "Table");
        return data;
    }

    IndexPartition Partition(int partition)
    {
        var (offset, length) = _indexHandles[partition];
        return _cache.GetOrLoadMetadata(FileNumber, offset,
            () => new IndexPartition(LsmChecksum.Verify(ReadChecked(offset, length), ObjectKey)));
    }

    BloomFilter Filter(int partition)
    {
        var (offset, length) = _filterHandles[partition];
        return _cache.GetOrLoadMetadata(FileNumber, offset,
            () => new CachedFilter(BloomFilter.Read(LsmChecksum.Verify(ReadChecked(offset, length), ObjectKey))))
            .Filter;
    }

    /// Index of the last partition whose first key is lower than (or equal to, when inclusive) key, or -1.
    int PartitionBefore(ReadOnlySpan<byte> key, bool inclusive)
    {
        int lo = 0, hi = _partitionFirstKeys.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            var cmp = LsmKeys.Compare(_partitionFirstKeys[mid], key);
            if (cmp < 0 || inclusive && cmp == 0) lo = mid + 1;
            else hi = mid;
        }

        return lo - 1;
    }

    /// Partition holding the global block number and the block's index inside it.
    (IndexPartition Partition, int Local) Locate(int block)
    {
        int lo = 0, hi = _partitionFirstBlocks.Length - 1;
        while (lo < hi)
        {
            var mid = (lo + hi + 1) >>> 1;
            if (_partitionFirstBlocks[mid] <= block) lo = mid;
            else hi = mid - 1;
        }

        return (Partition(lo), block - _partitionFirstBlocks[lo]);
    }

    /// Pins the raw bytes of a data block in the cache; wrap the handle's span in a RawBlock. A block being read
    /// ahead is awaited instead of read again.
    internal NativeClockCache.Handle PinBlock(int block)
    {
        var (partition, local) = Locate(block);
        var offset = partition.Offsets[local];
        var length = partition.Lengths[local];
        return _cache.Pin(FileNumber, offset, () =>
            _readahead.TryGetValue(offset, out var pending) && pending.GetAwaiter().GetResult() is { } prefetched
                ? prefetched
                : RawBlock.Decode(ReadChecked(offset, length), ObjectKey));
    }

    /// Largest single readahead request; L0 measured a 1 MiB GET at 2–3 times the latency of a 256 KiB one, while
    /// parallel requests keep their latency (ImplementationPlan.md, section 12.2).
    internal const int ReadaheadRequestSize = 256 * 1024;

    readonly ConcurrentDictionary<long, Task<byte[]?>> _readahead = new();
    volatile bool _evicted;

    /// Starts asynchronous reads of the blocks from first in direction (+1 or -1) up to about bytes of stored data,
    /// skipping blocks that are cached or already being read. Contiguous blocks are read together in requests of at
    /// most ReadaheadRequestSize, all in parallel; the decoded blocks go to the cache. Returns the last block covered.
    internal int Readahead(int first, int direction, long bytes)
    {
        var group = new List<(long Offset, int Length, TaskCompletionSource<byte[]?> Result)>();
        long groupBytes = 0;

        void Issue()
        {
            if (group.Count == 0) return;
            _ = ReadGroupAsync(group.ToArray());
            group.Clear();
            groupBytes = 0;
        }

        var block = first;
        for (long covered = 0; block >= 0 && block < BlockCount && covered < bytes; block += direction)
        {
            var (partition, local) = Locate(block);
            var offset = partition.Offsets[local];
            var length = partition.Lengths[local];
            covered += length;
            if (_cache.Contains(FileNumber, offset))
            {
                Issue();
                continue;
            }

            var result = new TaskCompletionSource<byte[]?>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_readahead.TryAdd(offset, result.Task))
            {
                Issue();
                continue;
            }

            // Groups are kept in ascending offset order; a backward scan adds blocks in front.
            if (group.Count > 0 && (groupBytes + length > ReadaheadRequestSize ||
                                    (direction > 0
                                        ? group[^1].Offset + group[^1].Length != offset
                                        : offset + length != group[0].Offset)))
                Issue();
            if (direction > 0) group.Add((offset, length, result));
            else group.Insert(0, (offset, length, result));
            groupBytes += length;
        }

        Issue();
        return Math.Clamp(block - direction, 0, Math.Max(0, BlockCount - 1));
    }

    async Task ReadGroupAsync((long Offset, int Length, TaskCompletionSource<byte[]?> Result)[] blocks)
    {
        var start = blocks[0].Offset;
        try
        {
            var buffer = new byte[blocks[^1].Offset + blocks[^1].Length - start];
            await _storage.ReadRangeAsync(ObjectKey, start, buffer, CancellationToken.None).ConfigureAwait(false);
            foreach (var (offset, length, result) in blocks)
            {
                var data = RawBlock.Decode(buffer.AsSpan((int)(offset - start), length), ObjectKey);
                if (!_evicted) _cache.Insert(FileNumber, offset, data);
                result.TrySetResult(data);
            }
        }
        catch
        {
            // A cursor that needs a failed block reads it itself and reports the error.
        }
        finally
        {
            foreach (var (offset, _, result) in blocks)
            {
                result.TrySetResult(null);
                _readahead.TryRemove(offset, out _);
            }
        }
    }

    public byte[] BlockFirstKey(int block)
    {
        var (partition, local) = Locate(block);
        return partition.FirstKeys[local];
    }

    public bool IsStored => true;

    public void Prefetch(ReadOnlySpan<byte> key)
    {
        if (!HasPoints) return;
        var block = Math.Max(0, CountBlocksBefore(key, true) - 1);
        Readahead(block, 1, 1);
    }

    public void AddFinePivots(int fromBlock, int toBlock, List<byte[]> pivots)
    {
        for (var block = Math.Max(0, fromBlock); block < Math.Min(toBlock, BlockCount); block++)
        {
            using var pin = PinBlock(block);
            var data = new RawBlock(pin.Span);
            for (var restart = 0; restart < data.RestartCount; restart++) pivots.Add(data.RestartKey(restart).ToArray());
        }
    }

    public int CountBlocksBefore(ReadOnlySpan<byte> key, bool inclusive)
    {
        var partition = PartitionBefore(key, inclusive);
        if (partition < 0) return 0;
        return _partitionFirstBlocks[partition] + Partition(partition).CountBlocksBefore(key, inclusive);
    }

    public long PointDeltaBefore(ReadOnlySpan<byte> key)
    {
        var block = CountBlocksBefore(key, false) - 1;
        if (block < 0) return 0;
        var (partition, local) = Locate(block);
        using var pin = PinBlock(block);
        var data = new RawBlock(pin.Span);
        return partition.DeltaBefore[local] + data.DeltaPrefix(data.LowerBound(key, true));
    }

    public (long Min, long Max) PointDeltaBounds(ReadOnlySpan<byte> key)
    {
        var block = CountBlocksBefore(key, true) - 1;
        if (block < 0) return (0, 0);
        var (partition, local) = Locate(block);
        var before = partition.DeltaBefore[local];
        if (LsmKeys.Compare(partition.FirstKeys[local], key) == 0) return (before, before);
        return (before + partition.MinPrefix[local], before + partition.MaxPrefix[local]);
    }

    public bool TryGet(ReadOnlySpan<byte> key, out LsmEntry entry)
    {
        entry = default;
        if (!HasPoints || LsmKeys.Compare(key, Smallest) < 0 || LsmKeys.Compare(key, Largest) > 0) return false;
        var partition = PartitionBefore(key, true);
        if (partition < 0 || !Filter(partition).MayContain(key)) return false;
        var block = _partitionFirstBlocks[partition] + Partition(partition).CountBlocksBefore(key, true) - 1;
        if (block < 0) return false;
        using var pin = PinBlock(block);
        var data = new RawBlock(pin.Span);
        var index = data.LowerBound(key, true);
        if (index >= data.Count) return false;
        var candidate = data.Entry(index);
        if (LsmKeys.Compare(candidate.Key, key) != 0) return false;
        entry = candidate;
        return true;
    }

    public bool SeekGE(ReadOnlySpan<byte> key, bool inclusive, out LsmEntry entry)
    {
        for (var block = Math.Max(0, CountBlocksBefore(key, true) - 1); block < BlockCount; block++)
        {
            using var pin = PinBlock(block);
            var data = new RawBlock(pin.Span);
            var index = data.LowerBound(key, inclusive);
            if (index < data.Count)
            {
                entry = data.Entry(index);
                return true;
            }
        }

        entry = default;
        return false;
    }

    public bool SeekLE(ReadOnlySpan<byte> key, bool inclusive, out LsmEntry entry)
    {
        var block = CountBlocksBefore(key, inclusive) - 1;
        if (block >= 0)
        {
            using var pin = PinBlock(block);
            var data = new RawBlock(pin.Span);
            // The block's first key qualifies, so the result is inside this block.
            entry = data.Entry(data.LowerBound(key, !inclusive) - 1);
            return true;
        }

        entry = default;
        return false;
    }

    public bool SeekLast(out LsmEntry entry)
    {
        if (!HasPoints)
        {
            entry = default;
            return false;
        }

        using var pin = PinBlock(BlockCount - 1);
        var data = new RawBlock(pin.Span);
        entry = data.Entry(data.Count - 1);
        return true;
    }

    /// Size of one ranged read when a table is read sequentially for flush or compaction input.
    public const int SequentialReadSize = 4 * 1024 * 1024;

    /// All points in order, read with large sequential ranged reads that bypass the data block cache, so compaction
    /// input does not evict the working set (ImplementationPlan.md, section 12.2).
    public IEnumerable<LsmEntry> Enumerate()
    {
        for (var partitionIndex = 0; partitionIndex < PartitionCount; partitionIndex++)
        {
            var partition = Partition(partitionIndex);
            var first = 0;
            while (first < partition.Offsets.Length)
            {
                var last = first;
                var start = partition.Offsets[first];
                while (last + 1 < partition.Offsets.Length &&
                       partition.Offsets[last + 1] + partition.Lengths[last + 1] - start <= SequentialReadSize)
                    last++;
                var chunk = ReadChecked(start, (int)(partition.Offsets[last] + partition.Lengths[last] - start));
                for (var block = first; block <= last; block++)
                {
                    var stored = chunk.AsSpan((int)(partition.Offsets[block] - start), partition.Lengths[block]);
                    foreach (var entry in DecodeAll(RawBlock.Decode(stored, ObjectKey))) yield return entry;
                }

                first = last + 1;
            }
        }
    }

    static List<LsmEntry> DecodeAll(byte[] raw)
    {
        var block = new RawBlock(raw);
        var entries = new List<LsmEntry>(block.Count);
        if (block.Count == 0) return entries;
        var entry = block.Entry(0, out var next);
        entries.Add(entry);
        for (var i = 1; i < block.Count; i++)
        {
            entry = block.EntryAt(next, entry.Key, out next);
            entries.Add(entry);
        }

        return entries;
    }

    public void EvictFromCache()
    {
        _evicted = true;
        _cache.Evict(FileNumber);
    }
}

/// Positioned cursor over one table: a block index, an entry index and the offset of the next entry, so stepping
/// forward decodes one entry. The block is pinned only during each call; a reloaded block has identical content, so
/// the remembered offset stays valid after eviction.
sealed class TableCursor(TableReader table) : IComponentCursor
{
    int _block;
    int _index;
    int _count;
    int _nextOffset;

    // Readahead state: consecutive block crossings in one direction, the last block requested and the window.
    const long InitialReadahead = 64 * 1024;
    const long MaxReadahead = 2 * 1024 * 1024;
    int _direction;
    int _crossings;
    int _aheadUntil;
    int _aheadBlocks;
    long _window = InitialReadahead;

    public bool Valid { get; private set; }
    public LsmEntry Current { get; private set; }

    bool At(int block, int index)
    {
        using var pin = table.PinBlock(block);
        var data = new RawBlock(pin.Span);
        Current = data.Entry(index, out _nextOffset);
        _count = data.Count;
        _block = block;
        _index = index;
        return Valid = true;
    }

    bool Invalid()
    {
        Valid = false;
        return false;
    }

    void ResetReadahead()
    {
        _crossings = 0;
        _window = InitialReadahead;
    }

    /// Called when the cursor steps onto the next block in direction. From the second consecutive crossing it keeps
    /// reads running ahead, requesting more when half of the previous request is consumed and doubling the window.
    void CrossedBlock(int direction)
    {
        if (direction != _direction)
        {
            _direction = direction;
            ResetReadahead();
        }

        if (++_crossings < 2) return;
        if (_crossings > 2 && (_aheadUntil - _block) * direction > _aheadBlocks / 2) return;
        var from = _crossings == 2 ? _block + direction : _aheadUntil + direction;
        if (from < 0 || from >= table.BlockCount) return;
        _aheadUntil = table.Readahead(from, direction, _window);
        _aheadBlocks = Math.Abs(_aheadUntil - from) + 1;
        _window = Math.Min(_window * 2, MaxReadahead);
    }

    public bool SeekGE(ReadOnlySpan<byte> key, bool inclusive)
    {
        ResetReadahead();
        for (var block = Math.Max(0, table.CountBlocksBefore(key, true) - 1); block < table.BlockCount; block++)
        {
            int index;
            using (var pin = table.PinBlock(block))
            {
                var data = new RawBlock(pin.Span);
                index = data.LowerBound(key, inclusive);
                if (index >= data.Count) continue;
            }

            return At(block, index);
        }

        return Invalid();
    }

    public bool SeekLE(ReadOnlySpan<byte> key, bool inclusive)
    {
        ResetReadahead();
        var block = table.CountBlocksBefore(key, inclusive) - 1;
        if (block < 0) return Invalid();
        int index;
        using (var pin = table.PinBlock(block))
        {
            // The block's first key qualifies, so the result is inside this block.
            index = new RawBlock(pin.Span).LowerBound(key, !inclusive) - 1;
        }

        return At(block, index);
    }

    public bool SeekLast()
    {
        ResetReadahead();
        if (table.BlockCount == 0) return Invalid();
        var block = table.BlockCount - 1;
        int count;
        using (var pin = table.PinBlock(block)) count = new RawBlock(pin.Span).Count;
        return At(block, count - 1);
    }

    public bool Next()
    {
        if (!Valid) return false;
        if (_index + 1 >= _count)
        {
            if (_block + 1 >= table.BlockCount) return Invalid();
            At(_block + 1, 0);
            CrossedBlock(1);
            return true;
        }

        using var pin = table.PinBlock(_block);
        Current = new RawBlock(pin.Span).EntryAt(_nextOffset, Current.Key, out _nextOffset);
        _index++;
        return true;
    }

    public bool Prev()
    {
        if (!Valid) return false;
        if (_index > 0) return At(_block, _index - 1);
        if (_block == 0) return Invalid();
        int count;
        using (var pin = table.PinBlock(_block - 1)) count = new RawBlock(pin.Span).Count;
        At(_block - 1, count - 1);
        CrossedBlock(-1);
        return true;
    }
}
