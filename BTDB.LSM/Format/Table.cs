using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
using BTDB.LSM.Storage;

namespace BTDB.LSM.Format;

sealed class TableBuildOptions
{
    public int BlockSize { get; init; } = 32 * 1024;
    public int RestartInterval { get; init; } = 16;
    public int BloomBitsPerKey { get; init; } = 10;
    public bool Compress { get; init; } = true;
}

/// Writes a table (ImplementationPlan.md, section 9). L1 layout: data blocks, one index section, a bloom filter, the
/// tombstone fragments, properties and a fixed footer. Index and filter partitioning comes in phase L2.
static class TableBuilder
{
    public const ulong Magic = 0x314D534C42445442; // "BTDBLSM1"
    public const int FormatVersion = 1;
    public const int FooterSize = 8 + 4 + 4 + 8 * 8 + 8;

    public static byte[] Build(IEnumerable<LsmEntry> entries, TombstoneSet tombstones, TableBuildOptions options)
    {
        var output = new LsmEncoder(64 * 1024);
        var index = new LsmEncoder();
        var hashes = new List<ulong>();
        var block = new DataBlockBuilder(options.RestartInterval);
        long entryCount = 0;
        long deltaSum = 0;
        var blockCount = 0;
        byte[]? smallest = null;
        byte[]? largest = null;

        void FlushBlock()
        {
            if (block.EntryCount == 0) return;
            var offset = output.Length;
            index.WriteByteArray(block.FirstKey!);
            index.WriteVUInt64((ulong)offset);
            index.WriteVUInt64((ulong)block.EntryCount);
            index.WriteVInt64(deltaSum);
            index.WriteVInt64(block.MinPrefix);
            index.WriteVInt64(block.MaxPrefix);
            block.Finish(output, options.Compress);
            index.WriteVUInt64((ulong)(output.Length - offset));
            deltaSum += block.DeltaSum;
            blockCount++;
            block = new(options.RestartInterval);
        }

        foreach (var entry in entries)
        {
            if (largest != null && LsmKeys.Compare(largest, entry.Key) >= 0)
                throw new InvalidOperationException("Table entries must be strictly ascending.");
            smallest ??= entry.Key;
            largest = entry.Key;
            block.Add(entry);
            hashes.Add(BloomFilter.Hash(entry.Key));
            entryCount++;
            if (block.EstimatedSize >= options.BlockSize) FlushBlock();
        }

        FlushBlock();

        var indexOffset = output.Length;
        var indexStart = output.Length;
        output.WriteVUInt64((ulong)blockCount);
        output.WriteBytes(index.WrittenSpan);
        output.WriteChecksum(indexStart);
        var bloomOffset = output.Length;
        BloomFilter.Build(output, hashes.ToArray(), options.BloomBitsPerKey);
        output.WriteChecksum(bloomOffset);
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
        output.WriteVUInt64((ulong)entryCount);
        output.WriteVInt64(deltaSum);
        output.WriteByteArray(smallest ?? []);
        output.WriteByteArray(largest ?? []);
        output.WriteChecksum(propertiesOffset);
        var footerStart = output.Length;
        output.WriteUInt64(Magic);
        output.WriteInt32(FormatVersion);
        output.WriteInt32(0);
        foreach (var (start, end) in new[]
                 {
                     (indexOffset, bloomOffset), (bloomOffset, tombstoneOffset),
                     (tombstoneOffset, propertiesOffset), (propertiesOffset, footerStart)
                 })
        {
            output.WriteInt64(start);
            output.WriteInt64(end - start);
        }

        output.WriteChecksum(footerStart);
        return output.WrittenSpan.ToArray();
    }
}

/// Reads a table through ranged storage reads and the block cache. Index, bloom filter, tombstones and properties are
/// pinned for the reader's lifetime.
sealed class TableReader : ILsmComponent
{
    readonly ILsmStorage _storage;
    readonly LsmBlockCache _cache;
    readonly byte[][] _firstKeys;
    readonly long[] _offsets;
    readonly int[] _lengths;
    readonly int[] _entryCounts;
    readonly long[] _deltaBefore;
    readonly int[] _minPrefix;
    readonly int[] _maxPrefix;
    readonly BloomFilter _bloom;

    TableReader(ILsmStorage storage, LsmBlockCache cache, ulong fileNumber, string objectKey, long length,
        byte[][] firstKeys, long[] offsets, int[] lengths, int[] entryCounts, long[] deltaBefore, int[] minPrefix,
        int[] maxPrefix, BloomFilter bloom, TombstoneSet tombstones, long entryCount, long deltaSum,
        byte[] smallest, byte[] largest)
    {
        _storage = storage;
        _cache = cache;
        FileNumber = fileNumber;
        ObjectKey = objectKey;
        Length = length;
        _firstKeys = firstKeys;
        _offsets = offsets;
        _lengths = lengths;
        _entryCounts = entryCounts;
        _deltaBefore = deltaBefore;
        _minPrefix = minPrefix;
        _maxPrefix = maxPrefix;
        _bloom = bloom;
        Tombstones = tombstones;
        EntryCount = entryCount;
        DeltaSum = deltaSum;
        Smallest = smallest;
        Largest = largest;
    }

    public ulong FileNumber { get; }
    public string ObjectKey { get; }
    public long Length { get; }
    public long EntryCount { get; }
    public long DeltaSum { get; }
    public TombstoneSet Tombstones { get; }
    public byte[] Smallest { get; }
    public byte[] Largest { get; }
    public int BlockCount => _firstKeys.Length;

    public static TableReader Open(ILsmStorage storage, LsmBlockCache cache, ulong fileNumber, string objectKey,
        long length, int tailReadSize = 64 * 1024)
    {
        if (length < TableBuilder.FooterSize) throw new BTDBException($"Table {objectKey} is truncated.");
        var tailLength = (int)Math.Min(length, Math.Max(tailReadSize, TableBuilder.FooterSize));
        var tail = new byte[tailLength];
        storage.ReadRange(objectKey, length - tailLength, tail);
        var footer = LsmChecksum.Verify(tail.AsSpan(tailLength - TableBuilder.FooterSize), objectKey);
        if (BinaryPrimitives.ReadUInt64LittleEndian(footer) != TableBuilder.Magic)
            throw new BTDBException($"Table {objectKey} has an unknown format.");
        if (BinaryPrimitives.ReadInt32LittleEndian(footer[8..]) != TableBuilder.FormatVersion)
            throw new BTDBException($"Table {objectKey} has an unsupported format version.");
        var sections = new (long Offset, long Length)[4];
        for (var i = 0; i < 4; i++)
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
            storage.ReadRange(objectKey, metaStart, meta);
            tailStart = metaStart;
        }

        ReadOnlySpan<byte> Section(int i) => LsmChecksum.Verify(
            meta.AsSpan((int)(sections[i].Offset - tailStart), (int)sections[i].Length), objectKey);

        var indexDecoder = new LsmDecoder(Section(0));
        var blockCount = indexDecoder.ReadVUInt32();
        var firstKeys = new byte[blockCount][];
        var offsets = new long[blockCount];
        var lengths = new int[blockCount];
        var entryCounts = new int[blockCount];
        var deltaBefore = new long[blockCount];
        var minPrefix = new int[blockCount];
        var maxPrefix = new int[blockCount];
        for (var i = 0; i < blockCount; i++)
        {
            firstKeys[i] = indexDecoder.ReadByteArray();
            offsets[i] = (long)indexDecoder.ReadVUInt64();
            entryCounts[i] = indexDecoder.ReadVUInt32();
            deltaBefore[i] = indexDecoder.ReadVInt64();
            minPrefix[i] = indexDecoder.ReadVInt32();
            maxPrefix[i] = indexDecoder.ReadVInt32();
            lengths[i] = indexDecoder.ReadVUInt32();
        }

        var bloom = BloomFilter.Read(Section(1));
        var tombstoneDecoder = new LsmDecoder(Section(2));
        var tombstones = new LsmTombstone[tombstoneDecoder.ReadVUInt32()];
        for (var i = 0; i < tombstones.Length; i++)
            tombstones[i] = new(tombstoneDecoder.ReadByteArray(), tombstoneDecoder.ReadByteArray(),
                tombstoneDecoder.ReadVInt64());
        var properties = new LsmDecoder(Section(3));
        var entryCount = (long)properties.ReadVUInt64();
        var deltaSum = properties.ReadVInt64();
        var smallest = properties.ReadByteArray();
        var largest = properties.ReadByteArray();
        return new(storage, cache, fileNumber, objectKey, length, firstKeys, offsets, lengths, entryCounts,
            deltaBefore, minPrefix, maxPrefix, bloom, new(tombstones), entryCount, deltaSum, smallest, largest);
    }

    DataBlock LoadBlock(int block) => _cache.GetOrLoad(FileNumber, _offsets[block], () =>
    {
        var stored = new byte[_lengths[block]];
        _storage.ReadRange(ObjectKey, _offsets[block], stored);
        return DataBlock.Decode(stored, ObjectKey);
    });

    public byte[] BlockFirstKey(int block) => _firstKeys[block];

    public int CountBlocksBefore(ReadOnlySpan<byte> key, bool inclusive)
    {
        int lo = 0, hi = _firstKeys.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            var cmp = LsmKeys.Compare(_firstKeys[mid], key);
            if (cmp < 0 || inclusive && cmp == 0) lo = mid + 1;
            else hi = mid;
        }

        return lo;
    }

    public long PointDeltaBefore(ReadOnlySpan<byte> key)
    {
        var block = CountBlocksBefore(key, false) - 1;
        if (block < 0) return 0;
        var data = LoadBlock(block);
        return _deltaBefore[block] + data.DeltaPrefix(data.LowerBound(key, true));
    }

    public (long Min, long Max) PointDeltaBounds(ReadOnlySpan<byte> key)
    {
        var block = CountBlocksBefore(key, true) - 1;
        if (block < 0) return (0, 0);
        var before = _deltaBefore[block];
        if (LsmKeys.Compare(_firstKeys[block], key) == 0) return (before, before);
        return (before + _minPrefix[block], before + _maxPrefix[block]);
    }

    public bool TryGet(ReadOnlySpan<byte> key, out LsmEntry entry)
    {
        entry = default;
        if (_firstKeys.Length == 0 || LsmKeys.Compare(key, Smallest) < 0 || LsmKeys.Compare(key, Largest) > 0)
            return false;
        if (!_bloom.MayContain(key)) return false;
        var block = CountBlocksBefore(key, true) - 1;
        if (block < 0) return false;
        var data = LoadBlock(block);
        var index = data.LowerBound(key, true);
        if (index >= data.Count || LsmKeys.Compare(data.Key(index), key) != 0) return false;
        entry = data.Entry(index);
        return true;
    }

    public bool SeekGE(ReadOnlySpan<byte> key, bool inclusive, out LsmEntry entry)
    {
        var block = Math.Max(0, CountBlocksBefore(key, true) - 1);
        for (; block < _firstKeys.Length; block++)
        {
            var data = LoadBlock(block);
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
            var data = LoadBlock(block);
            // The block's first key qualifies, so the result is inside this block.
            var index = data.LowerBound(key, !inclusive) - 1;
            entry = data.Entry(index);
            return true;
        }

        entry = default;
        return false;
    }

    public bool SeekLast(out LsmEntry entry)
    {
        if (_firstKeys.Length == 0)
        {
            entry = default;
            return false;
        }

        var data = LoadBlock(_firstKeys.Length - 1);
        entry = data.Entry(data.Count - 1);
        return true;
    }

    public IEnumerable<LsmEntry> Enumerate()
    {
        for (var block = 0; block < _firstKeys.Length; block++)
        {
            var data = LoadBlock(block);
            for (var i = 0; i < data.Count; i++) yield return data.Entry(i);
        }
    }

    public void EvictFromCache() => _cache.Evict(FileNumber);
}
