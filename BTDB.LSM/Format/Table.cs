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
/// tombstone fragments, properties and a fixed footer. Index and filter partitioning comes later in phase L2.
sealed class TableBuilder(TableBuildOptions options)
{
    public const ulong Magic = 0x314D534C42445442; // "BTDBLSM1"
    public const int FormatVersion = 2;
    public const int FooterSize = 8 + 4 + 4 + 8 * 8 + 8;

    readonly LsmEncoder _output = new(64 * 1024);
    readonly LsmEncoder _index = new();
    readonly List<ulong> _hashes = [];
    DataBlockBuilder _block = new(options.RestartInterval);
    long _deltaSum;
    int _blockCount;
    byte[]? _smallest;
    byte[]? _largest;
    readonly SortedDictionary<ulong, long> _valueReferences = [];

    public long EntryCount { get; private set; }
    public long EstimatedSize => _output.Length + _block.EstimatedSize + _index.Length + _hashes.Count * 2L;

    public static byte[] Build(IEnumerable<LsmEntry> entries, TombstoneSet tombstones, TableBuildOptions options)
    {
        var builder = new TableBuilder(options);
        foreach (var entry in entries) builder.Add(entry);
        return builder.Finish(tombstones);
    }

    void FlushBlock()
    {
        if (_block.EntryCount == 0) return;
        var offset = _output.Length;
        _index.WriteByteArray(_block.FirstKey!);
        _index.WriteVUInt64((ulong)offset);
        _index.WriteVUInt64((ulong)_block.EntryCount);
        _index.WriteVInt64(_deltaSum);
        _index.WriteVInt64(_block.MinPrefix);
        _index.WriteVInt64(_block.MaxPrefix);
        _block.Finish(_output, options.Compress);
        _index.WriteVUInt64((ulong)(_output.Length - offset));
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

        _hashes.Add(BloomFilter.Hash(entry.Key));
        EntryCount++;
        if (_block.EstimatedSize >= options.BlockSize) FlushBlock();
    }

    public byte[] Finish(TombstoneSet tombstones)
    {
        FlushBlock();
        var output = _output;
        var indexOffset = output.Length;
        output.WriteVUInt64((ulong)_blockCount);
        output.WriteBytes(_index.WrittenSpan);
        output.WriteChecksum(indexOffset);
        var bloomOffset = output.Length;
        BloomFilter.Build(output, _hashes.ToArray(), options.BloomBitsPerKey);
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

    /// Stored bytes this table references in each value file.
    public IReadOnlyDictionary<ulong, long> ValueReferences { get; private init; } = new Dictionary<ulong, long>();
    public bool HasPoints => _firstKeys.Length > 0;

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
        var valueReferences = new Dictionary<ulong, long>();
        for (var count = properties.ReadVUInt32(); count > 0; count--)
            valueReferences.Add(properties.ReadVUInt64(), (long)properties.ReadVUInt64());
        return new(storage, cache, fileNumber, objectKey, length, firstKeys, offsets, lengths, entryCounts,
            deltaBefore, minPrefix, maxPrefix, bloom, new(tombstones), entryCount, deltaSum, smallest, largest)
        {
            ValueReferences = valueReferences
        };
    }

    public IComponentCursor CreateCursor() => new TableCursor(this);

    internal DataBlock LoadBlock(int block) => _cache.GetOrLoad(FileNumber, _offsets[block], () =>
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

/// Positioned cursor over one table: a block index and an entry index inside the decoded block.
sealed class TableCursor(TableReader table) : IComponentCursor
{
    int _block;
    int _index;
    DataBlock? _data;

    public bool Valid { get; private set; }
    public LsmEntry Current { get; private set; }

    bool At(int block, int index)
    {
        if (block != _block || _data == null)
        {
            _data = table.LoadBlock(block);
            _block = block;
        }

        _index = index;
        Current = _data.Entry(index);
        return Valid = true;
    }

    bool Invalid()
    {
        Valid = false;
        return false;
    }

    public bool SeekGE(ReadOnlySpan<byte> key, bool inclusive)
    {
        for (var block = Math.Max(0, table.CountBlocksBefore(key, true) - 1); block < table.BlockCount; block++)
        {
            var data = table.LoadBlock(block);
            var index = data.LowerBound(key, inclusive);
            if (index < data.Count) return At(block, index);
        }

        return Invalid();
    }

    public bool SeekLE(ReadOnlySpan<byte> key, bool inclusive)
    {
        var block = table.CountBlocksBefore(key, inclusive) - 1;
        if (block < 0) return Invalid();
        // The block's first key qualifies, so the result is inside this block.
        return At(block, table.LoadBlock(block).LowerBound(key, !inclusive) - 1);
    }

    public bool SeekLast()
    {
        if (table.BlockCount == 0) return Invalid();
        var block = table.BlockCount - 1;
        return At(block, table.LoadBlock(block).Count - 1);
    }

    public bool Next()
    {
        if (!Valid) return false;
        if (_index + 1 < _data!.Count) return At(_block, _index + 1);
        return _block + 1 < table.BlockCount ? At(_block + 1, 0) : Invalid();
    }

    public bool Prev()
    {
        if (!Valid) return false;
        if (_index > 0) return At(_block, _index - 1);
        return _block > 0 ? At(_block - 1, table.LoadBlock(_block - 1).Count - 1) : Invalid();
    }
}
