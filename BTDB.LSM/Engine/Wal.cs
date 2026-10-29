using System;
using System.Collections.Generic;
using BTDB.KVDBLayer;
using BTDB.LSM.Format;

namespace BTDB.LSM.Engine;

/// One part of a commit record inside a WAL chunk. A record larger than a chunk is split into parts; it applies only
/// once its last part is present.
readonly record struct WalPart(bool First, bool Last, long TransactionNumber, byte[] Payload);

/// Decoded WAL chunk (ImplementationPlan.md, section 8.1). An epoch marker names its writer's first own chunk.
sealed record WalChunk(long Epoch, long Generation, bool EpochMarker, long FirstOwnChunk, List<WalPart> Parts);

/// WAL chunk format: magic, format version, writer epoch, memtable generation, flags, the first own chunk of the
/// writer (meaningful in epoch markers), the record parts and a checksum of everything before it. Chunks are
/// immutable objects wal/{number}.
static class WalFormat
{
    const ulong Magic = 0x574D534C42445442; // "BTDBLSMW"
    const int FormatVersion = 2;

    public static byte[] Encode(long epoch, long generation, bool epochMarker, long firstOwnChunk,
        IReadOnlyList<WalPart> parts)
    {
        var encoder = new LsmEncoder(64 * 1024);
        encoder.WriteUInt64(Magic);
        encoder.WriteVUInt64(FormatVersion);
        encoder.WriteVUInt64((ulong)epoch);
        encoder.WriteVUInt64((ulong)generation);
        encoder.WriteByte((byte)(epochMarker ? 1 : 0));
        encoder.WriteVUInt64((ulong)firstOwnChunk);
        encoder.WriteVUInt64((ulong)parts.Count);
        foreach (var part in parts)
        {
            encoder.WriteByte((byte)((part.First ? 1 : 0) | (part.Last ? 2 : 0)));
            encoder.WriteVUInt64((ulong)part.TransactionNumber);
            encoder.WriteByteArray(part.Payload);
        }

        encoder.WriteChecksum(0);
        return encoder.WrittenSpan.ToArray();
    }

    public static WalChunk Decode(ReadOnlySpan<byte> data, string objectName)
    {
        var decoder = new LsmDecoder(LsmChecksum.Verify(data, objectName));
        if (decoder.ReadUInt64() != Magic) throw new BTDBException($"WAL chunk {objectName} has an unknown format.");
        if (decoder.ReadVUInt64() != FormatVersion)
            throw new BTDBException($"WAL chunk {objectName} has an unsupported format version.");
        var epoch = (long)decoder.ReadVUInt64();
        var generation = (long)decoder.ReadVUInt64();
        var epochMarker = decoder.ReadByte() != 0;
        var firstOwnChunk = (long)decoder.ReadVUInt64();
        var parts = new List<WalPart>();
        for (var count = decoder.ReadVUInt64(); count > 0; count--)
        {
            var flags = decoder.ReadByte();
            parts.Add(new((flags & 1) != 0, (flags & 2) != 0, (long)decoder.ReadVUInt64(), decoder.ReadByteArray()));
        }

        return new(epoch, generation, epochMarker, firstOwnChunk, parts);
    }
}

/// Commit metadata carried by every commit record.
readonly record struct WalCommitMetadata(long TransactionNumber, ulong CommitUlong, ulong[] Ulongs);

/// Builds a commit record from the memtable operations of a writing transaction, in the order they were applied.
/// Each operation carries its precomputed ExistsBelow or CoveredBelow, so replay needs no table lookups
/// (ImplementationPlan.md, section 8.1).
sealed class WalRecordBuilder
{
    const byte Insert = 1;
    const byte Update = 2;
    const byte Remove = 3;
    const byte RemoveRange = 4;
    const byte Tombstones = 5;

    readonly LsmEncoder _operations = new();

    public int Length => _operations.Length;

    public void LogInsert(ReadOnlySpan<byte> key, bool isSet, bool existsBelow, ReadOnlySpan<byte> value)
    {
        _operations.WriteByte(Insert);
        _operations.WriteByteArray(key);
        _operations.WriteByte((byte)((isSet ? 1 : 0) | (existsBelow ? 2 : 0)));
        _operations.WriteByteArray(value);
    }

    public void LogUpdate(ReadOnlySpan<byte> key, bool isSet, ReadOnlySpan<byte> value)
    {
        _operations.WriteByte(Update);
        _operations.WriteByteArray(key);
        _operations.WriteByte((byte)(isSet ? 1 : 0));
        _operations.WriteByteArray(value);
    }

    public void LogRemove(ReadOnlySpan<byte> key)
    {
        _operations.WriteByte(Remove);
        _operations.WriteByteArray(key);
    }

    public void LogRemoveRange(ReadOnlySpan<byte> start, ReadOnlySpan<byte> end)
    {
        _operations.WriteByte(RemoveRange);
        _operations.WriteByteArray(start);
        _operations.WriteByteArray(end);
    }

    public void LogTombstones(TombstoneSet tombstones)
    {
        _operations.WriteByte(Tombstones);
        _operations.WriteVUInt64((ulong)tombstones.Count);
        foreach (var fragment in tombstones.Items)
        {
            _operations.WriteByteArray(fragment.Start);
            _operations.WriteByteArray(fragment.End);
            _operations.WriteVInt64(fragment.CoveredBelow);
        }
    }

    /// Calls set, erase and eraseRange (start inclusive, end exclusive) for the logical effect of each logged
    /// operation, in order. Tombstone fragments follow a range erase as its consequence and are skipped.
    public void ReplayOperations(Action<byte[], byte[]> set, Action<byte[]> erase, Action<byte[], byte[]> eraseRange)
    {
        var decoder = new LsmDecoder(_operations.WrittenSpan);
        while (!decoder.End)
        {
            switch (decoder.ReadByte())
            {
                case Insert:
                {
                    var key = decoder.ReadByteArray();
                    var isSet = (decoder.ReadByte() & 1) != 0;
                    var value = decoder.ReadByteArray();
                    if (isSet) set(key, value);
                    else erase(key);
                    break;
                }
                case Update:
                {
                    var key = decoder.ReadByteArray();
                    var isSet = decoder.ReadByte() != 0;
                    var value = decoder.ReadByteArray();
                    if (isSet) set(key, value);
                    else erase(key);
                    break;
                }
                case Remove:
                    erase(decoder.ReadByteArray());
                    break;
                case RemoveRange:
                {
                    var start = decoder.ReadByteArray();
                    eraseRange(start, decoder.ReadByteArray());
                    break;
                }
                case Tombstones:
                    for (var count = decoder.ReadVUInt64(); count > 0; count--)
                    {
                        decoder.ReadByteArray();
                        decoder.ReadByteArray();
                        decoder.ReadVInt64();
                    }

                    break;
                default:
                    throw LsmDecoder.Corrupted();
            }
        }
    }

    public byte[] Finish(in WalCommitMetadata metadata)
    {
        var record = new LsmEncoder(_operations.Length + 64);
        record.WriteVUInt64((ulong)metadata.TransactionNumber);
        record.WriteVUInt64(metadata.CommitUlong);
        record.WriteVUInt64((ulong)metadata.Ulongs.Length);
        foreach (var value in metadata.Ulongs) record.WriteVUInt64(value);
        record.WriteBytes(_operations.WrittenSpan);
        return record.WrittenSpan.ToArray();
    }

    /// Applies a commit record to the memtable of its generation; returns its metadata.
    public static WalCommitMetadata Replay(IMemtable memtable, ReadOnlySpan<byte> record)
    {
        var decoder = new LsmDecoder(record);
        var transactionNumber = (long)decoder.ReadVUInt64();
        var commitUlong = decoder.ReadVUInt64();
        var ulongs = new ulong[decoder.ReadVUInt32()];
        for (var i = 0; i < ulongs.Length; i++) ulongs[i] = decoder.ReadVUInt64();
        while (!decoder.End)
        {
            switch (decoder.ReadByte())
            {
                case Insert:
                {
                    var key = decoder.ReadByteArray();
                    var flags = decoder.ReadByte();
                    // Insert adds a node without looking for one, so a present key would be held twice.
                    if (memtable.TryGetNode(key, out _, out _))
                        throw new BTDBException("WAL replay inserts a key already in its memtable.");
                    memtable.Insert(key, (flags & 1) != 0, (flags & 2) != 0, decoder.Take(decoder.ReadVUInt32()));
                    break;
                }
                case Update:
                {
                    var key = decoder.ReadByteArray();
                    var isSet = decoder.ReadByte() != 0;
                    if (!memtable.UpdateExisting(key, isSet, decoder.Take(decoder.ReadVUInt32())))
                        throw new BTDBException("WAL replay updates a key missing from its memtable.");
                    break;
                }
                case Remove:
                    memtable.Remove(decoder.ReadByteArray());
                    break;
                case RemoveRange:
                {
                    var start = decoder.ReadByteArray();
                    memtable.RemoveRange(start, decoder.ReadByteArray());
                    break;
                }
                case Tombstones:
                {
                    var fragments = new LsmTombstone[decoder.ReadVUInt32()];
                    for (var i = 0; i < fragments.Length; i++)
                        fragments[i] = new(decoder.ReadByteArray(), decoder.ReadByteArray(), decoder.ReadVInt64());
                    memtable.SetTombstones(new(fragments));
                    break;
                }
                default:
                    throw LsmDecoder.Corrupted();
            }
        }

        return new(transactionNumber, commitUlong, ulongs);
    }
}
