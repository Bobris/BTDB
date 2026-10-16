using System;
using BTDB.KVDBLayer;
using BTDB.LSM.Format;

namespace BTDB.LSM.Engine;

/// A memtable sealed by rotation, waiting to be flushed, with its WAL generation and the commit metadata as of its
/// last commit.
sealed record SealedMemtable(IMemtable Memtable, long Generation, ulong CommitUlong, ulong[] Ulongs,
    long TransactionNumber);

/// Immutable published database state: the mutable memtable's committed version, sealed memtables waiting for flush
/// (newest first), the table set and commit metadata. A snapshot pins one state; nothing later changes it
/// (ImplementationPlan.md, invariant L4).
sealed class LsmState(
    IMemtable memtable,
    long memtableGeneration,
    SealedMemtable[] immutables,
    LsmVersion version,
    ulong commitUlong,
    ulong[] ulongs,
    long transactionNumber,
    long commitSequence,
    long sequence)
{
    public IMemtable Memtable { get; } = memtable;

    /// WAL generation of the mutable memtable; sealing starts the next generation.
    public long MemtableGeneration { get; } = memtableGeneration;

    public SealedMemtable[] Immutables { get; } = immutables;
    public LsmVersion Version { get; } = version;
    public ulong CommitUlong { get; } = commitUlong;
    public ulong[] Ulongs { get; } = ulongs;
    public long TransactionNumber { get; } = transactionNumber;

    /// Increases with every commit; an upgradable transaction may start writing only if no commit happened since its
    /// snapshot.
    public long CommitSequence { get; } = commitSequence;

    /// Increases with every published state; used to decide when obsolete objects can be deleted.
    public long Sequence { get; } = sequence;

    public LsmView CreateView(IMemtable memtable)
    {
        var components = new ILsmComponent[1 + Immutables.Length + Version.Components.Length];
        components[0] = memtable;
        for (var i = 0; i < Immutables.Length; i++) components[1 + i] = Immutables[i].Memtable;
        Version.Components.CopyTo(components, 1 + Immutables.Length);
        return new(components);
    }
}

/// Manifest object content (ImplementationPlan.md, section 11): a full checkpoint of the table set, the value files,
/// the commit metadata of the newest flushed generation and the WAL position from which recovery replays. Objects are
/// manifest/{sequence}, created once; the sequence doubles as the writer fence.
sealed class LsmManifest
{
    const ulong Magic = 0x4D4D534C42445442; // "BTDBLSMM"
    const int FormatVersion = 4;

    public long Sequence { get; init; }
    public ulong NextFileNumber { get; init; }
    public long TransactionNumber { get; init; }
    public ulong CommitUlong { get; init; }
    public ulong[] Ulongs { get; init; } = [];
    public int LevelCount { get; init; }

    /// Epoch of the writer session that wrote this manifest; a new writer fences by writing a higher one.
    public long WriterEpoch { get; init; }

    /// Every WAL generation up to this one is in tables.
    public long FlushedGeneration { get; init; }

    /// First WAL chunk that may hold a record of a generation not yet in tables.
    public long WalStartChunk { get; init; }

    /// Level 0 tables newest first, then every other level sorted by key.
    public (int Level, ulong FileNumber, long Length)[] Tables { get; init; } = [];

    public (ulong FileNumber, long Length)[] ValueFiles { get; init; } = [];

    public byte[] Encode()
    {
        var encoder = new LsmEncoder();
        encoder.WriteUInt64(Magic);
        encoder.WriteVUInt64(FormatVersion);
        encoder.WriteVInt64(Sequence);
        encoder.WriteVUInt64(NextFileNumber);
        encoder.WriteVInt64(TransactionNumber);
        encoder.WriteVUInt64(CommitUlong);
        encoder.WriteVUInt64((ulong)Ulongs.Length);
        foreach (var value in Ulongs) encoder.WriteVUInt64(value);
        encoder.WriteVUInt64((ulong)LevelCount);
        encoder.WriteVUInt64((ulong)WriterEpoch);
        encoder.WriteVUInt64((ulong)FlushedGeneration);
        encoder.WriteVUInt64((ulong)WalStartChunk);
        encoder.WriteVUInt64((ulong)Tables.Length);
        foreach (var (level, fileNumber, length) in Tables)
        {
            encoder.WriteVUInt64((ulong)level);
            encoder.WriteVUInt64(fileNumber);
            encoder.WriteVUInt64((ulong)length);
        }

        encoder.WriteVUInt64((ulong)ValueFiles.Length);
        foreach (var (fileNumber, length) in ValueFiles)
        {
            encoder.WriteVUInt64(fileNumber);
            encoder.WriteVUInt64((ulong)length);
        }

        encoder.WriteChecksum(0);
        return encoder.WrittenSpan.ToArray();
    }

    public static LsmManifest Decode(ReadOnlySpan<byte> data, string objectName)
    {
        var decoder = new LsmDecoder(LsmChecksum.Verify(data, objectName));
        if (decoder.ReadUInt64() != Magic) throw new BTDBException($"Manifest {objectName} has an unknown format.");
        if (decoder.ReadVUInt64() != FormatVersion)
            throw new BTDBException($"Manifest {objectName} has an unsupported format version.");
        var sequence = decoder.ReadVInt64();
        var nextFileNumber = decoder.ReadVUInt64();
        var transactionNumber = decoder.ReadVInt64();
        var commitUlong = decoder.ReadVUInt64();
        var ulongs = new ulong[decoder.ReadVUInt32()];
        for (var i = 0; i < ulongs.Length; i++) ulongs[i] = decoder.ReadVUInt64();
        var levelCount = decoder.ReadVUInt32();
        var writerEpoch = (long)decoder.ReadVUInt64();
        var flushedGeneration = (long)decoder.ReadVUInt64();
        var walStartChunk = (long)decoder.ReadVUInt64();
        var tables = new (int, ulong, long)[decoder.ReadVUInt32()];
        for (var i = 0; i < tables.Length; i++)
            tables[i] = (decoder.ReadVUInt32(), decoder.ReadVUInt64(), (long)decoder.ReadVUInt64());
        var valueFiles = new (ulong, long)[decoder.ReadVUInt32()];
        for (var i = 0; i < valueFiles.Length; i++) valueFiles[i] = (decoder.ReadVUInt64(), (long)decoder.ReadVUInt64());
        return new()
        {
            Sequence = sequence, NextFileNumber = nextFileNumber, TransactionNumber = transactionNumber,
            CommitUlong = commitUlong, Ulongs = ulongs, LevelCount = levelCount, Tables = tables,
            ValueFiles = valueFiles, WriterEpoch = writerEpoch, FlushedGeneration = flushedGeneration,
            WalStartChunk = walStartChunk
        };
    }
}
