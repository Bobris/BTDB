using System;
using BTDB.KVDBLayer;
using BTDB.LSM.Format;

namespace BTDB.LSM.Engine;

/// Immutable published database state: the committed memtable, the tables (newest first) and commit metadata. A
/// snapshot pins one state; nothing later changes it (ImplementationPlan.md, invariant L4).
sealed class LsmState(
    IMemtable memtable,
    LsmVersion version,
    ulong commitUlong,
    ulong[] ulongs,
    long transactionNumber,
    long sequence)
{
    public IMemtable Memtable { get; } = memtable;
    public LsmVersion Version { get; } = version;
    public ulong CommitUlong { get; } = commitUlong;
    public ulong[] Ulongs { get; } = ulongs;
    public long TransactionNumber { get; } = transactionNumber;

    /// Increases with every published state; used to decide when obsolete tables can be deleted.
    public long Sequence { get; } = sequence;

    public LsmView CreateView(IMemtable memtable)
    {
        var components = new ILsmComponent[Version.Components.Length + 1];
        components[0] = memtable;
        Version.Components.CopyTo(components, 1);
        return new(components);
    }
}

/// Manifest object content. Until phase L3 every manifest object is a full checkpoint of the table set and metadata;
/// incremental version edits come with the WAL in phase L3 (ImplementationPlan.md, section 11).
sealed class LsmManifest
{
    const ulong Magic = 0x4D4D534C42445442; // "BTDBLSMM"
    const int FormatVersion = 3;

    public long Sequence { get; init; }
    public ulong NextFileNumber { get; init; }
    public long TransactionNumber { get; init; }
    public ulong CommitUlong { get; init; }
    public ulong[] Ulongs { get; init; } = [];
    public int LevelCount { get; init; }

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
        var tables = new (int, ulong, long)[decoder.ReadVUInt32()];
        for (var i = 0; i < tables.Length; i++)
            tables[i] = (decoder.ReadVUInt32(), decoder.ReadVUInt64(), (long)decoder.ReadVUInt64());
        var valueFiles = new (ulong, long)[decoder.ReadVUInt32()];
        for (var i = 0; i < valueFiles.Length; i++) valueFiles[i] = (decoder.ReadVUInt64(), (long)decoder.ReadVUInt64());
        return new()
        {
            Sequence = sequence, NextFileNumber = nextFileNumber, TransactionNumber = transactionNumber,
            CommitUlong = commitUlong, Ulongs = ulongs, LevelCount = levelCount, Tables = tables,
            ValueFiles = valueFiles
        };
    }
}
