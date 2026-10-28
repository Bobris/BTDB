using System;
using System.Collections.Generic;
using System.Linq;
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
    long sequence,
    long manifestSequence = 0)
{
    /// Manifest whose table set this state's version is; readers report the oldest one they use.
    public long ManifestSequence { get; } = manifestSequence;

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

    /// View of components stacked on this whole state, for example a spilled transaction's memtable and private runs.
    public LsmView CreateView(IReadOnlyList<ILsmComponent> above)
    {
        var components = new List<ILsmComponent>(1 + above.Count + Immutables.Length + Version.Components.Length);
        components.AddRange(above);
        components.Add(Memtable);
        foreach (var sealedMemtable in Immutables) components.Add(sealedMemtable.Memtable);
        components.AddRange(Version.Components);
        return new([.. components]);
    }

    public LsmView CreateView(IMemtable memtable)
    {
        var components = new ILsmComponent[1 + Immutables.Length + Version.Components.Length];
        components[0] = memtable;
        for (var i = 0; i < Immutables.Length; i++) components[1 + i] = Immutables[i].Memtable;
        Version.Components.CopyTo(components, 1 + Immutables.Length);
        return new(components);
    }
}

/// Kind of an object waiting for deletion. Manifests carry pending tables, value files and manifests; covered WAL
/// chunks are deleted at once.
enum LsmObjectKind : byte
{
    Table,
    ValueFile,
    Manifest,
    WalChunk
}

/// An object no current state references, obsolete since Since; Number is its file, manifest or chunk number.
readonly record struct LsmPendingDeletion(LsmObjectKind Kind, ulong Number, DateTimeOffset Since);

/// Manifest object content (ImplementationPlan.md, section 11): a full checkpoint of the table set, the value files,
/// the commit metadata of the newest flushed generation, the WAL position from which recovery replays and the objects
/// waiting for deletion. Objects are manifest/{sequence}, created once; the sequence doubles as the writer fence.
sealed class LsmManifest
{
    const ulong Magic = 0x4D4D534C42445442; // "BTDBLSMM"
    const int FormatVersion = 7;

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

    /// Epoch of the writer whose chunk WalStartChunk is; replay starting there ignores lower epochs.
    public long WalStartEpoch { get; init; }

    /// Level 0 tables newest run first, each run sorted by key, then every other level sorted by key.
    public (int Level, ulong FileNumber, long Length)[] Tables { get; init; } = [];

    /// Number of tables in each level 0 run, newest first.
    public int[] Level0RunLengths { get; init; } = [];

    public (ulong FileNumber, long Length)[] ValueFiles { get; init; } = [];

    /// Objects the writer deletes once DeletionGracePeriod passed since they became obsolete; a new writer continues
    /// with them, so takeovers do not restart the period.
    public LsmPendingDeletion[] PendingDeletions { get; init; } = [];

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
        encoder.WriteVUInt64((ulong)WalStartEpoch);
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

        encoder.WriteVUInt64((ulong)Level0RunLengths.Length);
        foreach (var length in Level0RunLengths) encoder.WriteVUInt64((ulong)length);

        // Pending deletions are grouped by time and stored as runs of consecutive numbers: the writer rounds the times
        // up, so the manifests replaced within one period form one run.
        var groups = PendingDeletions.GroupBy(d => d.Since.UtcTicks).OrderBy(g => g.Key).ToArray();
        encoder.WriteVUInt64((ulong)groups.Length);
        var previousTicks = 0L;
        foreach (var group in groups)
        {
            encoder.WriteVInt64(group.Key - previousTicks);
            previousTicks = group.Key;
            var runs = new List<(LsmObjectKind Kind, ulong Start, ulong Length)>();
            foreach (var deletion in group.OrderBy(d => d.Kind).ThenBy(d => d.Number).Distinct())
            {
                if (runs.Count > 0 && runs[^1] is var last && last.Kind == deletion.Kind &&
                    last.Start + last.Length == deletion.Number)
                    runs[^1] = last with { Length = last.Length + 1 };
                else
                    runs.Add((deletion.Kind, deletion.Number, 1));
            }

            encoder.WriteVUInt64((ulong)runs.Count);
            foreach (var (kind, start, length) in runs)
            {
                encoder.WriteByte((byte)kind);
                encoder.WriteVUInt64(start);
                encoder.WriteVUInt64(length);
            }
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
        var walStartEpoch = (long)decoder.ReadVUInt64();
        var tables = new (int, ulong, long)[decoder.ReadVUInt32()];
        for (var i = 0; i < tables.Length; i++)
            tables[i] = (decoder.ReadVUInt32(), decoder.ReadVUInt64(), (long)decoder.ReadVUInt64());
        var valueFiles = new (ulong, long)[decoder.ReadVUInt32()];
        for (var i = 0; i < valueFiles.Length; i++) valueFiles[i] = (decoder.ReadVUInt64(), (long)decoder.ReadVUInt64());
        var runLengths = new int[decoder.ReadVUInt32()];
        for (var i = 0; i < runLengths.Length; i++) runLengths[i] = (int)decoder.ReadVUInt32();
        var pending = new List<LsmPendingDeletion>();
        var ticks = 0L;
        for (var groups = decoder.ReadVUInt64(); groups > 0; groups--)
        {
            ticks += decoder.ReadVInt64();
            for (var runs = decoder.ReadVUInt64(); runs > 0; runs--)
            {
                var kind = (LsmObjectKind)decoder.ReadByte();
                if (kind > LsmObjectKind.WalChunk) throw LsmDecoder.Corrupted();
                var start = decoder.ReadVUInt64();
                for (var length = decoder.ReadVUInt64(); length > 0; length--)
                    pending.Add(new(kind, start++, new(ticks, TimeSpan.Zero)));
            }
        }

        return new()
        {
            Sequence = sequence, NextFileNumber = nextFileNumber, TransactionNumber = transactionNumber,
            CommitUlong = commitUlong, Ulongs = ulongs, LevelCount = levelCount, Tables = tables,
            ValueFiles = valueFiles, WriterEpoch = writerEpoch, FlushedGeneration = flushedGeneration,
            WalStartChunk = walStartChunk, WalStartEpoch = walStartEpoch, PendingDeletions = [.. pending], Level0RunLengths = runLengths
        };
    }
}
