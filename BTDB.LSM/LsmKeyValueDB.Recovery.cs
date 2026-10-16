using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
using BTDB.LSM.Format;

namespace BTDB.LSM;

public sealed partial class LsmKeyValueDB
{
    /// Replays WAL chunks into one memtable per generation not yet in tables (ImplementationPlan.md, sections 14
    /// and 15). Used by recovery and kept by readers, which continue it on every refresh.
    sealed class WalReplay(LsmKeyValueDBOptions options, long flushedGeneration, long nextChunk,
        WalCommitMetadata metadata)
    {
        readonly List<byte[]> _partial = [];
        long _partialTransaction = -1;

        public long FlushedGeneration { get; private set; } = flushedGeneration;

        /// Next WAL chunk of the contiguous history to read.
        public long NextChunk { get; set; } = nextChunk;

        public long MaxEpoch { get; private set; }
        public IMemtable? Memtable { get; private set; }
        public long Generation { get; private set; } = -1;

        /// Complete generations before the current one, newest first.
        public List<SealedMemtable> Sealed { get; } = [];

        public WalCommitMetadata Metadata { get; private set; } = metadata;
        public Dictionary<long, long> GenerationFirstChunk { get; } = new();

        /// Starts a round of changes: the current memtable is copied, so snapshots of the previous round never change.
        public void BeginRound()
        {
            if (Memtable != null) Memtable = Memtable.CloneForWriting(NewOwner());
        }

        public void Apply(long number, WalChunk chunk)
        {
            NextChunk = number + 1;
            // A predecessor's chunk that landed after a newer writer's epoch marker is not history. Chunks are
            // ordered by the epochs of earlier chunks only, never by the manifest's epoch.
            if (chunk.Epoch < MaxEpoch) return;
            if (chunk.Epoch > MaxEpoch)
            {
                MaxEpoch = chunk.Epoch;
                DropPartial();
            }

            if (chunk.EpochMarker)
            {
                DropPartial();
                return;
            }

            GenerationFirstChunk.TryAdd(chunk.Generation, number);
            foreach (var part in chunk.Parts)
            {
                if (part.First)
                {
                    _partial.Clear();
                    _partialTransaction = part.TransactionNumber;
                }
                else if (_partialTransaction != part.TransactionNumber) continue;

                _partial.Add(part.Payload);
                if (!part.Last) continue;
                var record = _partial.Count == 1 ? _partial[0] : _partial.SelectMany(p => p).ToArray();
                DropPartial();
                if (chunk.Generation <= FlushedGeneration) continue;
                if (chunk.Generation != Generation)
                {
                    if (Memtable != null)
                        Sealed.Insert(0, new(Memtable, Generation, Metadata.CommitUlong, Metadata.Ulongs,
                            Metadata.TransactionNumber));
                    Memtable = NewMemtable(options).CloneForWriting(NewOwner());
                    Generation = chunk.Generation;
                }

                Metadata = WalRecordBuilder.Replay(Memtable!, record);
            }
        }

        void DropPartial()
        {
            _partial.Clear();
            _partialTransaction = -1;
        }

        /// A newer manifest has every generation up to flushed in tables; their memtables are no longer needed.
        public void AdvanceFlushed(LsmManifest manifest)
        {
            if (manifest.FlushedGeneration <= FlushedGeneration) return;
            FlushedGeneration = manifest.FlushedGeneration;
            Sealed.RemoveAll(m => m.Generation <= FlushedGeneration);
            if (Generation <= FlushedGeneration)
            {
                Memtable = null;
                Generation = -1;
                Metadata = new(manifest.TransactionNumber, manifest.CommitUlong, manifest.Ulongs);
                if (Sealed.Count > 0)
                {
                    var newest = Sealed[0];
                    Metadata = new(newest.TransactionNumber, newest.CommitUlong, newest.Ulongs);
                }
            }

            if (NextChunk < manifest.WalStartChunk)
            {
                NextChunk = manifest.WalStartChunk;
                DropPartial();
            }
        }

        /// The mutable memtable to publish: the current generation's, or an empty one for the next generation.
        public (IMemtable Memtable, long Generation) Current()
        {
            if (Memtable != null) return (Memtable, Generation);
            var next = Math.Max(FlushedGeneration + 1, GenerationFirstChunk.Count == 0 ? 0 : GenerationFirstChunk.Keys.Max() + 1);
            Memtable = NewMemtable(options);
            Generation = next;
            return (Memtable, Generation);
        }
    }

    /// Database state rebuilt from storage: the newest manifest's table set plus every complete WAL record of a
    /// generation not yet in tables.
    sealed class LoadedDatabase
    {
        public required LsmManifest Manifest { get; init; }
        public required LsmVersion Version { get; init; }
        public required WalReplay Replay { get; init; }

        /// One past the highest existing WAL chunk name.
        public required long NextChunk { get; init; }

        public IMemtable Memtable => Replay.Current().Memtable;
        public long MemtableGeneration => Replay.Current().Generation;
        public SealedMemtable[] Immutables => [.. Replay.Sealed];
        public WalCommitMetadata Metadata => Replay.Metadata;

        /// Highest writer epoch seen in the manifest or the WAL.
        public long MaxEpoch => Math.Max(Replay.MaxEpoch, Manifest.WriterEpoch);

        /// First WAL chunk name not part of the contiguous replayed prefix.
        public long FirstFreeChunk => Replay.NextChunk;

        public Dictionary<long, long> GenerationFirstChunk => Replay.GenerationFirstChunk;
    }

    static string WalKey(string prefix, long chunk) => $"{prefix}wal/{chunk:D16}";

    static async ValueTask<(LsmManifest Manifest, string? Key)> ReadNewestManifestAsync(LsmKeyValueDBOptions options,
        CancellationToken cancellation)
    {
        string? manifestKey = null;
        await foreach (var info in options.Storage.ListAsync(options.Prefix + "manifest/", cancellation))
            manifestKey = info.Key;
        if (manifestKey == null) return (new() { NextFileNumber = 1, LevelCount = options.LevelCount }, null);
        var content = await options.Storage.TryReadAsync(manifestKey, cancellation) ??
                      throw new BTDBException($"Manifest {manifestKey} disappeared while loading.");
        return (LsmManifest.Decode(content, manifestKey), manifestKey);
    }

    static LsmVersion BuildVersion(LsmKeyValueDBOptions options, LsmBlockCache cache, LsmManifest manifest,
        IReadOnlyDictionary<ulong, TableReader>? openTables)
    {
        var levels = Enumerable.Range(0, manifest.LevelCount).Select(_ => new List<TableReader>()).ToArray();
        foreach (var (level, fileNumber, length) in manifest.Tables)
        {
            levels[level].Add(openTables != null && openTables.TryGetValue(fileNumber, out var table)
                ? table
                : TableReader.Open(options.Storage, cache, cache.Scope(options.Storage, options.Prefix), fileNumber,
                    TableKey(options.Prefix, fileNumber), length, options.TableTailReadSize));
        }

        return new([.. levels[0]], levels.Select(l => l.ToArray()).ToArray(),
            manifest.ValueFiles.ToDictionary(v => v.FileNumber, v => v.Length));
    }

    static async ValueTask<LoadedDatabase> LoadAsync(LsmKeyValueDBOptions options, LsmBlockCache cache,
        IReadOnlyDictionary<ulong, TableReader>? openTables, CancellationToken cancellation)
    {
        var storage = options.Storage;
        var (manifest, _) = await ReadNewestManifestAsync(options, cancellation);
        var version = BuildVersion(options, cache, manifest, openTables);
        var chunkNumbers = new List<long>();
        await foreach (var info in storage.ListAsync(options.Prefix + "wal/", cancellation))
        {
            var number = long.Parse(info.Key.AsSpan(info.Key.LastIndexOf('/') + 1), CultureInfo.InvariantCulture);
            if (number >= manifest.WalStartChunk) chunkNumbers.Add(number);
        }

        chunkNumbers.Sort();
        var replay = new WalReplay(options, manifest.FlushedGeneration, manifest.WalStartChunk,
            new(manifest.TransactionNumber, manifest.CommitUlong, manifest.Ulongs));
        foreach (var number in chunkNumbers)
        {
            // Only a contiguous prefix is durable history; chunks after a gap belong to commits never acknowledged.
            if (number != replay.NextChunk) break;
            var key = WalKey(options.Prefix, number);
            var content = await storage.TryReadAsync(key, cancellation);
            if (content == null) break;
            replay.Apply(number, WalFormat.Decode(content, key));
        }

        var highest = chunkNumbers.Count == 0 ? manifest.WalStartChunk - 1 : chunkNumbers[^1];
        return new()
        {
            Manifest = manifest, Version = version, Replay = replay,
            NextChunk = Math.Max(highest + 1, replay.NextChunk)
        };
    }
}
