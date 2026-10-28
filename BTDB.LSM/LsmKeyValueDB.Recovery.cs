using System;
using System.Runtime.CompilerServices;
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
    sealed class WalReplay(LsmKeyValueDBOptions options, long flushedGeneration, long nextChunk, long epoch,
        WalCommitMetadata metadata)
    {
        readonly List<byte[]> _partial = [];
        long _partialTransaction = -1;

        // The first own chunk of the MaxEpoch writer, and whether a predecessor's chunk took one of its own names.
        long _firstOwnChunk = nextChunk;
        bool _broken;

        public long FlushedGeneration { get; private set; } = flushedGeneration;

        /// Next WAL chunk of the contiguous history to read.
        public long NextChunk { get; set; } = nextChunk;

        public long MaxEpoch { get; private set; } = epoch;
        public IMemtable? Memtable { get; private set; }
        public long Generation { get; private set; } = -1;

        /// Complete generations before the current one, newest first.
        public List<SealedMemtable> Sealed { get; } = [];

        public WalCommitMetadata Metadata { get; private set; } = metadata;
        /// The first chunk of each generation with the epoch of its writer.
        public Dictionary<long, (long Chunk, long Epoch)> GenerationFirstChunk { get; } = new();

        /// Starts a round of changes: the current memtable is copied, so snapshots of the previous round never change.
        public void BeginRound()
        {
            if (Memtable != null) Memtable = Memtable.CloneForWriting(NewOwner());
        }

        public void Apply(long number, WalChunk chunk)
        {
            NextChunk = number + 1;
            // A predecessor's chunk that landed after a newer writer's epoch marker is not history. Chunks are
            // ordered by the epochs of earlier chunks only, never by the manifest's epoch. When it took one of the
            // newer writer's own names, that writer was fenced there, and its later chunks follow a missing one
            // (BTDB.LSM/Proofs, Wal.lean).
            if (chunk.Epoch < MaxEpoch)
            {
                if (number >= _firstOwnChunk)
                {
                    _broken = true;
                    DropPartial();
                }

                return;
            }

            if (chunk.Epoch > MaxEpoch)
            {
                MaxEpoch = chunk.Epoch;
                _firstOwnChunk = chunk.EpochMarker ? chunk.FirstOwnChunk : number;
                _broken = false;
                DropPartial();
            }

            if (chunk.EpochMarker)
            {
                DropPartial();
                return;
            }

            if (_broken) return;
            GenerationFirstChunk.TryAdd(chunk.Generation, (number, chunk.Epoch));
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
                // Earlier chunks are in tables; replay continues in the epoch of the writer of the start chunk.
                NextChunk = manifest.WalStartChunk;
                MaxEpoch = Math.Max(MaxEpoch, manifest.WalStartEpoch);
                _firstOwnChunk = manifest.WalStartChunk;
                _broken = false;
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

        /// The replay's current memtable; the replay creates it once, on first access, when the WAL had none.
        public IMemtable Memtable => Replay.Current().Memtable;
        public long MemtableGeneration => Replay.Current().Generation;
        public SealedMemtable[] Immutables => [.. Replay.Sealed];
        public WalCommitMetadata Metadata => Replay.Metadata;

        /// Highest writer epoch seen in the manifest or the WAL.
        public long MaxEpoch => Math.Max(Replay.MaxEpoch, Manifest.WriterEpoch);

        /// First WAL chunk name not part of the contiguous replayed prefix.
        public long FirstFreeChunk => Replay.NextChunk;

        public Dictionary<long, (long Chunk, long Epoch)> GenerationFirstChunk => Replay.GenerationFirstChunk;

        /// WAL chunk names below NextChunk that did not exist when the WAL was listed, from FirstFreeChunk on.
        public required List<long> MissingChunks { get; init; }
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

    const int WalReadParallelism = 16;

    /// Reads WAL chunks in order with up to WalReadParallelism reads in flight; a missing chunk yields null content.
    static async IAsyncEnumerable<(long Number, byte[]? Content)> ReadChunksAsync(LsmKeyValueDBOptions options,
        IReadOnlyList<long> numbers, [EnumeratorCancellation] CancellationToken cancellation)
    {
        var reads = new Queue<(long, Task<byte[]?>)>();
        var next = 0;
        while (next < numbers.Count || reads.Count > 0)
        {
            while (next < numbers.Count && reads.Count < WalReadParallelism)
            {
                var number = numbers[next++];
                reads.Enqueue((number,
                    options.Storage.TryReadAsync(WalKey(options.Prefix, number), cancellation).AsTask()));
            }

            var (read, content) = reads.Dequeue();
            yield return (read, await content);
        }
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

        var runs = new List<TableReader[]>();
        var next = 0;
        foreach (var length in manifest.Level0RunLengths)
        {
            runs.Add(levels[0].GetRange(next, length).ToArray());
            next += length;
        }

        if (next != levels[0].Count) throw new BTDBException("Manifest level 0 runs do not match its tables.");
        return new([.. runs], levels.Select(l => l.ToArray()).ToArray(),
            manifest.ValueFiles.ToDictionary(v => v.FileNumber, v => v.Length));
    }

    static async ValueTask<LoadedDatabase> LoadAsync(LsmKeyValueDBOptions options, LsmBlockCache cache,
        IReadOnlyDictionary<ulong, TableReader>? openTables, CancellationToken cancellation)
    {
        var (manifest, _) = await ReadNewestManifestAsync(options, cancellation);
        var version = BuildVersion(options, cache, manifest, openTables);
        var replay = new WalReplay(options, manifest.FlushedGeneration, manifest.WalStartChunk, manifest.WalStartEpoch,
            new(manifest.TransactionNumber, manifest.CommitUlong, manifest.Ulongs));
        var (nextChunk, missing) = await ReplayListedWalAsync(options, replay, cancellation);
        return new()
        {
            Manifest = manifest, Version = version, Replay = replay, NextChunk = nextChunk, MissingChunks = missing
        };
    }

    /// Lists the WAL and replays its contiguous prefix from the replay's next chunk; returns one past the highest
    /// existing chunk name, which a new writer must not reuse, and the names below it that did not exist from the
    /// first one the replay did not read on, which a new writer must fence.
    static async ValueTask<(long NextChunk, List<long> Missing)> ReplayListedWalAsync(LsmKeyValueDBOptions options,
        WalReplay replay, CancellationToken cancellation)
    {
        var chunkNumbers = new List<long>();
        await foreach (var info in options.Storage.ListAsync(options.Prefix + "wal/", cancellation))
        {
            var number = long.Parse(info.Key.AsSpan(info.Key.LastIndexOf('/') + 1), CultureInfo.InvariantCulture);
            if (number >= replay.NextChunk) chunkNumbers.Add(number);
        }

        chunkNumbers.Sort();
        // Only a contiguous prefix is durable history; chunks after a gap belong to commits never acknowledged.
        var contiguous = new List<long>();
        foreach (var number in chunkNumbers)
        {
            if (number != replay.NextChunk + contiguous.Count) break;
            contiguous.Add(number);
        }

        await foreach (var (number, content) in ReadChunksAsync(options, contiguous, cancellation))
        {
            if (content == null) break;
            replay.Apply(number, WalFormat.Decode(content, WalKey(options.Prefix, number)));
        }

        var next = Math.Max(chunkNumbers.Count == 0 ? replay.NextChunk : chunkNumbers[^1] + 1, replay.NextChunk);
        var listed = chunkNumbers.ToHashSet();
        var missing = new List<long>();
        for (var number = replay.NextChunk; number < next; number++)
            if (!listed.Contains(number)) missing.Add(number);
        return (next, missing);
    }
}
