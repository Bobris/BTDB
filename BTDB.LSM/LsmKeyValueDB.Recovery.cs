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
    /// Database state rebuilt from storage: the newest manifest's table set plus every complete WAL record of a
    /// generation not yet in tables, replayed into one memtable per generation.
    sealed class LoadedDatabase
    {
        public required LsmManifest Manifest { get; init; }
        public required LsmVersion Version { get; init; }
        public required IMemtable Memtable { get; init; }
        public required long MemtableGeneration { get; init; }
        public required SealedMemtable[] Immutables { get; init; }
        public required WalCommitMetadata Metadata { get; init; }

        /// Highest writer epoch seen in the manifest or the WAL.
        public required long MaxEpoch { get; init; }

        /// First WAL chunk name not part of the contiguous replayed prefix.
        public required long FirstFreeChunk { get; init; }

        /// One past the highest existing WAL chunk name.
        public required long NextChunk { get; init; }

        public required Dictionary<long, long> GenerationFirstChunk { get; init; }
    }

    static string WalKey(string prefix, long chunk) => $"{prefix}wal/{chunk:D16}";

    static async ValueTask<LoadedDatabase> LoadAsync(LsmKeyValueDBOptions options, LsmBlockCache cache,
        IReadOnlyDictionary<ulong, TableReader>? openTables, CancellationToken cancellation)
    {
        var storage = options.Storage;
        string? manifestKey = null;
        await foreach (var info in storage.ListAsync(options.Prefix + "manifest/", cancellation))
            manifestKey = info.Key;
        var manifest = new LsmManifest { NextFileNumber = 1, LevelCount = options.LevelCount };
        if (manifestKey != null)
        {
            var content = await storage.TryReadAsync(manifestKey, cancellation) ??
                          throw new BTDBException($"Manifest {manifestKey} disappeared while loading.");
            manifest = LsmManifest.Decode(content, manifestKey);
        }

        var levels = Enumerable.Range(0, manifest.LevelCount).Select(_ => new List<TableReader>()).ToArray();
        foreach (var (level, fileNumber, length) in manifest.Tables)
        {
            levels[level].Add(openTables != null && openTables.TryGetValue(fileNumber, out var table)
                ? table
                : TableReader.Open(storage, cache, fileNumber, TableKey(options.Prefix, fileNumber), length,
                    options.TableTailReadSize));
        }

        var version = new LsmVersion([.. levels[0]], levels.Select(l => l.ToArray()).ToArray(),
            manifest.ValueFiles.ToDictionary(v => v.FileNumber, v => v.Length));

        var chunkNumbers = new List<long>();
        await foreach (var info in storage.ListAsync(options.Prefix + "wal/", cancellation))
        {
            var number = long.Parse(info.Key.AsSpan(info.Key.LastIndexOf('/') + 1), CultureInfo.InvariantCulture);
            if (number >= manifest.WalStartChunk) chunkNumbers.Add(number);
        }

        chunkNumbers.Sort();
        var sealedMemtables = new List<SealedMemtable>();
        var generationFirstChunk = new Dictionary<long, long>();
        IMemtable? memtable = null;
        var generation = -1L;
        var metadata = new WalCommitMetadata(manifest.TransactionNumber, manifest.CommitUlong, manifest.Ulongs);
        // Chunks are ordered by the epochs of earlier chunks only: a newer writer's manifest does not make the
        // predecessor's chunks before its epoch marker stale.
        var maxEpoch = 0L;
        var expected = manifest.WalStartChunk;
        long partialTransaction = -1;
        var partial = new List<byte[]>();
        foreach (var number in chunkNumbers)
        {
            // Only a contiguous prefix is durable history; chunks after a gap belong to commits never acknowledged.
            if (number != expected) break;
            expected++;
            var key = WalKey(options.Prefix, number);
            var content = await storage.TryReadAsync(key, cancellation) ??
                          throw new BTDBException($"WAL chunk {key} disappeared while loading.");
            var chunk = WalFormat.Decode(content, key);
            // A predecessor's chunk that landed after a newer writer's epoch marker is not history.
            if (chunk.Epoch < maxEpoch) continue;
            if (chunk.Epoch > maxEpoch)
            {
                maxEpoch = chunk.Epoch;
                partial.Clear();
                partialTransaction = -1;
            }

            if (chunk.EpochMarker)
            {
                partial.Clear();
                partialTransaction = -1;
                continue;
            }

            generationFirstChunk.TryAdd(chunk.Generation, number);
            foreach (var part in chunk.Parts)
            {
                if (part.First)
                {
                    partial.Clear();
                    partialTransaction = part.TransactionNumber;
                }
                else if (partialTransaction != part.TransactionNumber) continue;

                partial.Add(part.Payload);
                if (!part.Last) continue;
                var record = partial.Count == 1 ? partial[0] : partial.SelectMany(p => p).ToArray();
                partial.Clear();
                partialTransaction = -1;
                if (chunk.Generation <= manifest.FlushedGeneration) continue;
                if (chunk.Generation != generation)
                {
                    if (memtable != null)
                        sealedMemtables.Insert(0,
                            new(memtable, generation, metadata.CommitUlong, metadata.Ulongs,
                                metadata.TransactionNumber));
                    memtable = NewMemtable(options).CloneForWriting(NewOwner());
                    generation = chunk.Generation;
                }

                metadata = WalRecordBuilder.Replay(memtable!, record);
            }
        }

        if (memtable == null)
        {
            memtable = NewMemtable(options);
            generation = Math.Max(manifest.FlushedGeneration + 1,
                generationFirstChunk.Count == 0 ? 0 : generationFirstChunk.Keys.Max() + 1);
        }

        var highest = chunkNumbers.Count == 0 ? manifest.WalStartChunk - 1 : chunkNumbers[^1];
        return new()
        {
            Manifest = manifest, Version = version, Memtable = memtable, MemtableGeneration = generation,
            Immutables = [.. sealedMemtables], Metadata = metadata, MaxEpoch = Math.Max(maxEpoch, manifest.WriterEpoch),
            FirstFreeChunk = expected,
            NextChunk = Math.Max(highest + 1, expected), GenerationFirstChunk = generationFirstChunk
        };
    }
}
