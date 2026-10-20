using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
using BTDB.LSM.Format;

namespace BTDB.LSM;

/// Non-overlapping private tables of a spilled transaction, sorted by key, as one component. Weight counts the
/// segments merged into it.
sealed record SpilledRun(TableReader[] Tables, int Weight)
{
    public ILsmComponent Component { get; } = LsmVersion.RunComponent(Tables);
}

/// Writing transactions larger than MaxTransactionMemory (ImplementationPlan.md, section 7.5): their changes move into
/// private tables, and the commit adds those tables to level 0 by one manifest edit instead of a WAL record.
public sealed partial class LsmKeyValueDB
{
    sealed class IngestRequest(TableReader[] run, List<(ulong FileNumber, long Bytes)> valueFiles,
        WalCommitMetadata metadata)
    {
        /// The transaction's changes as one run sorted by key; empty when they cancel out.
        public TableReader[] Run => run;
        public List<(ulong FileNumber, long Bytes)> ValueFiles => valueFiles;
        public WalCommitMetadata Metadata => metadata;
        public TaskCompletionSource Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// Set by the worker while it publishes the request; a request it has not started can be withdrawn.
        public bool Started { get; set; }
    }

    readonly List<IngestRequest> _ingestRequests = [];

    /// Seals the shared memtable for a transaction that starts spilling, so background work flushes it while the
    /// transaction runs. The transaction holds the writer slot, so nothing is committed until its own commit.
    internal void SealForSpill() => RotateHoldingWriter();

    internal IMemtable NewSegmentMemtable() => NewMemtable(_options).CloneForWriting(NewOwner());

    /// Writes a spilled segment as a table and value files that no manifest references yet; null when it is empty.
    internal (TableReader Table, List<(ulong FileNumber, long Bytes)> ValueFiles)? WriteSpilledSegment(IMemtable memtable)
    {
        if (memtable.IsEmpty) return null;
        var created = new List<(ulong FileNumber, long Bytes)>();
        try
        {
            var values = NewValueSeparation(_state.Version, created, pace: false);
            var table = WriteTable(TableBuilder.Build(memtable.Enumerate().Select(e => values.Apply(e)),
                memtable.Tombstones, _tableOptions), pace: false);
            values.Finish();
            return (table, created);
        }
        catch
        {
            DiscardSpilled([], created);
            throw;
        }
    }

    /// Merges counts of runs of equal weight, like a counter in base SpillMergeFactor, so a transaction keeps at most
    /// SpillMergeFactor - 1 runs per weight and writes each change about log(segments) times. With final, merges all
    /// runs into one. Runs are newest first and stacked on snapshot; the merged tables go at once, because only this
    /// transaction ever read them.
    internal void MergeSpilled(List<SpilledRun> runs, LsmState snapshot,
        List<(ulong FileNumber, long Bytes)> valueFiles, bool final)
    {
        while (true)
        {
            var count = final ? runs.Count
                : runs.Count >= SpillMergeFactor && runs.Take(SpillMergeFactor).All(r => r.Weight == runs[0].Weight)
                    ? SpillMergeFactor
                    : 0;
            if (count < 2) return;
            var merged = runs.GetRange(0, count);
            var below = snapshot.CreateView([.. runs.Skip(count).Select(r => r.Component)]);
            var created = new List<(ulong FileNumber, long Bytes)>();
            List<TableReader> tables;
            try
            {
                tables = LsmCompaction.Merge([.. merged.Select(r => r.Component)], below, _compactionOptions,
                    _tableOptions, content => WriteTable(content, false),
                    NewValueSeparation(_state.Version, created, pace: false));
            }
            catch
            {
                DiscardSpilled([], created);
                throw;
            }

            valueFiles.AddRange(created);
            runs.RemoveRange(0, count);
            if (tables.Count > 0) runs.Insert(0, new([.. tables], merged.Sum(r => r.Weight)));
            DiscardSpilled(merged.SelectMany(r => r.Tables), []);
            if (final) return;
        }
    }

    const int SpillMergeFactor = 4;

    /// Commits a spilled transaction: its last segment becomes a table too and all its runs merge into one. The worker
    /// publishes that run as the newest level 0 run, with the transaction's commit metadata, once every sealed
    /// memtable is flushed. The manifest edit is the commit point, so the commit is durable when this returns.
    internal void CommitSpilledTransaction(LsmKeyValueDBTransaction transaction, IMemtable last,
        List<SpilledRun> runs, LsmState snapshot, List<(ulong FileNumber, long Bytes)> valueFiles,
        WalCommitMetadata metadata)
    {
        IngestRequest? request = null;
        try
        {
            if (!_isWriter || transaction.WriterEpoch != _epoch)
                throw new LsmWriterFencedException("The writer lease was lost; the transaction was not committed.");
            if (WriteSpilledSegment(last) is { } written)
            {
                runs.Insert(0, new([written.Table], 1));
                valueFiles.AddRange(written.ValueFiles);
            }

            MergeSpilled(runs, snapshot, valueFiles, true);
            request = new(runs.Count == 0 ? [] : runs[0].Tables, valueFiles, metadata);
            lock (_ingestRequests) _ingestRequests.Add(request);
            RequestWork();
            while (Task.WaitAny([request.Done.Task], 100) < 0)
            {
                if (!_isWriter) throw new LsmWriterFencedException("The writer lease was lost before the commit.");
                ThrowIfWorkerKeepsFailing();
            }

            request.Done.Task.GetAwaiter().GetResult();
        }
        catch
        {
            if (request == null || Withdraw(request)) DiscardSpilled(runs.SelectMany(r => r.Tables), valueFiles);
            throw;
        }
        finally
        {
            // Only this transaction used its last segment.
            last.Arena?.Dispose();
            ReleaseWriter();
        }
    }

    /// Removes a request the worker has not started; false when it is running or already published.
    bool Withdraw(IngestRequest request)
    {
        lock (_ingestRequests) return !request.Started && _ingestRequests.Remove(request);
    }

    /// Queues the private objects of a rolled back or failed spilled transaction for deletion; no reader ever saw them.
    /// An instance that is no longer the writer leaves them to the next writer's orphan search.
    internal void DiscardSpilled(IEnumerable<TableReader> tables, IEnumerable<(ulong FileNumber, long Bytes)> valueFiles)
    {
        if (!_isWriter) return;
        lock (_obsoleteObjects)
        {
            foreach (var table in tables)
                _obsoleteObjects.Add(new(LsmObjectKind.Table, table.FileNumber, 0, DateTimeOffset.MinValue));
            foreach (var (fileNumber, _) in valueFiles)
                _obsoleteObjects.Add(new(LsmObjectKind.ValueFile, fileNumber, 0, DateTimeOffset.MinValue));
        }
    }

    /// Publishes the oldest waiting spilled transaction; RunOneUnit calls it only when no sealed memtable is left.
    bool RunIngest()
    {
        IngestRequest? request;
        lock (_ingestRequests)
        {
            request = _ingestRequests.FirstOrDefault();
            if (request == null) return false;
            request.Started = true;
        }

        try
        {
            var state = _state;
            // The transaction held the writer slot since it sealed the shared memtable, so the mutable memtable is
            // still empty and has no WAL chunk: its generation becomes the transaction's.
            if (!state.Memtable.IsEmpty)
                throw new InvalidOperationException("BTDB.LSM spilled commit found a non-empty memtable.");
            var metadata = request.Metadata;
            var version = request.Run.Length == 0
                ? state.Version
                : state.Version.WithLevel0Runs([request.Run, .. state.Version.Level0Runs]);
            PublishVersion(version, [],
                request.ValueFiles,
                new(state.Memtable, state.MemtableGeneration, metadata.CommitUlong, metadata.Ulongs,
                    metadata.TransactionNumber), committed: true);
        }
        catch
        {
            lock (_ingestRequests) request.Started = false;
            throw;
        }

        lock (_ingestRequests) _ingestRequests.Remove(request);
        long durable;
        lock (_walLock)
            durable = _durableTransaction = Math.Max(_durableTransaction, request.Metadata.TransactionNumber);
        CompleteDurabilityWaiters(durable, null);
        request.Done.TrySetResult();
        return true;
    }

    /// Fails spilled commits waiting for the worker when the writer role ends.
    void FailIngestRequests(Exception reason)
    {
        IngestRequest[] requests;
        lock (_ingestRequests)
        {
            requests = [.. _ingestRequests];
            _ingestRequests.Clear();
        }

        foreach (var request in requests) request.Done.TrySetException(reason);
    }
}
