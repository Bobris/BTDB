using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbLargeVersionedRelationTest(ITestOutputHelper output)
{
    public enum PipelineState { Draft, Production, Invalidated }

    public class Pipeline
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        [PrimaryKey(3), SecondaryKey("StateVersion", Order = 4)]
        public ulong VersionNumber { get; set; }
        [SecondaryKey("StateVersion", Order = 3, IncludePrimaryKeyOrder = 2)]
        public PipelineState VersionState { get; set; }
        [SecondaryKey("Date", IncludePrimaryKeyOrder = 3)] public DateTime LastUpdate { get; set; }
        [SecondaryKey("SourceChangeSet", IncludePrimaryKeyOrder = 1)] public Guid? SourceChangeSet { get; set; }
        public string Content { get; set; } = "";
    }

    public interface IPipelineTable : IRelation<Pipeline>
    {
        bool RemoveById(ulong companyId, ulong id, ulong versionNumber);
        Pipeline? FirstByStateVersionOrDefault(Constraint<ulong> companyId, Constraint<ulong> id,
            Constraint<PipelineState> versionState, IOrderer[] orderers);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task LargeVersionedRelationMatchesOracleAcrossBatchesAndReplay(bool useNoEmit, bool inBatch)
    {
        var saved = (IFieldHandler.UseNoEmit, IFieldHandler.UseNoEmitForKeyValue,
            IFieldHandler.UseNoEmitForRelations, IFieldHandler.UseNoEmitForDescriptors);
        try
        {
            IFieldHandler.UseNoEmit = useNoEmit;
            IFieldHandler.UseNoEmitForKeyValue = useNoEmit;
            IFieldHandler.UseNoEmitForRelations = useNoEmit;
            IFieldHandler.UseNoEmitForDescriptors = useNoEmit;
            ObjectDB.ResetAllMetadataCaches();
            await RunWorkload(inBatch);
        }
        finally
        {
            (IFieldHandler.UseNoEmit, IFieldHandler.UseNoEmitForKeyValue,
                IFieldHandler.UseNoEmitForRelations, IFieldHandler.UseNoEmitForDescriptors) = saved;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    async Task RunWorkload(bool inBatch)
    {
        const int companies = 64, pipelinesPerCompany = 256;
        ulong[] versions = [1, 2, 63, 64, 127, 128, 255, 256, 16383, 16384, 65535, 65536,
            uint.MaxValue - 1UL, uint.MaxValue, 1UL << 32, (1UL << 32) + 1];
        var expected = new List<Pipeline>[companies * pipelinesPerCompany];
        var random = new Random(318804);
        var timer = Stopwatch.StartNew();
        using var files = new InMemoryFileCollection();
        using (var kv = Open(files))
        using (var db = new ObjectDB())
        {
            db.Open(kv, false);
            for (var company = 0; company < companies; company++)
            {
                using var tr = await db.StartWritingTransaction(inBatch: inBatch);
                var table = tr.GetRelation<IPipelineTable>();
                // Interleave versions and pipelines so insertion order differs from every index order.
                foreach (var version in versions.Reverse())
                for (var pipeline = 0; pipeline < pipelinesPerCompany; pipeline++)
                {
                    var index = company * pipelinesPerCompany + pipeline;
                    expected[index] ??= [];
                    var row = MakeRow(index, version, (int)((version + (ulong)index) % 3), 0);
                    expected[index].Add(row);
                    table.Upsert(row);
                }
                tr.SetCommitUlong((ulong)company + 1);
                tr.Commit();
                if (company % 8 == 7) db.FinishTransactionBatchAfterCurrentTransaction();
            }
            using var original = db.StartReadOnlyTransaction();
            Assert.True(original.KeyValueDBTransaction.GetKeyValueCount() > 1_000_000);
            var originalTable = original.GetRelation<IPipelineTable>();
            var originalExpected = expected.Select(rows => rows.ToArray()).ToArray();
            CheckAll(originalTable, expected);
            output.WriteLine($"Seeded {companies * pipelinesPerCompany * versions.Length:N0} rows / " +
                             $"{original.KeyValueDBTransaction.GetKeyValueCount():N0} keys in {timer.Elapsed}.");

            for (var batch = 0; batch < 8; batch++)
            {
                var pending = new Dictionary<int, List<Pipeline>>();
                for (var operation = 0; operation < 128; operation++)
                {
                    var index = random.Next(expected.Length);
                    if (!pending.TryGetValue(index, out var rows))
                        rows = new List<Pipeline>(expected[index]);
                    var changed = new List<Pipeline>(rows);
                    using (var writer = await db.StartWritingTransaction(inBatch: inBatch))
                    {
                        var table = writer.GetRelation<IPipelineTable>();
                        var latest = changed.MaxBy(row => row.VersionNumber)!;
                        switch (operation % 3)
                        {
                            case 0:
                                var added = MakeRow(index, latest.VersionNumber + 1, batch % 3, batch + 1);
                                table.Upsert(added);
                                changed.Add(added);
                                break;
                            case 1:
                                var previous = changed[random.Next(changed.Count)];
                                var updated = MakeRow(index, previous.VersionNumber,
                                    ((int)previous.VersionState + 1) % 3, batch + 1);
                                table.Upsert(updated);
                                changed[changed.IndexOf(previous)] = updated;
                                break;
                            default:
                                Assert.True(table.RemoveById(latest.CompanyId, latest.Id, latest.VersionNumber));
                                changed.Remove(latest);
                                break;
                        }
                        if (operation % 16 == 0)
                        {
                            using var during = db.StartReadOnlyTransaction();
                            Check(during.GetRelation<IPipelineTable>(), expected[index], index);
                        }
                        if (operation % 7 != 0)
                        {
                            writer.SetCommitUlong((ulong)(companies + batch * 128 + operation + 1));
                            writer.Commit();
                            pending[index] = changed;
                            if (!inBatch) expected[index] = changed;
                        }
                        // Otherwise dispose rolls back insert/update/delete inside the batch.
                    }
                    if (operation % 16 == 0)
                    {
                        using var between = db.StartReadOnlyTransaction();
                        Check(between.GetRelation<IPipelineTable>(), expected[index], index);
                    }
                }
                db.FinishTransactionBatchAfterCurrentTransaction();
                foreach (var (index, rows) in pending) expected[index] = rows;
                using var published = db.StartReadOnlyTransaction();
                foreach (var index in pending.Keys)
                {
                    Check(published.GetRelation<IPipelineTable>(), expected[index], index);
                    Check(originalTable, originalExpected[index], index);
                }
            }
            using var final = db.StartReadOnlyTransaction();
            output.WriteLine($"Before reopen: {final.KeyValueDBTransaction.GetKeyValueCount()} keys, {files.Enumerate().Count()} files.");
            CheckAll(final.GetRelation<IPipelineTable>(), expected);
        }
        // Rebuild the BTree from TRL, rather than reusing the writer's in-memory tree.
        using (var reopenedKv = Open(files))
        using (var reopened = new ObjectDB())
        {
            reopened.Open(reopenedKv, false);
            using var tr = reopened.StartReadOnlyTransaction();
            output.WriteLine($"After reopen: {tr.KeyValueDBTransaction.GetKeyValueCount()} keys.");
            CheckAll(tr.GetRelation<IPipelineTable>(), expected);
        }
        output.WriteLine($"Workload and TRL replay completed in {timer.Elapsed}.");
    }

    static BTreeKeyValueDB Open(IFileCollection files) => new(new KeyValueDBOptions
    {
        FileCollection = files, CompactorScheduler = null, FileSplitSize = 16 * 1024 * 1024
    });

    static Pipeline MakeRow(int index, ulong version, int state, int revision) => new()
    {
        CompanyId = 725140944 + (ulong)(index / 256),
        Id = 725145400 + (ulong)(index % 256),
        VersionNumber = version,
        VersionState = (PipelineState)state,
        LastUpdate = new DateTime(2026, 9, 18, 9, 3, 6, DateTimeKind.Utc).AddTicks(index + revision * 100000L),
        SourceChangeSet = (index + revision) % 3 == 0 ? null : new Guid(index, (short)revision, 0, new byte[8]),
        Content = $"pipeline {index}, version {version}, revision {revision}"
    };

    static void CheckAll(IPipelineTable table, List<Pipeline>[] expected)
    {
        for (var index = 0; index < expected.Length; index++) Check(table, expected[index], index);
        Assert.Null(table.FirstByStateVersionOrDefault(Constraint.Unsigned.Exact(725140976),
            Constraint.Unsigned.Exact(ulong.MaxValue), Constraint.Enum<PipelineState>.Any,
            [Orderer.Descending((Pipeline p) => p.VersionNumber)]));
    }

    static void Check(IPipelineTable table, IReadOnlyCollection<Pipeline> rows, int index)
    {
        foreach (var state in new PipelineState?[] { null, PipelineState.Draft, PipelineState.Production,
                     PipelineState.Invalidated })
        {
            var wanted = rows.Where(row => state == null || row.VersionState == state)
                .MaxBy(row => row.VersionNumber);
            var actual = table.FirstByStateVersionOrDefault(
                Constraint.Unsigned.Exact(725140944 + (ulong)(index / 256)),
                Constraint.Unsigned.Exact(725145400 + (ulong)(index % 256)),
                state.HasValue ? Constraint.Enum<PipelineState>.Exact(state.Value) : Constraint.Enum<PipelineState>.Any,
                [Orderer.Descending((Pipeline p) => p.VersionNumber)]);
            if (wanted == null) { Assert.Null(actual); continue; }
            Assert.True(actual != null, $"Missing pipeline {index}, state {state}, expected version {wanted.VersionNumber}");
            Assert.Equal(wanted.CompanyId, actual.CompanyId);
            Assert.Equal(wanted.Id, actual.Id);
            Assert.Equal(wanted.VersionNumber, actual.VersionNumber);
            Assert.Equal(wanted.VersionState, actual.VersionState);
            Assert.Equal(wanted.LastUpdate, actual.LastUpdate);
            Assert.Equal(wanted.SourceChangeSet, actual.SourceChangeSet);
            Assert.Equal(wanted.Content, actual.Content);
        }
    }
}
