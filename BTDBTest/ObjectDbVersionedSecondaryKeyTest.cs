using System;
using System.Threading.Tasks;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbVersionedSecondaryKeyTest(ITestOutputHelper output) : ObjectDbTestBase(output)
{
    public enum VersionState
    {
        Draft,
        Production,
        Invalidated
    }

    public class VersionedItem
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }

        [PrimaryKey(3)]
        [SecondaryKey("StateVersion", Order = 4)]
        public ulong VersionNumber { get; set; }

        [SecondaryKey("StateVersion", Order = 3, IncludePrimaryKeyOrder = 2)]
        public VersionState State { get; set; }

        public string Content { get; set; } = "";

        [SecondaryKey("Date", IncludePrimaryKeyOrder = 3)]
        public DateTime LastUpdate { get; set; }

        [SecondaryKey("SourceChangeSet", IncludePrimaryKeyOrder = 1)]
        public Guid? SourceChangeSet { get; set; }
    }

    public interface IVersionedItemTable : IRelation<VersionedItem>
    {
        void UpdateById(ulong companyId, ulong id, ulong versionNumber, VersionState state);
        VersionedItem? FirstByStateVersionOrDefault(Constraint<ulong> companyId, Constraint<ulong> id,
            Constraint<VersionState> state, IOrderer[] orderers);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OrderedSecondaryKeyReadsKeepPublishedSnapshotUntilBatchFinishes(bool activeWriter)
    {
        using (var seed = await _db.StartWritingTransaction())
        {
            seed.GetRelation<IVersionedItemTable>().Upsert(new()
            {
                CompanyId = 1234567890, Id = 42, VersionNumber = 1,
                State = VersionState.Production, Content = "Published"
            });
            seed.Commit();
        }
        using (var batch = await _db.StartWritingTransaction(inBatch: true))
        {
            batch.GetRelation<IVersionedItemTable>().Upsert(new()
            {
                CompanyId = 1234567890, Id = 42, VersionNumber = 2,
                State = VersionState.Draft, Content = "Pending"
            });
            batch.Commit();
        }
        using var current = activeWriter ? await _db.StartWritingTransaction(inBatch: true) : null;
        current?.GetRelation<IVersionedItemTable>().UpdateById(1234567890, 42, 2, VersionState.Production);
        using var during = _db.StartReadOnlyTransaction();
        var snapshot = during.GetRelation<IVersionedItemTable>();
        AssertPublishedVersion(snapshot, 1, "Published");
        current?.Commit();
        _db.FinishTransactionBatchAfterCurrentTransaction();
        using var after = _db.StartReadOnlyTransaction();
        AssertPublishedVersion(after.GetRelation<IVersionedItemTable>(), 2, "Pending");
        AssertPublishedVersion(snapshot, 1, "Published");
    }

    static void AssertPublishedVersion(IVersionedItemTable table, ulong version, string content)
    {
        var result = table.FirstByStateVersionOrDefault(Constraint.Unsigned.Exact(1234567890),
            Constraint.Unsigned.Exact(42), Constraint.Enum<VersionState>.Any,
            [Orderer.Descending((VersionedItem item) => item.VersionNumber)]);
        Assert.NotNull(result);
        Assert.Equal(version, result.VersionNumber);
        Assert.Equal(content, result.Content);
    }
}
