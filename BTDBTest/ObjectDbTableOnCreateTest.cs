using System;
using BTDB.IOC;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ObjectDbTableOnCreateTest : IDisposable
{
    readonly IKeyValueDB _lowDb;
    IObjectDB _db;
    IContainer? _container;

    public ObjectDbTableOnCreateTest()
    {
        _lowDb = new InMemoryKeyValueDB();
        OpenDb();
    }

    public void Dispose()
    {
        _db.Dispose();
        _lowDb.Dispose();
    }

    void ReopenDb()
    {
        _db.Dispose();
        OpenDb();
    }

    void OpenDb()
    {
        _db = new ObjectDB();
        _db.Open(_lowDb, false,
            new DBOptions().WithoutAutoRegistration().WithContainer(_container!));
    }

    public class JobV1
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public string? Name { get; set; }
    }

    public interface IJobTable1 : IRelation<JobV1>
    {
        void Insert(JobV1 job);
    }

    public class JobV2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        [SecondaryKey("Name")] public string Name { get; set; }

        [PrimaryKey(2)] public uint Cost { get; set; } = 1;
    }

    public class JobTable2OnCreate : IRelationOnCreate<IJobTable2>
    {
        public void OnCreate(IObjectDBTransaction transaction, IJobTable2 creating)
        {
            var from = transaction.GetRelation<IJobTable1>();
            creating.UpsertRange(from.As<JobV2>());
            from.RemoveAll();
        }
    }

    public interface IJobTable2 : IRelation<JobV2>
    {
        JobV2 FindByNameOrDefault(string name);
    }

    [Fact]
    public void UpgradeJobV1ToJobV2UsingOnCreate()
    {
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IJobTable1>();
            table.Insert(new() { Id = 11, Name = "Code" });
            table.Insert(new() { Id = 42, Name = "1337" });
            tr.Commit();
        }

        var builder = new ContainerBuilder(ContainerBuilderBehaviour.UniqueRegistrations);
        builder.RegisterType<JobTable2OnCreate>().As<IRelationOnCreate<IJobTable2>>();
        _container = builder.BuildAndVerify();
        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IJobTable2>();
            Assert.Equal(2, table.Count);
            Assert.Equal(42ul, table.FindByNameOrDefault("1337").Id);
            Assert.Equal(1u, table.FindByNameOrDefault("Code").Cost);
        }
    }
}
