using System.Linq;
using BTDB;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbSingletonCacheTest : ObjectDbTestBase
{
    public ObjectDbSingletonCacheTest(ITestOutputHelper output) : base(output)
    {
    }

    [Generate]
    public class Root
    {
        public int Value { get; set; }
    }

    // Before the fix storing the singleton, got otherwise than by Singleton<T>() before the database knew its oid, did
    // not invalidate the cached singleton content, so the old content a reader cached meanwhile was served after the
    // commit.
    [Fact]
    public void StoringSingletonGotByEnumerationInvalidatesCachedContent()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.Singleton<Root>().Value = 1;
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(Root));
        using (var writer = _db.StartTransaction())
        {
            var root = writer.Enumerate<Root>().Single();
            root.Value = 2;
            writer.StoreAndFlush(root);
            using (var reader = _db.StartReadOnlyTransaction())
                Assert.Equal(1, reader.Singleton<Root>().Value);
            writer.Commit();
        }

        using var after = _db.StartReadOnlyTransaction();
        Assert.Equal(2, after.Singleton<Root>().Value);
    }

    // Before the fix deleting the singleton by its oid, or all data, kept its content cached, so the deleted singleton
    // was served afterwards instead of a new one.
    [Fact]
    public void DeletingSingletonByOidInvalidatesCachedContent()
    {
        ulong oid;
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<Root>();
            root.Value = 1;
            tr.Commit();
            oid = tr.GetOid(root);
        }

        using (var reader = _db.StartReadOnlyTransaction())
            Assert.Equal(1, reader.Singleton<Root>().Value);
        using (var writer = _db.StartTransaction())
        {
            writer.Delete(oid);
            writer.Commit();
        }

        using var after = _db.StartReadOnlyTransaction();
        Assert.Equal(0, after.Singleton<Root>().Value);
    }

    [Fact]
    public void DeletingAllDataInvalidatesCachedSingletonContent()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.Singleton<Root>().Value = 1;
            tr.Commit();
        }

        using (var reader = _db.StartReadOnlyTransaction())
            Assert.Equal(1, reader.Singleton<Root>().Value);
        using (var writer = _db.StartTransaction())
        {
            writer.DeleteAllData();
            writer.Commit();
        }

        using var after = _db.StartReadOnlyTransaction();
        Assert.Equal(0, after.Singleton<Root>().Value);
    }
}
