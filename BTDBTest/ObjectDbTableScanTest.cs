using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest
{
    public class ObjectDbTableScanTest : ObjectDbTestBase
    {
        public ObjectDbTableScanTest(ITestOutputHelper output) : base(output)
        {
        }

        public class Person
        {
            [PrimaryKey(1)] public ulong TenantId { get; set; }

            [PrimaryKey(2)] public string? Email { get; set; }

            public string? Name { get; set; }
        }

        public interface IPersonTable : IRelation<Person>
        {
            IEnumerable<Person> ScanById(Constraint<ulong> tenantId, Constraint<string> email);
        }

        [Fact]
        public void ScanByIdWorks()
        {
            using (var tr = _db.StartTransaction())
            {
                var t = tr.GetRelation<IPersonTable>();
                t.Upsert(new() { TenantId = 1, Email = "a@b.cd", Name = "a" });
                t.Upsert(new() { TenantId = 1, Email = "b@b.cd", Name = "b" });
                t.Upsert(new() { TenantId = 2, Email = "a@c.cd", Name = "a2" });
                t.Upsert(new() { TenantId = 2, Email = "b@c.cd", Name = "b2" });
                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                var t = tr.GetRelation<IPersonTable>();
                var p = t.ScanById(Constraint.Unsigned.Exact(1), Constraint.String.StartsWith("a")).Single();
                Assert.Equal("a", p.Name);
            }
        }
    }
}
