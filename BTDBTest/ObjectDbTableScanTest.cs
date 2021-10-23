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
            FillPersonData();

            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IPersonTable>();
            var p = t.ScanById(Constraint.Unsigned.Exact(1), Constraint.String.StartsWith("a")).Single();
            Assert.Equal("A", p.Name);
        }

        [Theory]
        [InlineData("a","AC")]
        [InlineData("b","BD")]
        [InlineData("","ABCD")]
        [InlineData("c","")]
        public void ConstraintUnsignedAnyStringPrefixWorks(string prefix, string expectedNames)
        {
            FillPersonData();

            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IPersonTable>();
            var names = string.Concat(t.ScanById(Constraint.Unsigned.Any, Constraint.String.StartsWith(prefix)).Select(p=>p.Name));
            Assert.Equal(expectedNames, names);
        }

        [Theory]
        [InlineData("a","AC")]
        [InlineData("b","ABD")]
        [InlineData("","ABCD")]
        [InlineData(".c","ABCD")]
        [InlineData("a@b.cd","A")]
        [InlineData("a@c.cd","C")]
        [InlineData("a@c.cd2","")]
        public void ConstraintUnsignedAnyStringContainsWorks(string contain, string expectedNames)
        {
            FillPersonData();

            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IPersonTable>();
            var names = string.Concat(t.ScanById(Constraint.Unsigned.Any, Constraint.String.Contains(contain)).Select(p=>p.Name));
            Assert.Equal(expectedNames, names);
        }

        void FillPersonData()
        {
            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IPersonTable>();
            t.Upsert(new() { TenantId = 1, Email = "a@b.cd", Name = "A" });
            t.Upsert(new() { TenantId = 1, Email = "b@b.cd", Name = "B" });
            t.Upsert(new() { TenantId = 2, Email = "a@c.cd", Name = "C" });
            t.Upsert(new() { TenantId = 2, Email = "b@c.cd", Name = "D" });
            tr.Commit();
        }
    }
}
