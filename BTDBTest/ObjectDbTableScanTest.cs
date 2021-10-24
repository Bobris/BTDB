using System;
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
        [InlineData("a", "AC")]
        [InlineData("b", "BD")]
        [InlineData("", "ABCD")]
        [InlineData("c", "")]
        public void ConstraintUnsignedAnyStringPrefixWorks(string prefix, string expectedNames)
        {
            FillPersonData();

            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IPersonTable>();
            var names = string.Concat(t.ScanById(Constraint.Unsigned.Any, Constraint.String.StartsWith(prefix))
                .Select(p => p.Name));
            Assert.Equal(expectedNames, names);
        }

        [Theory]
        [InlineData("a", "AC")]
        [InlineData("b", "ABD")]
        [InlineData("", "ABCD")]
        [InlineData(".c", "ABCD")]
        [InlineData("a@b.cd", "A")]
        [InlineData("a@c.cd", "C")]
        [InlineData("a@c.cd2", "")]
        public void ConstraintUnsignedAnyStringContainsWorks(string contain, string expectedNames)
        {
            FillPersonData();

            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IPersonTable>();
            var names = string.Concat(t.ScanById(Constraint.Unsigned.Any, Constraint.String.Contains(contain))
                .Select(p => p.Name));
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

        public class ThreeUlongs
        {
            [PrimaryKey(1)] public ulong N1 { get; set; }
            [PrimaryKey(2)] public ulong N2 { get; set; }
            [PrimaryKey(3)] public ulong N3 { get; set; }

            public ulong Id { get; set; }
        }

        public interface IThreeUlongsTable : IRelation<ThreeUlongs>
        {
            IEnumerable<ThreeUlongs> ScanById(Constraint<ulong> n1, Constraint<ulong> n2);
            IEnumerable<ThreeUlongs> ScanById(Constraint<ulong> n1, Constraint<ulong> n2, Constraint<ulong> n3);
        }

        [Fact]
        public void ConstraintUnsignedAnyUnsignedPredicateWorks()
        {
            FillThreeUlongsData();

            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IThreeUlongsTable>();
            AssertSameCondition(t.Where(v => v.N2 > 3),
                t.ScanById(Constraint.Unsigned.Any, Constraint.Unsigned.Predicate(n => n > 3)));
        }

        static void AssertSameCondition(IEnumerable<ThreeUlongs> expectedResult, IEnumerable<ThreeUlongs> scanResult)
        {
            var expected = string.Join(',', expectedResult.Select(v => v.Id));
            var test = string.Join(',', scanResult.Select(v => v.Id));
            Assert.Equal(expected, test);
        }

        void FillThreeUlongsData()
        {
            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IThreeUlongsTable>();
            for (var i = 1; i <= 5; i++)
            {
                for (var j = 1; j <= 5; j++)
                {
                    for (var k = 1; k <= 5; k++)
                    {
                        t.Upsert(new()
                            { N1 = (ulong)i, N2 = (ulong)j, N3 = (ulong)k, Id = (ulong)(i * 100 + j * 10 + k) });
                    }
                }
            }

            tr.Commit();
        }
    }
}
