using BTDB.ODBLayer;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest
{
    public class MultipleEnumerationTest : ObjectDbTestBase
    {
        public MultipleEnumerationTest(ITestOutputHelper output) : base(output)
        {
        }

        public class Application
        {
            [PrimaryKey(1)]
            public ulong CompanyId { get; set; }
            [PrimaryKey(2)]
            public ulong ApplicationId { get; set; }
        }

        public interface IApplicationTable : IRelation<Application>
        {
            IEnumerable<Application> ListById(AdvancedEnumeratorParam<ulong> param);
            IEnumerable<Application> FindById(ulong companyId);
        }

        [Fact]
        public void CanEnumerateAdvancedEnumeratorMultipleTimes()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IApplicationTable>("ApplicationTable");
            var table = creator(tr);
            var application1 = new Application { CompanyId = 1, ApplicationId = 100 };
            var application2 = new Application { CompanyId = 1, ApplicationId = 101 };
            var application3 = new Application { CompanyId = 2, ApplicationId = 102 };

            table.Upsert(application2);
            table.Upsert(application1);
            table.Upsert(application3);

            var en = table.ListById(new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending,
                1, KeyProposition.Included, 2, KeyProposition.Included));

            Assert.Equal(3, en.Count());
            Assert.Equal(3, en.Count());
            Assert.Equal(en.Select(a => a.ApplicationId), new ulong[] { 100, 101, 102 });

            tr.Commit();
        }

        [Fact]
        public void CanEnumerateEnumeratorMultipleTimes()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IApplicationTable>("ApplicationTable");
            var table = creator(tr);
            table.Upsert(new Application { CompanyId = 1, ApplicationId = 100 });
            table.Upsert(new Application { CompanyId = 1, ApplicationId = 101 });
            table.Upsert(new Application { CompanyId = 2, ApplicationId = 102 });

            var en = table.FindById(1);

            Assert.Equal(2, en.Count());
            Assert.Equal(2, en.Count());
            Assert.Equal(en.Select(a => a.ApplicationId), new ulong[] { 100, 101 });
            tr.Commit();
        }

        [Fact]
        public void CanEnumerateRelationAdvancedEnumeratorAfterMoveNext()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IApplicationTable>("ApplicationTable");
            var table = creator(tr);
            var application1 = new Application { CompanyId = 1, ApplicationId = 100 };
            var application2 = new Application { CompanyId = 1, ApplicationId = 101 };
            var application3 = new Application { CompanyId = 2, ApplicationId = 102 };

            table.Upsert(application2);
            table.Upsert(application1);
            table.Upsert(application3);

            var result = table.ListById(new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending,
                1, KeyProposition.Included, 2, KeyProposition.Included));

            var resultEnumerator = result.GetEnumerator();
            Assert.True(resultEnumerator.MoveNext());

            var val1 = resultEnumerator.Current;
            Assert.Equal(100U, val1.ApplicationId);

            var values = result.ToList();
            Assert.Equal(3, values.Count());

            tr.Commit();
        }
    }
}