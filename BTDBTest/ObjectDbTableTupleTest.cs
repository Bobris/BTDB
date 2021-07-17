using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest
{
    public class ObjectDbTableTupleTest : ObjectDbTestBase
    {
        public ObjectDbTableTupleTest(ITestOutputHelper output) : base(output)
        {
        }

        public class Obj
        {
            [PrimaryKey(1)] public ulong Id { get; set; }

            public Tuple<int, uint> Val {
                get;
                set;
            }
        }

        public interface IObjTable : IRelation<Obj>
        {
            void Insert(Obj obj);
        }

        [Fact]
        public void BasicTupleInValueWorks()
        {
            using (var tr = _db.StartTransaction())
            {
                tr.GetRelation<IObjTable>().Insert(new() { Id=1, Val = new(2,3)});
                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                Assert.Equal(new Tuple<int, uint>(2, 3), tr.GetRelation<IObjTable>().First().Val);
            }
        }
    }
}
