using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using System;
using Xunit;

namespace BTDBTest
{
    public class EnumCompatibilityTest : IDisposable
    {
        readonly IKeyValueDB _kvdb;

        public EnumCompatibilityTest()
        {
            _kvdb = new InMemoryKeyValueDB();
        }

        IObjectDB CreateObjectDB()
        {
            var odb = new ObjectDB();
            odb.Open(_kvdb, false);
            return odb;
        }

        static Func<IObjectDBTransaction, T> GetRelationCreator<T>(IObjectDB odb) where T: IRelation
        {
            using var tr = odb.StartTransaction();
            var creator = tr.InitRelation<T>("TestRelation");
            tr.Commit();
            return creator;
        }

        [Fact]
        public void EnumIsBinaryCompatible()
        {
            const ulong testId = 1ul;
            var odb = CreateObjectDB();
            var itemCreator = GetRelationCreator<IItemTable>(odb);
            var tr = odb.StartTransaction();
            var itemRelation = itemCreator(tr);
            itemRelation.Insert(new Item { Id = testId, Type = ItemType.B });
            tr.Commit();
            tr.Dispose();
            odb = CreateObjectDB();
            var flagCreator = GetRelationCreator<IFlagTable>(odb);
            tr = odb.StartReadOnlyTransaction();
            var flagRelation = flagCreator(tr);
            var flag = flagRelation.FindById(testId);
            tr.Dispose();
            Assert.Equal(FlagType.B, flag.Type);
        }

        public void Dispose()
        {
            _kvdb.Dispose();
        }

        public enum ItemType
        {
            Undefined = 0,
            A = 1,
            B = 2
        }

        public class Item
        {
            [PrimaryKey]
            public ulong Id { get; set; }
            [SecondaryKey("Type")]
            public ItemType Type { get; set; }
        }

        public interface IItemTable: IRelation<Item>
        {
            void Insert(Item item);
            Item FindById(ulong id);
        }

        [BinaryCompatibilityOnly]
        [Flags]
        public enum FlagType
        {
            Undefined = 0,
            A = 1,
            B = 2
        }

        public class Flag
        {
            [PrimaryKey]
            public ulong Id { get; set; }
            [SecondaryKey("Type")]
            public FlagType Type { get; set; }
        }

        public interface IFlagTable: IRelation<Flag>
        {
            void Insert(Flag item);
            Flag FindById(ulong id);
        }
    }
}
