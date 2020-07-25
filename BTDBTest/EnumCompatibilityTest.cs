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

        [Fact]
        public void EnumIsBinaryCompatible()
        {
            const ulong testId = 1ul;
            var odb = CreateObjectDB();
            var tr = odb.StartWritingTransaction().Result;
            var itemRelation = tr.GetRelation<IItemTable>();
            itemRelation.Insert(new Item { Id = testId, Type = ItemType.B });
            tr.Commit();
            tr.Dispose();
            odb = CreateObjectDB();
            tr = odb.StartReadOnlyTransaction();
            var flagRelation = tr.GetRelation<IFlagTable>();
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

        [PersistedName("Test")]
        public interface IItemTable : IRelation<Item>
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

        [PersistedName("Test")]
        public interface IFlagTable : IRelation<Flag>
        {
            void Insert(Flag item);
            Flag FindById(ulong id);
        }
    }
}
