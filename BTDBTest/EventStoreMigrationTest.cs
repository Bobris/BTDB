using System;
using System.Collections.Generic;
using BTDB.Encrypted;
using BTDB.EventStore2Layer;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using Xunit;

namespace BTDBTest
{
    public class EventStoreMigrationTest
    {
        public class Item
        {
            public string Field { get; set; }
        }

        public class EventRoot
        {
            public List<Item> Items { get; set; }
        }

        [Fact]
        public void CanMigrateList()
        {
            var obj = PassThroughEventStorage(new EventRoot
            {
                Items = new List<Item> {new Item {Field = "A"}}
            }, new FullNameTypeMapper());
            var serializer = new EventSerializer();
            bool hasMetadata;
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
            deserializer.ProcessMetadataLog(meta);
            Assert.True(deserializer.Deserialize(out obj2, data));
        }

        static object PassThroughEventStorage(object @event, ITypeNameMapper mapper, bool ignoreIndirect = true)
        {
            var options = TypeSerializersOptions.Default;
            options.IgnoreIIndirect = ignoreIndirect;
            options.SymmetricCipher = new AesGcmSymmetricCipher(new byte[]
            {
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
                28, 29, 30, 31
            });
            var manager = new EventStoreManager(options);
            var storage = new MemoryEventFileStorage();
            var appender = manager.AppendToStore(storage);
            var events = new[]
            {
                @event
            };
            appender.Store(null, events);
            manager = new EventStoreManager(options);
            manager.SetNewTypeNameMapper(mapper);
            var eventObserver = new EventStoreTest.StoringEventObserver();
            manager.OpenReadOnlyStore(storage).ReadFromStartToEnd(eventObserver);
            return eventObserver.Events[0][0];
        }

        public class Item2
        {
            public string Field { get; set; }
            public string Field2 { get; set; }
        }

        public class EventRoot2
        {
            public List<Item2> Items { get; set; }
        }

        [Fact]
        public void CanMigrateListWithNewFields()
        {
            var parentMapper = new FullNameTypeMapper();
            var mapper = new EventStoreTest.OverloadableTypeMapper(typeof(Item2), parentMapper.ToName(typeof(Item)),
                new EventStoreTest.OverloadableTypeMapper(typeof(EventRoot2), parentMapper.ToName(typeof(EventRoot)),
                    parentMapper
                ));
            var obj = PassThroughEventStorage(new EventRoot
            {
                Items = new List<Item> {new Item {Field = "A"}}
            }, mapper);
            var serializer = new EventSerializer(mapper);
            bool hasMetadata;
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer(mapper);
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
            deserializer.ProcessMetadataLog(meta);
            Assert.True(deserializer.Deserialize(out obj2, data));
        }

        public class EventDictListRoot
        {
            public IDictionary<ulong, IList<Item>> Items { get; set; }
        }

        [Fact]
        public void CanMigrateListInDict()
        {
            var obj = PassThroughEventStorage(new EventDictListRoot
            {
                Items = new Dictionary<ulong, IList<Item>> {{1, new List<Item> {new Item {Field = "A"}}}}
            }, new FullNameTypeMapper());
            var serializer = new EventSerializer();
            bool hasMetadata;
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
            deserializer.ProcessMetadataLog(meta);
            Assert.True(deserializer.Deserialize(out obj2, data));
        }

        public enum ItemEn
        {
            One = 1
        }

        public class EventRootEn
        {
            public List<ItemEn> Items { get; set; }
        }

        public enum ItemEn2
        {
            One = 1,
            Two = 2
        }

        public class EventRootEn2
        {
            public List<ItemEn2> Items { get; set; }
        }

        [Fact]
        public void CanMigrateListWithChangedEnum()
        {
            var parentMapper = new FullNameTypeMapper();
            var mapper = new EventStoreTest.OverloadableTypeMapper(typeof(ItemEn2), parentMapper.ToName(typeof(ItemEn)),
                new EventStoreTest.OverloadableTypeMapper(typeof(EventRootEn2),
                    parentMapper.ToName(typeof(EventRootEn)),
                    parentMapper
                ));
            var obj = PassThroughEventStorage(new EventRootEn
            {
                Items = new List<ItemEn> {ItemEn.One}
            }, mapper);
            var serializer = new EventSerializer(mapper);
            bool hasMetadata;
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer(mapper);
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
            deserializer.ProcessMetadataLog(meta);
            Assert.True(deserializer.Deserialize(out obj2, data));
        }

        public class EventWithInt
        {
            public int A { get; set; }
        }

        public class EventWithUlong
        {
            public ulong A { get; set; }
        }

        [Fact]
        public void CanMigrateIntToUlong()
        {
            var parentMapper = new FullNameTypeMapper();
            var mapper = new EventStoreTest.OverloadableTypeMapper(typeof(EventWithUlong),
                parentMapper.ToName(typeof(EventWithInt)),
                parentMapper
            );
            var obj = (EventWithUlong) PassThroughEventStorage(new EventWithInt
            {
                A = 42
            }, mapper);
            Assert.Equal(42ul, obj.A);
            var obj2 = (EventWithUlong) PassThroughEventStorage(new EventWithInt
            {
                A = -1
            }, mapper);
            Assert.Equal(0xffffffff, obj2.A);
        }

        public class EventWithString
        {
            public string A { get; set; }
        }

        public class EventWithVersion
        {
            public Version A { get; set; }
        }

        [Fact]
        public void CanMigrateStringToVersion()
        {
            var parentMapper = new FullNameTypeMapper();
            var mapper = new EventStoreTest.OverloadableTypeMapper(typeof(EventWithVersion),
                parentMapper.ToName(typeof(EventWithString)),
                parentMapper
            );
            var obj = (EventWithVersion) PassThroughEventStorage(new EventWithString
            {
                A = "1.2.3"
            }, mapper);
            Assert.Equal(new Version(1, 2, 3), obj.A);
            var obj2 = (EventWithVersion) PassThroughEventStorage(new EventWithString
            {
                A = null
            }, mapper);
            Assert.Null(obj2.A);
        }

        public class EventWithEncryptedString
        {
            public EncryptedString A { get; set; }
        }

        [Fact]
        public void CanMigrateStringToEncryptedString()
        {
            var parentMapper = new FullNameTypeMapper();
            var mapper = new EventStoreTest.OverloadableTypeMapper(typeof(EventWithEncryptedString),
                parentMapper.ToName(typeof(EventWithString)),
                parentMapper
            );
            var obj = (EventWithEncryptedString) PassThroughEventStorage(new EventWithString
            {
                A = "pass"
            }, mapper);
            Assert.Equal("pass", obj.A);
            var obj2 = (EventWithEncryptedString) PassThroughEventStorage(new EventWithString
            {
                A = null
            }, mapper);
            Assert.Null(obj2.A.Secret);
        }


        public abstract class ItemBase
        {
            public int A { get; set; }
        }

        public class ItemBase1 : ItemBase
        {
            public int B { get; set; }
        }

        public class EventDictIndirectAbstract
        {
            public IDictionary<ulong, IIndirect<ItemBase>> Items { get; set; }
        }

        public class EventDictAbstract
        {
            public IDictionary<ulong, ItemBase> Items { get; set; }
        }

        [Fact]
        public void CanMigrateDictionaryValueOutOfIndirect()
        {
            var parentMapper = new FullNameTypeMapper();
            var mapper = new EventStoreTest.OverloadableTypeMapper(typeof(EventDictAbstract),
                parentMapper.ToName(typeof(EventDictIndirectAbstract)),
                parentMapper
            );
            var obj = PassThroughEventStorage(new EventDictIndirectAbstract
            {
                Items = new Dictionary<ulong, IIndirect<ItemBase>>
                    {{1, new DBIndirect<ItemBase>(new ItemBase1 {A = 1, B = 2})}}
            }, mapper, false);
            Assert.IsType<EventDictAbstract>(obj);
            var res = (EventDictAbstract) obj;
            Assert.Equal(1, res.Items[1].A);
            Assert.Equal(2, ((ItemBase1) res.Items[1]).B);
        }
    }
}
