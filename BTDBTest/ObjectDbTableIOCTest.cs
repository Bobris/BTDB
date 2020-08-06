using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BTDB.FieldHandler;
using BTDB.IOC;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDBTest.IOCDomain;
using Xunit;

namespace BTDBTest
{
    public class ObjectDbTableIOCTest : IDisposable
    {
        IKeyValueDB _lowDb;
        IObjectDB _db;
        IContainer _container;

        public ObjectDbTableIOCTest()
        {
            _lowDb = new InMemoryKeyValueDB();
            var builder = new ContainerBuilder();
            builder.RegisterInstance<string>("Hello").Named<string>("param");
            builder.RegisterType<Item>().AsSelf();
            _container = builder.Build();
            OpenDb();
        }

        void OpenDb()
        {
            _db = new ObjectDB();
            _db.Open(_lowDb, false, new DBOptions().WithoutAutoRegistration().WithContainer(_container));
        }

        void ReopenDb()
        {
            _db.Dispose();
            OpenDb();
        }

        public class Item
        {
            public Item(string param)
            {
                Param = param;
            }

            [PrimaryKey] public ulong Id { get; set; }
            [NotStored] public string Param { get; }
        }

        public interface IItems : IRelation<Item>
        {
        }

        [Fact]
        public void ItemCreatedByIocWorks()
        {
            using var tr = _db.StartTransaction();
            var items = tr.GetRelation<IItems>();
            items.Upsert(_container.Resolve<Item>());
            Assert.Equal("Hello", items.First().Param);
        }

        [Fact]
        public void SingletonCreatedByIocWorks()
        {
            using var tr = _db.StartTransaction();
            var item = tr.Singleton<Item>();
            Assert.Equal("Hello", item.Param);
        }

        [Fact]
        public void NewObjectCreatedByIocWorks()
        {
            using var tr = _db.StartTransaction();
            _db.RegisterType(typeof(Item));
            var item = tr.New<Item>();
            Assert.Equal("Hello", item.Param);
        }

        public void Dispose()
        {
            _db.Dispose();
            _lowDb.Dispose();
        }
    }
}
