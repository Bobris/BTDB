using BTDB.EventStoreLayer;
using BTDB.StreamLayer;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class TypeSerializersTest
    {
        ITypeSerializers _ts;
        ITypeSerializersMapping _mapping;

        [SetUp]
        public void Setup()
        {
            _ts = new TypeSerializers();
            _mapping = _ts.CreateMapping();
        }

        [Test]
        public void CanSerializeString()
        {
            var writer = new ByteBufferWriter();
            var storedDescriptorCtx = _mapping.StoreNewDescriptors(writer, "Hello");
            _mapping.StoreObject(writer, "Hello");
            Assert.Null(storedDescriptorCtx);
            _mapping.CommitNewDescriptors(null);
            var reader = new ByteBufferReader(writer.Data);
            var obj = _mapping.LoadObject(reader);
            Assert.AreEqual("Hello", obj);
        }

        [Test]
        public void CanSerializeInt()
        {
            var writer = new ByteBufferWriter();
            var storedDescriptorCtx = _mapping.StoreNewDescriptors(writer, 12345);
            _mapping.StoreObject(writer, 12345);
            Assert.Null(storedDescriptorCtx);
            _mapping.CommitNewDescriptors(null);
            var reader = new ByteBufferReader(writer.Data);
            var obj = _mapping.LoadObject(reader);
            Assert.AreEqual(12345, (int)obj);
        }


    }
}