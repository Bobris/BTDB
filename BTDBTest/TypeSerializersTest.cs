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
            _mapping.CommitNewDescriptors(storedDescriptorCtx);
        }
    }
}