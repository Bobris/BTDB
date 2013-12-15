using System;
using BTDB.DtoChannel;
using BTDB.EventStoreLayer;
using BTDB.Service;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class DtoChannelTest
    {
        PipedTwoChannels _pipedTwoChannels;
        ITypeSerializerMappingFactory _typeSerializers;
        IDtoChannel _first;
        IDtoChannel _second;

        [SetUp]
        public void Setup()
        {
            _pipedTwoChannels = new PipedTwoChannels();
            _typeSerializers = new TypeSerializers();
            _first = new DtoChannel(_pipedTwoChannels.First, _typeSerializers);
            _second = new DtoChannel(_pipedTwoChannels.Second, _typeSerializers);
        }

        [TearDown]
        public void TearDown()
        {
            Disconnect();
            _first.Dispose();
            _second.Dispose();
        }

        void Disconnect()
        {
            _pipedTwoChannels.Disconnect();
        }

        [Test]
        public void CanSendSimpleType()
        {
            object u1 = new EventStoreTest.User { Name = "A", Age = 1 };
            object u2 = null;
            _second.OnReceive.Subscribe(o => u2 = o);
            _first.Send(u1);
            Assert.AreEqual(u1, u2);
        }

        [Test]
        public void CanSendSimpleTypeTwice()
        {
            object u1 = new EventStoreTest.User { Name = "A", Age = 1 };
            object u2 = null;
            _second.OnReceive.Subscribe(o => u2 = o);
            _first.Send(u1);
            u2 = null;
            _first.Send(u1);
            Assert.AreEqual(u1, u2);
        }
    }
}