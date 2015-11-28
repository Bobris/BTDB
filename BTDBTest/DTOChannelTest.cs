using System;
using BTDB.DtoChannel;
using BTDB.EventStoreLayer;
using BTDB.Service;
using Xunit;

namespace BTDBTest
{
    public class DtoChannelTest : IDisposable
    {
        PipedTwoChannels _pipedTwoChannels;
        ITypeSerializerMappingFactory _typeSerializers;
        IDtoChannel _first;
        IDtoChannel _second;

        public DtoChannelTest()
        {
            _pipedTwoChannels = new PipedTwoChannels();
            _typeSerializers = new TypeSerializers();
            _first = new DtoChannel(_pipedTwoChannels.First, _typeSerializers);
            _second = new DtoChannel(_pipedTwoChannels.Second, _typeSerializers);
        }

        public void Dispose()
        {
            Disconnect();
            _first.Dispose();
            _second.Dispose();
        }

        void Disconnect()
        {
            _pipedTwoChannels.Disconnect();
        }

        [Fact]
        public void CanSendSimpleType()
        {
            var u1 = new EventStoreTest.User { Name = "A", Age = 1 };
            object u2 = null;
            _second.OnReceive.Subscribe(o => u2 = o);
            _first.Send(u1);
            Assert.True(u1.Equals((EventStoreTest.User)u2));
        }

        [Fact]
        public void CanSendSimpleTypeTwice()
        {
            var u1 = new EventStoreTest.User { Name = "A", Age = 1 };
            object u2 = null;
            _second.OnReceive.Subscribe(o => u2 = o);
            _first.Send(u1);
            u2 = null;
            _first.Send(u1);
            Assert.True(u1.Equals((EventStoreTest.User)u2));
        }
    }
}