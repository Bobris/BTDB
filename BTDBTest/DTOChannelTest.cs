using System;
using System.Collections.Generic;
using BTDB.DtoChannel;
using BTDB.EventStoreLayer;
using BTDB.Service;
using Xunit;

namespace BTDBTest
{
    public class DtoChannelTest : IDisposable
    {
        PipedTwoChannels _pipedTwoChannels;
        IDtoChannel _first;
        IDtoChannel _second;

        public DtoChannelTest()
        {
            _pipedTwoChannels = new PipedTwoChannels();
            _first = new DtoChannel(_pipedTwoChannels.First, new TypeSerializers());
            _second = new DtoChannel(_pipedTwoChannels.Second, new TypeSerializers());
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

        class IdentityUserV2
        {
            public IdentityUserMetadata Metadata { get; set; }
        }

        public class IdentityUserMetadata
        {
            public Dictionary<ulong, Dictionary<string, UserMetadataValue>> Application { get; set; }
        }

        public class UserMetadataValue
        {
            public string StringValue { get; set; }
            public bool IsReadOnly { get; set; }
        }

        [Fact]
        public void CanSendNestedDictionary()
        {
            var user = new IdentityUserV2
            {
                Metadata = new IdentityUserMetadata
                {
                    Application = new Dictionary<ulong, Dictionary<string, UserMetadataValue>>
                    {
                        [0] = new Dictionary<string, UserMetadataValue>
                        {
                            ["Foo"] = new UserMetadataValue
                            {
                                StringValue = "Bar",
                            }
                        }
                    }
                }
            };
            var firstReceived = new List<object>();
            var secondReceived = new List<object>();
            _first.OnReceive.Subscribe(firstReceived.Add);
            _second.OnReceive.Subscribe(secondReceived.Add);

            _first.Send(user);
            var user1 = secondReceived[0];

            _first.Send(user1); // fail
            _second.Send(user1); // fail
            var user2 = firstReceived[0];

            _first.Send(user2);
        }
    }
}