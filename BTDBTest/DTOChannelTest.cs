using BTDB.DtoChannel;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using BTDB.Service;
using System;
using System.Collections.Generic;
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
            var options = new TypeSerializersOptions { IgnoreIIndirect = false };
            _first = new DtoChannel(_pipedTwoChannels.First, new TypeSerializers(options: options));
            _second = new DtoChannel(_pipedTwoChannels.Second, new TypeSerializers(options: options));
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

        [Fact]
        public void CanSendIIndirect()
        {
            File o1 = new File
            {
                Id = 1,
                RawData = new IIndirectImpl<ByteData>(new ByteData { Data = 2 })
            };
            File o2 = null;
            _second.OnReceive.Subscribe(o => o2 = (File)o);
            _first.Send(o1);
            Assert.True(o1.Equals(o2));
        }

        class File : IEquatable<File>
        {
            public ulong Id { get; set; }
            public IIndirect<ByteData> RawData { get; set; }

            public bool Equals(File other)
            {
                return Id == other.Id && RawData.Value.Data == other.RawData.Value.Data;
            }
        }

        class ByteData
        {
            public int Data { get; set; }
        }

        class IIndirectImpl<T> : IIndirect<T> where T : class
        {
            public T Value { get; set; }

            [NotStored]
            public ulong Oid => throw new NotSupportedException();

            [NotStored]
            public object ValueAsObject => throw new NotSupportedException();

            // required for deserialization
            public IIndirectImpl()
            {
            }

            public IIndirectImpl(T value)
            {
                Value = value;
            }
        }
    }
}