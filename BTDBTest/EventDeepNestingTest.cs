using System;
using System.Threading;
using BTDB;
using BTDB.Buffer;
using BTDB.EventStore2Layer;
using BTDB.KVDBLayer;
using Xunit;
using static BTDBTest.EventLayerRoundTripHelper;

namespace BTDBTest;

// Before the fix an event whose objects nest deeper than the stack holds, like a long linked list, overflowed the stack
// and ended the process.
public class EventDeepNestingTest
{
    [Generate]
    public class DeepEventNode
    {
        public int Value { get; set; }
        public DeepEventNode? Next { get; set; }
    }

    static DeepEventNode Chain(int depth)
    {
        var head = new DeepEventNode();
        for (var i = 1; i < depth; i++) head = new DeepEventNode { Value = i, Next = head };
        return head;
    }

    static Exception? RunWithStack(int stackSize, Action action)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try
            {
                action();
            }
            catch (Exception e)
            {
                error = e;
            }
        }, stackSize);
        thread.Start();
        thread.Join();
        return error;
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void TooDeepToStoreFailsWithException(EventLayer layer)
    {
        var error = RunWithStack(1024 * 1024, () => RoundTrip(layer, Chain(100_000)));
        Assert.IsType<BTDBException>(error);
    }

    [Fact]
    public void TooDeepToReadFailsWithException()
    {
        var serializer = new EventSerializer();
        ByteBuffer meta = default, data = default;
        Assert.Null(RunWithStack(256 * 1024 * 1024, () =>
        {
            var value = Chain(20_000);
            meta = serializer.Serialize(out _, value).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            data = serializer.Serialize(out _, value).ToAsyncSafe();
        }));
        var deserializer = new EventDeserializer();
        deserializer.ProcessMetadataLog(meta);
        Assert.IsType<BTDBException>(RunWithStack(512 * 1024, () => deserializer.Deserialize(out _, data)));
    }
}
