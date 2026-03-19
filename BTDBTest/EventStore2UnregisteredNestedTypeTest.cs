using BTDB;
using BTDB.EventStore2Layer;
using BTDB.FieldHandler;
using Xunit;

namespace BTDBTest;

public class EventStore2UnregisteredNestedTypeTest
{
    [Generate]
    public class EventWithObjectProperty
    {
        public object Data { get; set; }
    }

    [Fact]
    public void SerializingClassWithObjectPropertySetToNewObject()
    {
        var serializer = new EventSerializer();
        var obj = new EventWithObjectProperty { Data = new object() };

        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out var deserialized, data));

        var result = deserialized as EventWithObjectProperty;
        Assert.NotNull(result);
    }
}
