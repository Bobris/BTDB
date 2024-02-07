using Assent;
using BTDB;
using BTDB.Bon;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest.SerializationTests;

public class BonSerializerTest
{
    [Generate]
    public class AllSupportedTypes
    {
        public string Str;
        public int Int32;
    }

    [Fact]
    public void SerializeDeserializeAllSupportedTypes()
    {
        var obj = new AllSupportedTypes { Str = "Hello", Int32 = 42 };
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, obj);
        var bon = new Bon(builder.FinishAsMemory());
        this.Assent(bon.DumpToJson());
    }
}
