using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest.SerializationTests;

public class SandboxTest
{
    public class Person
    {
        public string Name { get; set; } = "";
        public int Age;
    }

    [Fact]
    public unsafe void Sandbox()
    {
        var metadata = new ClassMetadata();
        metadata.Name = "Person";
        metadata.Type = typeof(Person);
        metadata.Namespace = "BTDBTest.SerializationTests";
        var dummy = Unsafe.As<Person>(metadata);
        metadata.Fields = new[]
        {
            new FieldMetadata
            {
                Name = "Name",
                Type = typeof(string),
                PropObjGetter = &GetName,
                PropObjSetter = &SetName
            },
            new FieldMetadata
            {
                Name = "Age",
                Type = typeof(int),
                ByteOffset = RawData.CalcOffset(dummy, ref dummy.Age)
            }
        };

        Assert.Equal(2 * Unsafe.SizeOf<object>(), (int)metadata.Fields[1].ByteOffset.GetValueOrDefault());

        static object GetName(object obj)
        {
            return Unsafe.As<Person>(obj).Name;
        }

        static void SetName(object obj, object value)
        {
            Unsafe.As<Person>(obj).Name = Unsafe.As<string>(value);
        }
    }
}
