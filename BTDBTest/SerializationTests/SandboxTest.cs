using System.Runtime.CompilerServices;
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

    static void Setter(Person person, string value)
    {
        person.Name = value;
    }

    static void Setter2(object @this, object value)
    {
        Unsafe.As<Person>(@this).Name = Unsafe.As<string>(value);
    }

    static object Creator()
    {
        return RuntimeHelpers.GetUninitializedObject(typeof(Person));
    }

    [Fact]
    public unsafe void Sandbox()
    {
        var metadata = new ClassMetadata();
        metadata.Name = "Person";
        metadata.Type = typeof(Person);
        metadata.Namespace = "BTDBTest.SerializationTests";
        metadata.Creator = &Creator;
        var dummy = Unsafe.As<Person>(metadata);
        metadata.Fields =
        [
            new FieldMetadata
            {
                Name = "Name",
                Type = typeof(string),
                PropRefGetter = &GetName,
                PropRefSetter = &SetName
            },
            new FieldMetadata
            {
                Name = "Age",
                Type = typeof(int),
                ByteOffset = RawData.CalcOffset(dummy, ref dummy.Age),
            }
        ];

        Assert.Equal(2 * Unsafe.SizeOf<object>(), (int)metadata.Fields[1].ByteOffset.GetValueOrDefault());

        static void GetName(object obj, ref byte value)
        {
            Unsafe.As<byte, string>(ref value) = Unsafe.As<Person>(obj).Name;
        }

        static void SetName(object obj, ref byte value)
        {
            Unsafe.As<Person>(obj).Name = Unsafe.As<byte, string>(ref value);
        }
    }
}
