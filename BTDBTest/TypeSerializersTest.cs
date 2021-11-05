using Assent;
using BTDB.Buffer;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using BTDB.Encrypted;
using Microsoft.Extensions.Primitives;
using Xunit;

namespace BTDBTest;

public class TypeSerializersTest
{
    ITypeSerializers _ts;
    ITypeSerializersMapping _mapping;

    public TypeSerializersTest()
    {
        _ts = new TypeSerializers();
        _mapping = _ts.CreateMapping();
    }

    [Fact]
    public void CanSerializeString()
    {
        var writer = new SpanWriter();
        var storedDescriptorCtx = _mapping.StoreNewDescriptors("Hello");
        storedDescriptorCtx.FinishNewDescriptors(ref writer);
        storedDescriptorCtx.StoreObject(ref writer, "Hello");
        storedDescriptorCtx.CommitNewDescriptors();
        var reader = new SpanReader(writer.GetSpan());
        var obj = _mapping.LoadObject(ref reader);
        Assert.Equal("Hello", obj);
    }

    [Fact]
    public void CanSerializeInt()
    {
        var writer = new SpanWriter();
        var storedDescriptorCtx = _mapping.StoreNewDescriptors(12345);
        storedDescriptorCtx.FinishNewDescriptors(ref writer);
        storedDescriptorCtx.StoreObject(ref writer, 12345);
        storedDescriptorCtx.CommitNewDescriptors();
        var reader = new SpanReader(writer.GetSpan());
        var obj = _mapping.LoadObject(ref reader);
        Assert.Equal(12345, obj);
    }

    [Fact]
    public void CanSerializeSimpleTypes()
    {
        CanSerializeSimpleValue((byte)42);
        CanSerializeSimpleValue((sbyte)-20);
        CanSerializeSimpleValue((short)-1234);
        CanSerializeSimpleValue((ushort)1234);
        CanSerializeSimpleValue((uint)123456789);
        CanSerializeSimpleValue(-123456789012L);
        CanSerializeSimpleValue(123456789012UL);
        CanSerializeSimpleValue(new Version(4, 3, 2, 1));
        CanSerializeSimpleValue(new StringValues(new[] { "a", "b" }));
    }

    void CanSerializeSimpleValue(object value)
    {
        var writer = new SpanWriter();
        var storedDescriptorCtx = _mapping.StoreNewDescriptors(value);
        storedDescriptorCtx.FinishNewDescriptors(ref writer);
        storedDescriptorCtx.StoreObject(ref writer, value);
        storedDescriptorCtx.CommitNewDescriptors();
        var reader = new SpanReader(writer.GetSpan());
        var obj = _mapping.LoadObject(ref reader);
        Assert.Equal(value, obj);
    }

    public class SimpleDto
    {
        public string StringField { get; set; }
        public int IntField { get; set; }
    }

    public class SimpleDtoWithoutDefaultConstructor
    {
        public SimpleDtoWithoutDefaultConstructor(string a)
        {
            StringField = a;
        }

        public string StringField { get; set; }
        public int IntField { get; set; }
    }


    [Fact]
    public void CanSerializeSimpleDto()
    {
        var writer = new SpanWriter();
        var value = new SimpleDto { IntField = 42, StringField = "Hello" };
        var storedDescriptorCtx = _mapping.StoreNewDescriptors(value);
        storedDescriptorCtx.FinishNewDescriptors(ref writer);
        storedDescriptorCtx.StoreObject(ref writer, value);
        storedDescriptorCtx.CommitNewDescriptors();
        var reader = new SpanReader(writer.GetSpan());
        _mapping.LoadTypeDescriptors(ref reader);
        var obj = (SimpleDto)_mapping.LoadObject(ref reader);
        Assert.Equal(value.IntField, obj.IntField);
        Assert.Equal(value.StringField, obj.StringField);
    }

    [Fact]
    public void CanSerializeSimpleDtoWithoutDefaultConstructor()
    {
        var writer = new SpanWriter();
        var value = new SimpleDtoWithoutDefaultConstructor("Hello") { IntField = 42 };
        var storedDescriptorCtx = _mapping.StoreNewDescriptors(value);
        storedDescriptorCtx.FinishNewDescriptors(ref writer);
        storedDescriptorCtx.StoreObject(ref writer, value);
        storedDescriptorCtx.CommitNewDescriptors();
        var reader = new SpanReader(writer.GetSpan());
        _mapping.LoadTypeDescriptors(ref reader);
        var obj = (SimpleDtoWithoutDefaultConstructor)_mapping.LoadObject(ref reader);
        Assert.Equal(value.IntField, obj.IntField);
        Assert.Equal(value.StringField, obj.StringField);
    }

    void TestSerialization(object value)
    {
        var writer = new SpanWriter();
        var storedDescriptorCtx = _mapping.StoreNewDescriptors(value);
        storedDescriptorCtx.FinishNewDescriptors(ref writer);
        storedDescriptorCtx.StoreObject(ref writer, value);
        storedDescriptorCtx.CommitNewDescriptors();
        var reader = new SpanReader(writer.GetSpan());
        _mapping.LoadTypeDescriptors(ref reader);
        Assert.Equal(value, _mapping.LoadObject(ref reader));
        Assert.True(reader.Eof);
        _mapping = _ts.CreateMapping();
        reader = new SpanReader(writer.GetSpan());
        _mapping.LoadTypeDescriptors(ref reader);
        Assert.Equal(value, _mapping.LoadObject(ref reader));
        Assert.True(reader.Eof);
    }

#pragma warning disable 659
    public class ClassWithList : IEquatable<ClassWithList>
    {
        public List<int>? List { get; set; }

        public bool Equals(ClassWithList other)
        {
            if (List == other.List) return true;
            if (List == null || other.List == null) return false;
            if (List.Count != other.List.Count) return false;
            return List.Zip(other.List, (i1, i2) => i1 == i2).All(p => p);
        }

        public override bool Equals(object obj)
        {
            return Equals((obj as ClassWithList)!);
        }
    }

    [Fact]
    public void ListCanHaveContent()
    {
        TestSerialization(new ClassWithList { List = new List<int> { 1, 2, 3 } });
    }

    [Fact]
    public void ListCanBeEmpty()
    {
        TestSerialization(new ClassWithList { List = new List<int>() });
    }

    [Fact]
    public void ListCanBeNull()
    {
        TestSerialization(new ClassWithList { List = null });
    }

    public class ClassWithDict : IEquatable<ClassWithDict>
    {
        public Dictionary<int, string>? Dict { get; set; }

        public bool Equals(ClassWithDict other)
        {
            if (Dict == other.Dict) return true;
            if (Dict == null || other.Dict == null) return false;
            if (Dict.Count != other.Dict.Count) return false;
            foreach (var pair in Dict)
            {
                if (!other.Dict.ContainsKey(pair.Key))
                    return false;

                if (other.Dict[pair.Key] != pair.Value)
                    return false;
            }

            return true;
        }

        public override bool Equals(object obj)
        {
            return Equals((obj as ClassWithDict)!);
        }
    }

    [Fact]
    public void DictionaryCanHaveContent()
    {
        TestSerialization(new ClassWithDict
        { Dict = new Dictionary<int, string> { { 1, "a" }, { 2, "b" }, { 3, "c" } } });
    }

    [Fact]
    public void DictionaryCanBeEmpty()
    {
        TestSerialization(new ClassWithDict { Dict = new Dictionary<int, string>() });
    }

    [Fact]
    public void DictionaryCanBeNull()
    {
        TestSerialization(new ClassWithDict { Dict = null });
    }

    [Fact]
    public void BasicDescribe()
    {
        var ts = new TypeSerializers();
        var res = new object[]
        {
                "",
                1,
                1U,
                (byte)1,
                (sbyte)1,
                (short)1,
                (ushort)1,
                1L,
                1UL,
                (double)1,
                (decimal)1,
                new DateTime(),
                new TimeSpan(),
                Guid.Empty,
                Array.Empty<byte>(),
                ByteBuffer.NewEmpty(),
                false,
                new EncryptedString(),
                (1, 2u),
                new Tuple<int, uint>(1, 2)
        }.Select(o => ts.DescriptorOf(o)!.Describe());
        this.Assent(string.Join("\n", res) + "\n");
    }

    [Fact]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public void CheckCompatibilityOfRegistrationOfBasicTypeDescriptors()
    {
        this.Assent(string.Join("\n", BasicSerializersFactory.TypeDescriptors.Select(o => o.Name)));
    }

    public class SelfPointing1
    {
        public SelfPointing1 Self1 { get; set; }
        public SelfPointing2 Self2 { get; set; }
        public int Other1 { get; set; }
    }

    public class SelfPointing2
    {
        public SelfPointing1 Self1 { get; set; }
        public SelfPointing2 Self2 { get; set; }
        public string Other2 { get; set; }
    }

    public enum TestEnum
    {
        Item1,
        Item2
    }

    [Fact]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public void ComplexDescribe()
    {
        var ts = new TypeSerializers();
        var res = new object[]
        {
                new List<int>(),
                new Dictionary<string, double>(),
                new SimpleDto(),
                new ClassWithList(),
                new ClassWithDict(),
                new SelfPointing1(),
                new SelfPointing2(),
                new TestEnum()
        }.Select(o => ts.DescriptorOf(o).Describe());
        this.Assent(string.Join("\n", res));
    }

    [Fact]
    public void BasicEnumTest1()
    {
        TestSerialization(TestEnum.Item1);
    }

    [Fact]
    public void BasicEnumTest2()
    {
        TestSerialization(TestEnum.Item2);
    }

    [Fact]
    public void BasicTupleTest1()
    {
        TestSerialization((1, 2u));
    }

    [Fact]
    public void BasicTupleTest2()
    {
        TestSerialization(new Tuple<int, uint>(1, 2u));
    }

    dynamic ConvertToDynamicThroughSerialization(object value)
    {
        var writer = new SpanWriter();
        var storedDescriptorCtx = _mapping.StoreNewDescriptors(value);
        storedDescriptorCtx.FinishNewDescriptors(ref writer);
        storedDescriptorCtx.StoreObject(ref writer, value);
        storedDescriptorCtx.CommitNewDescriptors();
        var originalDescription = _ts.DescriptorOf(value).Describe();
        var reader = new SpanReader(writer.GetSpan());
        var ts = new TypeSerializers();
        ts.SetTypeNameMapper(new ToDynamicMapper());
        var mapping = ts.CreateMapping();
        mapping.LoadTypeDescriptors(ref reader);
        var obj = (dynamic)mapping.LoadObject(ref reader);
        Assert.Equal(originalDescription, ts.DescriptorOf((object)obj)!.Describe());
        return obj;
    }

    public class ToDynamicMapper : ITypeNameMapper
    {
        public string ToName(Type type)
        {
            return type.FullName;
        }

        public Type ToType(string name)
        {
            return null;
        }
    }

    [Fact]
    public void CanDeserializeSimpleDtoToDynamic()
    {
        var value = new SimpleDto { IntField = 42, StringField = "Hello" };
        var obj = ConvertToDynamicThroughSerialization(value);
        Assert.Equal(value.IntField, obj.IntField);
        Assert.Equal(value.StringField, obj.StringField);
        Assert.Throws<MemberAccessException>(() =>
        {
            var garbage = obj.Garbage;
        });
        var descriptor = _ts.DescriptorOf((object)obj);
        Assert.NotNull(descriptor);
        Assert.True(descriptor.ContainsField("IntField"));
        Assert.False(descriptor.ContainsField("Garbage"));
        Assert.Equal("String", descriptor.Fields.First(a => a.Key == "StringField").Value.Name);
        Assert.Equal(
            new[]
            {
                    new KeyValuePair<string, object>("IntField", 42),
                    new KeyValuePair<string, object>("StringField", "Hello")
            }, ((IEnumerable<KeyValuePair<string, object>>)obj)!.ToArray());
    }

    [Fact]
    public void CanDeserializeTupleToDynamic()
    {
        var value = (1, 2u);
        var obj = ConvertToDynamicThroughSerialization(value);
        Assert.Equal(value.Item1, obj.Item1);
        Assert.Equal(value.Item2, obj.Item2);
        var descriptor = _ts.DescriptorOf((object)obj);
        Assert.NotNull(descriptor);
        Assert.True(descriptor.ContainsField("Item2"));
        Assert.False(descriptor.ContainsField("Garbage"));
        Assert.Equal("VInt32", descriptor.Fields.First(a => a.Key == "Item1").Value.Name);
    }

    [Fact]
    public void CanDeserializeListToDynamic()
    {
        var value = new List<SimpleDto> { new SimpleDto { IntField = 42, StringField = "Hello" } };
        var obj = ConvertToDynamicThroughSerialization(value);
        Assert.Equal(value.Count, obj.Count);
        Assert.Equal(value[0].IntField, obj[0].IntField);
        Assert.Equal(value[0].StringField, obj[0].StringField);
    }

    [Fact]
    public void CanDeserializeDictionaryToDynamic()
    {
        var value = new Dictionary<int, SimpleDto>
                { { 10, new SimpleDto { IntField = 42, StringField = "Hello" } } };
        var obj = ConvertToDynamicThroughSerialization(value);
        Assert.Equal(value.Count, obj.Count);
        Assert.Equal(value[10].IntField, obj[10].IntField);
        Assert.Equal(value[10].StringField, obj[10].StringField);
    }

    [Fact]
    public void CanDeserializeEnumToDynamic()
    {
        var value = (object)TestEnum.Item1;
        var obj = ConvertToDynamicThroughSerialization(value);
        Assert.Equal(value.ToString(), obj.ToString());
        Assert.Equal(value.GetHashCode(), obj.GetHashCode());
        Assert.True(obj.Equals(value));
        Assert.False(obj.Equals(TestEnum.Item2));
    }

    [Fact]
    public void CanDeserializeFlagsEnumToDynamic()
    {
        var value = (object)(AttributeTargets.Method | AttributeTargets.Property);
        var obj = ConvertToDynamicThroughSerialization(value);
        Assert.Equal(value.ToString(), obj.ToString());
        Assert.Equal(value.GetHashCode(), obj.GetHashCode());
        Assert.True(obj.Equals(value));
        Assert.False(obj.Equals(AttributeTargets.Method));
    }

    public class SimpleDtoWithNullable
    {
        public int? IntField { get; set; }
    }

    [Fact]
    public void CanDeserializeNullableToDynamic()
    {
        var value = new SimpleDtoWithNullable { IntField = 1 };
        var obj = ConvertToDynamicThroughSerialization(value);
        Assert.Equal(1, obj.IntField);
    }

    public class ClassWithIDict : IEquatable<ClassWithIDict>
    {
        public IDictionary<Guid, IList<SimpleDto>> Dict { get; set; }

        public bool Equals(ClassWithIDict other)
        {
            if (Dict == other.Dict) return true;
            if (Dict == null || other.Dict == null) return false;
            if (Dict.Count != other.Dict.Count) return false;
            foreach (var pair in Dict)
            {
                if (!other.Dict.ContainsKey(pair.Key))
                    return false;

                if (!other.Dict[pair.Key].SequenceEqual(pair.Value, new SimpleDtoComparer()))
                    return false;
            }

            return true;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ClassWithIDict);
        }

        public class SimpleDtoComparer : IEqualityComparer<SimpleDto>
        {
            public bool Equals(SimpleDto x, SimpleDto y)
            {
                return x.IntField == y.IntField && x.StringField == y.StringField;
            }

            public int GetHashCode(SimpleDto obj)
            {
                return obj.IntField;
            }
        }
    }
#pragma warning restore 659

    [Fact]
    public void DictionaryAsIfaceCanHaveContent()
    {
        TestSerialization(new ClassWithIDict
        {
            Dict = new Dictionary<Guid, IList<SimpleDto>>
                {
                    { Guid.NewGuid(), new List<SimpleDto> { new SimpleDto { IntField = 1, StringField = "a" } } },
                    { Guid.NewGuid(), new List<SimpleDto> { new SimpleDto { IntField = 2, StringField = "b" } } },
                    { Guid.NewGuid(), new List<SimpleDto> { new SimpleDto { IntField = 3, StringField = "c" } } }
                }
        });
    }

    class GenericClass<T>
    {
        public T Value { get; set; }

        public override bool Equals(object obj)
        {
            return obj is GenericClass<T> wrapper &&
                   EqualityComparer<T>.Default.Equals(Value, wrapper.Value);
        }

        public override int GetHashCode()
        {
            return Value?.GetHashCode() ?? 0;
        }
    }

    [Fact]
    public void CanSerializeGenericType()
    {
        TestSerialization(new GenericClass<int> { Value = 42 });
    }

    class ClassWithIOrderedDictionary : IEquatable<ClassWithIOrderedDictionary>
    {
        public IOrderedDictionary<int, int> IOrderedDictionary { get; set; }

        public override bool Equals(object obj)
        {
            return Equals(obj as ClassWithIOrderedDictionary);
        }

        public bool Equals(ClassWithIOrderedDictionary other)
        {
            if (other == null)
                return false;
            if (IOrderedDictionary == other.IOrderedDictionary) return true;
            if (IOrderedDictionary == null || other.IOrderedDictionary == null) return false;
            if (IOrderedDictionary.Count != other.IOrderedDictionary.Count) return false;
            foreach (var pair in IOrderedDictionary)
            {
                if (!other.IOrderedDictionary.ContainsKey(pair.Key))
                    return false;

                if (other.IOrderedDictionary[pair.Key] != pair.Value)
                    return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            return IOrderedDictionary?.GetHashCode() ?? 0;
        }
    }

    class DummyOrderedDictionary<TKey, TValue> : Dictionary<TKey, TValue>, IOrderedDictionary<TKey, TValue>
    {
        public IOrderedDictionaryEnumerator<TKey, TValue>
            GetAdvancedEnumerator(AdvancedEnumeratorParam<TKey> param) => throw new NotSupportedException();

        public IEnumerable<KeyValuePair<TKey, TValue>> GetDecreasingEnumerator(TKey start) =>
            throw new NotSupportedException();

        public IEnumerable<KeyValuePair<TKey, TValue>> GetIncreasingEnumerator(TKey start) =>
            throw new NotSupportedException();

        public IEnumerable<KeyValuePair<TKey, TValue>> GetReverseEnumerator() => throw new NotSupportedException();

        public long RemoveRange(TKey start, bool includeStart, TKey end, bool includeEnd) =>
            throw new NotSupportedException();
    }

    [Fact]
    public void CanSerializeIOrderedDictionaryType()
    {
        TestSerialization(new ClassWithIOrderedDictionary
        {
            IOrderedDictionary = new DummyOrderedDictionary<int, int>
            {
                [1] = 2,
                [2] = 3,
            }
        });
    }

    public class ClassWithBoxedIEnumerable
    {
        public object? Value { get; set; }

        public override bool Equals(object obj)
        {
            if (obj is ClassWithBoxedIEnumerable o)
            {
                var enumA = ((IEnumerable)Value).GetEnumerator();
                var enumB = ((IEnumerable)o.Value).GetEnumerator();
                enumA.Reset();
                enumB.Reset();

                while (enumA.MoveNext() | enumB.MoveNext())
                {
                    if (!enumA.Current.Equals(enumB.Current))
                        return false;
                }

                return true;
            }

            return false;
        }

        public override int GetHashCode()
        {
            return Value?.GetHashCode() ?? 0;
        }
    }

    [Fact]
    public void CanSerializeBoxedList()
    {
        TestSerialization(new ClassWithBoxedIEnumerable
        {
            Value = new List<int>
                {
                    1, 2, 3
                }
        });
    }

    [Fact]
    public void CanSerializeBoxedDictionary()
    {
        TestSerialization(new ClassWithBoxedIEnumerable
        {
            Value = new Dictionary<int, int>
            {
                [1] = 2,
                [2] = 3
            }
        });
    }

    class MyList<T> : List<T>
    {
    }

    [Fact(Skip = "By design - not supported yet")]
    public void CanSerializeBoxedCustomList()
    {
        TestSerialization(new ClassWithBoxedIEnumerable
        {
            Value = new MyList<int>
                {
                    1, 2, 3
                }
        });
    }

    [Fact(Skip = "By design - not supported yet")]
    public void CanSerializeBoxedIOrderedDictionary()
    {
        TestSerialization(new ClassWithBoxedIEnumerable
        {
            Value = new DummyOrderedDictionary<int, int>
            {
                [1] = 2,
                [2] = 3
            }
        });
    }
}
