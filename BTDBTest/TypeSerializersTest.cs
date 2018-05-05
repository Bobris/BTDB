using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Assent;
using BTDB.Buffer;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest
{
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
            var writer = new ByteBufferWriter();
            var storedDescriptorCtx = _mapping.StoreNewDescriptors(writer, "Hello");
            storedDescriptorCtx.FinishNewDescriptors(writer);
            storedDescriptorCtx.StoreObject(writer, "Hello");
            storedDescriptorCtx.CommitNewDescriptors();
            var reader = new ByteBufferReader(writer.Data);
            var obj = _mapping.LoadObject(reader);
            Assert.Equal("Hello", obj);
        }

        [Fact]
        public void CanSerializeInt()
        {
            var writer = new ByteBufferWriter();
            var storedDescriptorCtx = _mapping.StoreNewDescriptors(writer, 12345);
            storedDescriptorCtx.FinishNewDescriptors(writer);
            storedDescriptorCtx.StoreObject(writer, 12345);
            storedDescriptorCtx.CommitNewDescriptors();
            var reader = new ByteBufferReader(writer.Data);
            var obj = _mapping.LoadObject(reader);
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
        }

        void CanSerializeSimpleValue(object value)
        {
            var writer = new ByteBufferWriter();
            var storedDescriptorCtx = _mapping.StoreNewDescriptors(writer, value);
            storedDescriptorCtx.FinishNewDescriptors(writer);
            storedDescriptorCtx.StoreObject(writer, value);
            storedDescriptorCtx.CommitNewDescriptors();
            var reader = new ByteBufferReader(writer.Data);
            var obj = _mapping.LoadObject(reader);
            Assert.Equal(value, obj);
        }

        public class SimpleDto
        {
            public string StringField { get; set; }
            public int IntField { get; set; }
        }

        [Fact]
        public void CanSerializeSimpleDto()
        {
            var writer = new ByteBufferWriter();
            var value = new SimpleDto { IntField = 42, StringField = "Hello" };
            var storedDescriptorCtx = _mapping.StoreNewDescriptors(writer, value);
            storedDescriptorCtx.FinishNewDescriptors(writer);
            storedDescriptorCtx.StoreObject(writer, value);
            storedDescriptorCtx.CommitNewDescriptors();
            var reader = new ByteBufferReader(writer.Data);
            _mapping.LoadTypeDescriptors(reader);
            var obj = (SimpleDto)_mapping.LoadObject(reader);
            Assert.Equal(value.IntField, obj.IntField);
            Assert.Equal(value.StringField, obj.StringField);
        }

        void TestSerialization(object value)
        {
            var writer = new ByteBufferWriter();
            var storedDescriptorCtx = _mapping.StoreNewDescriptors(writer, value);
            storedDescriptorCtx.FinishNewDescriptors(writer);
            storedDescriptorCtx.StoreObject(writer, value);
            storedDescriptorCtx.CommitNewDescriptors();
            var reader = new ByteBufferReader(writer.Data);
            _mapping.LoadTypeDescriptors(reader);
            Assert.Equal(value, _mapping.LoadObject(reader));
            Assert.True(reader.Eof);
            _mapping = _ts.CreateMapping();
            reader = new ByteBufferReader(writer.Data);
            _mapping.LoadTypeDescriptors(reader);
            Assert.Equal(value, _mapping.LoadObject(reader));
            Assert.True(reader.Eof);
        }

#pragma warning disable 659
        public class ClassWithList : IEquatable<ClassWithList>
        {
            public List<int> List { get; set; }

            public bool Equals(ClassWithList other)
            {
                if (List == other.List) return true;
                if (List == null || other.List == null) return false;
                if (List.Count != other.List.Count) return false;
                return List.Zip(other.List, (i1, i2) => i1 == i2).All(p => p);
            }

            public override bool Equals(object obj)
            {
                return Equals(obj as ClassWithList);
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
            public Dictionary<int, string> Dict { get; set; }

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
                return Equals(obj as ClassWithDict);
            }
        }

        [Fact]
        public void DictionaryCanHaveContent()
        {
            TestSerialization(new ClassWithDict { Dict = new Dictionary<int, string> { { 1, "a" }, { 2, "b" }, { 3, "c" } } });
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
                    (byte) 1,
                    (sbyte) 1,
                    (short) 1,
                    (ushort) 1,
                    1L,
                    1UL,
                    (double)1,
                    (decimal)1,
                    new DateTime(),
                    new TimeSpan(),
                    Guid.Empty,
                    new byte[0],
                    ByteBuffer.NewEmpty(),
                    false
                }.Select(o => ts.DescriptorOf(o).Describe());
            this.Assent(string.Join("\n", res));
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
                    new Dictionary<string,double>(),
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

        dynamic ConvertToDynamicThroughSerialization(object value)
        {
            var writer = new ByteBufferWriter();
            var storedDescriptorCtx = _mapping.StoreNewDescriptors(writer, value);
            storedDescriptorCtx.FinishNewDescriptors(writer);
            storedDescriptorCtx.StoreObject(writer, value);
            storedDescriptorCtx.CommitNewDescriptors();
            var originalDescription = _ts.DescriptorOf(value).Describe();
            var reader = new ByteBufferReader(writer.Data);
            var ts = new TypeSerializers();
            ts.SetTypeNameMapper(new ToDynamicMapper());
            var mapping = ts.CreateMapping();
            mapping.LoadTypeDescriptors(reader);
            var obj = (dynamic)mapping.LoadObject(reader);
            Assert.Equal(originalDescription, ts.DescriptorOf((object)obj).Describe());
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
            var value = new Dictionary<int, SimpleDto> { { 10, new SimpleDto { IntField = 42, StringField = "Hello" } } };
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
                    { Guid.NewGuid(), new List<SimpleDto> { new SimpleDto {IntField = 1,StringField = "a" } } },
                    { Guid.NewGuid(), new List<SimpleDto> { new SimpleDto {IntField = 2,StringField = "b" } } },
                    { Guid.NewGuid(), new List<SimpleDto> { new SimpleDto {IntField = 3,StringField = "c" } } }
                }
            });
        }

    }
}