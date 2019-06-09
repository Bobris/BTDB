using BTDB.EventStoreLayer;
using System;
using System.Collections.Generic;
using Xunit;

namespace BTDBTest
{
    public class FullNameTypeMapperTest
    {
        readonly FullNameTypeMapper _fullNameTypeMapper;

        public FullNameTypeMapperTest()
        {
            _fullNameTypeMapper = new FullNameTypeMapper();
        }

        [Theory]
        [MemberData(nameof(Mappings))]
        public void ToNameTest(Type type, string name)
        {
            var actualName = _fullNameTypeMapper.ToName(type);
            Assert.Equal(name, actualName);
        }

        [Theory]
        [MemberData(nameof(Mappings))]
        public void ToTypeTest(Type type, string name)
        {
            var actualType = _fullNameTypeMapper.ToType(name);
            Assert.Equal(type, actualType);
        }

        public static List<object[]> Mappings => new List<object[]>
        {
            new object[] { typeof(int), "System.Int32" },
            new object[] { typeof(IEnumerable<int>), "System.Collections.Generic.IEnumerable<System.Int32>" },
            new object[] { typeof(IEnumerable<IEnumerable<int>>), "System.Collections.Generic.IEnumerable<System.Collections.Generic.IEnumerable<System.Int32>>" },
            new object[] { typeof(IDictionary<int, string>), "System.Collections.Generic.IDictionary<System.Int32,System.String>" },
            new object[] { typeof(NestedClass), "BTDBTest.FullNameTypeMapperTest+NestedClass" },
            new object[] { typeof(NestedClass.NestedClass2), "BTDBTest.FullNameTypeMapperTest+NestedClass+NestedClass2" },
            new object[] { typeof(Tuple<int, Tuple<float, double, decimal>, long>), "System.Tuple<System.Int32,System.Tuple<System.Single,System.Double,System.Decimal>,System.Int64>" },
            new object[] { typeof(Tuple<Tuple<short, ushort>, Tuple<int, uint>, Tuple<long, ulong>>), "System.Tuple<System.Tuple<System.Int16,System.UInt16>,System.Tuple<System.Int32,System.UInt32>,System.Tuple<System.Int64,System.UInt64>>" }
        };

        class NestedClass
        {
            internal class NestedClass2
            {
            }
        }
    }
}
