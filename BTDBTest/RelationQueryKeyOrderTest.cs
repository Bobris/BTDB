using System;
using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Range seeks of relation queries rely on key encodings ordering bytes like values. This checks it for every field
/// handler the query planner treats as order preserving, using the handlers relations really use for keys.
public class RelationQueryKeyOrderTest : ObjectDbTestBase
{
    public RelationQueryKeyOrderTest(ITestOutputHelper output) : base(output)
    {
    }

    public enum SignedKind : short
    {
        Min = short.MinValue,
        Minus = -1,
        Zero = 0,
        Max = short.MaxValue
    }

    public enum UnsignedKind : uint
    {
        Zero = 0,
        Small = 1,
        Large = uint.MaxValue
    }

    [Generate]
    public class OrderedKeys
    {
        [PrimaryKey(1)] public ulong UInt64 { get; set; }
        [PrimaryKey(2)] public long Int64 { get; set; }
        [PrimaryKey(3)] public int Int32 { get; set; }
        [PrimaryKey(4)] public uint UInt32 { get; set; }
        [PrimaryKey(5)] public short Int16 { get; set; }
        [PrimaryKey(6)] public byte UInt8 { get; set; }
        [PrimaryKey(7)] public sbyte Int8 { get; set; }
        [PrimaryKey(8)] public bool Bool { get; set; }
        [PrimaryKey(9)] public SignedKind Signed { get; set; }
        [PrimaryKey(10)] public UnsignedKind Unsigned { get; set; }
    }

    public interface IOrderedKeysTable : IRelation<OrderedKeys>
    {
    }

    [Fact]
    public void OrderPreservingKeyHandlersOrderBytesLikeValues()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IOrderedKeysTable>();
        var relationInfo = ((IRelationDbManipulator)table).RelationInfo;
        var random = new Random(1);
        var checkedFields = new List<string>();
        foreach (var field in relationInfo.ClientRelationVersionInfo.PrimaryKeyFields.Span)
        {
            Assert.True(RelationQueryKeyRanges.IsOrderPreserving(field.Handler!),
                $"{field.Name} uses {field.Handler!.Name}");
            var type = typeof(OrderedKeys).GetProperty(field.Name)!.PropertyType;
            var values = Values(type, random);
            var save = field.Handler!.Save(type, relationInfo.TypeConverterFactory);
            var encoded = values.Select(v => Encode(save, type, v)).ToArray();
            for (var i = 0; i < values.Length; i++)
            for (var j = 0; j < values.Length; j++)
            {
                var valueOrder = Math.Sign(Comparer<object>.Default.Compare(values[i], values[j]));
                var byteOrder = Math.Sign(encoded[i].AsSpan().SequenceCompareTo(encoded[j]));
                Assert.True(valueOrder == byteOrder,
                    $"{field.Name}: {values[i]} vs {values[j]} orders values {valueOrder} but bytes {byteOrder}");
            }

            checkedFields.Add(field.Name);
        }

        Assert.Equal(10, checkedFields.Count);
    }

    static object[] Values(Type type, Random random)
    {
        var integers = new List<long> { 0, 1, -1, 63, 64, 127, 128, 255, 256, 8191, 8192, 16383, 16384, 65535, 65536 };
        for (var shift = 7; shift < 63; shift += 7)
        {
            integers.Add(1L << shift);
            integers.Add((1L << shift) - 1);
            integers.Add(-(1L << shift));
            integers.Add(-(1L << shift) - 1);
        }

        for (var i = 0; i < 200; i++) integers.Add(random.NextInt64(long.MinValue, long.MaxValue) >> random.Next(64));
        integers.Add(long.MinValue);
        integers.Add(long.MaxValue);
        if (type == typeof(ulong))
            return integers.Select(v => (object)unchecked((ulong)v)).Append(ulong.MaxValue).Distinct().ToArray();
        if (type == typeof(long)) return integers.Select(v => (object)v).Distinct().ToArray();
        if (type == typeof(int)) return integers.Select(v => (object)unchecked((int)v)).Distinct().ToArray();
        if (type == typeof(uint)) return integers.Select(v => (object)unchecked((uint)v)).Distinct().ToArray();
        if (type == typeof(short)) return integers.Select(v => (object)unchecked((short)v)).Distinct().ToArray();
        if (type == typeof(byte)) return Enumerable.Range(0, 256).Select(v => (object)(byte)v).ToArray();
        if (type == typeof(sbyte)) return Enumerable.Range(-128, 256).Select(v => (object)(sbyte)v).ToArray();
        if (type == typeof(bool)) return [false, true];
        if (type == typeof(SignedKind)) return Enum.GetValues<SignedKind>().Select(v => (object)v).ToArray();
        if (type == typeof(UnsignedKind)) return Enum.GetValues<UnsignedKind>().Select(v => (object)v).ToArray();
        throw new NotSupportedException(type.Name);
    }

    static byte[] Encode(FieldHandlerSave save, Type type, object value)
    {
        var writer = new MemWriter();
        var storage = new byte[16];
        if (type.IsEnum) value = Convert.ChangeType(value, Enum.GetUnderlyingType(type));
        switch (value)
        {
            case ulong v: BitConverter.TryWriteBytes(storage, v); break;
            case long v: BitConverter.TryWriteBytes(storage, v); break;
            case int v: BitConverter.TryWriteBytes(storage, v); break;
            case uint v: BitConverter.TryWriteBytes(storage, v); break;
            case short v: BitConverter.TryWriteBytes(storage, v); break;
            case byte v: storage[0] = v; break;
            case sbyte v: storage[0] = (byte)v; break;
            case bool v: storage[0] = v ? (byte)1 : (byte)0; break;
            default: throw new NotSupportedException(value.GetType().Name);
        }

        save(ref writer, null, ref storage[0]);
        return writer.GetSpan().ToArray();
    }
}
