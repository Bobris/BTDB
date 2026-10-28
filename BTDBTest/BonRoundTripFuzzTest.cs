using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using BTDB.Bon;
using Xunit;

namespace BTDBTest;

/// Random value trees of every BON type, written with BonBuilder and read back with Bon: arrays by index in random
/// order (also past 32 items, where offsets are split), objects by key iteration and by lookup, classes, tuples and
/// dictionaries. Numbers are compared by value, as the builder may pick a smaller encoding.
public class BonRoundTripFuzzTest
{
    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 500).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void RandomValuesRoundTrip(int seed)
    {
        var random = new Random(seed);
        var builder = new BonBuilder();
        var expected = Write(ref builder, random, 0);
        var bon = new Bon(builder.FinishAsMemory());
        Assert.Equal(expected, Read(ref bon, random));
    }

    static string RandomString(Random random)
    {
        var length = random.Next(0, 6);
        var sb = new StringBuilder();
        for (var i = 0; i < length; i++)
            sb.Append(random.Next(5) switch
            {
                0 => "ž",
                1 => "😀",
                2 => "\0",
                _ => ((char)('a' + random.Next(3))).ToString()
            });
        return sb.ToString();
    }

    static string Number(double value) => Math.Abs(value) < 9e15 && value == Math.Floor(value)
        ? "n:" + ((long)value).ToString(CultureInfo.InvariantCulture)
        : "n:" + value.ToString("R", CultureInfo.InvariantCulture);

    static string Write(ref BonBuilder builder, Random random, int depth)
    {
        var kind = depth > 3 ? random.Next(11) : random.Next(16);
        switch (kind)
        {
            case 0:
                builder.WriteNull();
                return "null";
            case 1:
                builder.WriteUndefined();
                return "undefined";
            case 2:
            {
                var v = random.Next(2) == 0;
                builder.Write(v);
                return v ? "true" : "false";
            }
            case 3:
            {
                var v = random.Next(4) switch
                {
                    0 => random.Next(-12, 12), 1 => long.MinValue, 2 => long.MaxValue, _ => random.NextInt64()
                };
                builder.Write(v);
                return "n:" + v.ToString(CultureInfo.InvariantCulture);
            }
            case 4:
            {
                var v = random.Next(3) switch { 0 => (ulong)random.Next(20), 1 => ulong.MaxValue, _ => (ulong)random.NextInt64() * 2 };
                builder.Write(v);
                return "n:" + v.ToString(CultureInfo.InvariantCulture);
            }
            case 5:
            {
                var v = random.Next(5) switch
                {
                    0 => 0.5, 1 => -1.25, 2 => random.NextDouble() * 1e6, 3 => 3.0, _ => double.MaxValue
                };
                builder.Write(v);
                return Number(v);
            }
            case 6:
            case 7:
            {
                var v = RandomString(random);
                builder.Write(v);
                return "s:" + v;
            }
            case 8:
            {
                var v = new DateTime(random.NextInt64(DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks), DateTimeKind.Utc);
                builder.Write(v);
                return "d:" + v.Ticks;
            }
            case 9:
            {
                var bytes = new byte[16];
                random.NextBytes(bytes);
                var v = new Guid(bytes);
                builder.Write(v);
                return "g:" + v;
            }
            case 10:
            {
                var v = new byte[random.Next(0, 5)];
                random.NextBytes(v);
                builder.Write(v);
                return "b:" + Convert.ToHexString(v);
            }
            case 11:
            case 12:
            {
                var count = random.Next(4) == 0 ? random.Next(30, 80) : random.Next(0, 5);
                builder.StartArray();
                var items = new List<string>();
                for (var i = 0; i < count; i++) items.Add(Write(ref builder, random, depth + 1));
                builder.FinishArray();
                return "[" + string.Join(",", items) + "]";
            }
            case 13:
            {
                var count = random.Next(1, 4);
                builder.StartTuple();
                var items = new List<string>();
                for (var i = 0; i < count; i++) items.Add(Write(ref builder, random, depth + 1));
                builder.FinishTuple();
                return "(" + string.Join(",", items) + ")";
            }
            case 14:
            {
                var asClass = random.Next(2) == 0;
                var name = "C" + random.Next(3);
                if (asClass) builder.StartClass(name);
                else builder.StartObject();
                var keys = new List<string>();
                var items = new List<string>();
                for (var i = random.Next(0, 5); i > 0; i--)
                {
                    var key = RandomString(random) + i;
                    keys.Add(key);
                    builder.WriteKey(key);
                    items.Add(key + "=" + Write(ref builder, random, depth + 1));
                }

                if (asClass) builder.FinishClass();
                else builder.FinishObject();
                return (asClass ? name : "") + "{" + string.Join(",", items) + "}";
            }
            default:
            {
                builder.StartDictionary();
                var items = new List<string>();
                for (var i = random.Next(0, 4); i > 0; i--)
                {
                    var key = Write(ref builder, random, depth + 2);
                    items.Add(key + ":" + Write(ref builder, random, depth + 1));
                }

                builder.FinishDictionary();
                return "<" + string.Join(",", items) + ">";
            }
        }
    }

    static string Read(ref Bon bon, Random random)
    {
        switch (bon.BonType)
        {
            case BonType.Null:
                Assert.True(bon.TryGetNull());
                return "null";
            case BonType.Undefined:
                Assert.True(bon.TryGetUndefined());
                return "undefined";
            case BonType.Bool:
                Assert.True(bon.TryGetBool(out var b));
                return b ? "true" : "false";
            case BonType.Integer:
                // TryGetLong casts unsigned values above long.MaxValue, so non-negative values are read unsigned first,
                // as BonSerializer and DumpToJson do.
                if (bon.TryGetULong(out var u)) return "n:" + u.ToString(CultureInfo.InvariantCulture);
                Assert.True(bon.TryGetLong(out var l));
                return "n:" + l.ToString(CultureInfo.InvariantCulture);
            case BonType.Float:
                Assert.True(bon.TryGetDouble(out var d));
                return Number(d);
            case BonType.String:
                Assert.True(bon.TryGetString(out var s));
                return "s:" + s;
            case BonType.DateTime:
                Assert.True(bon.TryGetDateTime(out var dt));
                return "d:" + dt.Ticks;
            case BonType.Guid:
                Assert.True(bon.TryGetGuid(out var g));
                return "g:" + g;
            case BonType.ByteArray:
                Assert.True(bon.TryGetByteArray(out var bytes));
                return "b:" + Convert.ToHexString(bytes);
            case BonType.Array:
            case BonType.Tuple:
            {
                var tuple = bon.BonType == BonType.Tuple;
                ArrayBon array;
                Assert.True(tuple ? bon.TryGetTuple(out array) : bon.TryGetArray(out array));
                var items = new string[array.Items];
                foreach (var i in Enumerable.Range(0, (int)array.Items).OrderBy(_ => random.Next()))
                {
                    Assert.True(array.TryGet((uint)i, out var item));
                    items[i] = Read(ref item, random);
                }

                Assert.False(array.TryGet(array.Items, out _));
                return tuple ? "(" + string.Join(",", items) + ")" : "[" + string.Join(",", items) + "]";
            }
            case BonType.Object:
            case BonType.Class:
            {
                var isClass = bon.BonType == BonType.Class;
                KeyedBon keyed;
                var name = "";
                if (isClass)
                {
                    Assert.True(bon.TryGetClass(out keyed, out var nameBytes));
                    name = Encoding.UTF8.GetString(nameBytes);
                }
                else Assert.True(bon.TryGetObject(out keyed));

                var keys = new List<string>();
                while (keyed.NextKey() is { } key) keys.Add(key);
                var values = keyed.Values();
                var items = new List<string>();
                foreach (var key in keys)
                {
                    var value = Read(ref values, random);
                    items.Add(key + "=" + value);
                    Assert.True(keyed.TryGet(key, out var byKey));
                    Assert.Equal(value, Read(ref byKey, random));
                }

                return name + "{" + string.Join(",", items) + "}";
            }
            case BonType.Dictionary:
            {
                Assert.True(bon.TryGetDictionary(out var pairs));
                var items = new List<string>();
                while (pairs.Items > 0)
                {
                    var key = Read(ref pairs, random);
                    items.Add(key + ":" + Read(ref pairs, random));
                }

                return "<" + string.Join(",", items) + ">";
            }
            default:
                throw new InvalidOperationException($"Unexpected {bon.BonType}");
        }
    }
}
