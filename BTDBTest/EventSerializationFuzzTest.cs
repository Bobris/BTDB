using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using BTDB;
using BTDB.EventStore2Layer;
using BTDB.EventStoreLayer;
using Xunit;

namespace BTDBTest;

/// Random events over many field types, nested objects and collections survive a round trip through the event
/// serializer pair and through an event store file unchanged.
public class EventSerializationFuzzTest
{
    public enum FuzzColor
    {
        Blue = -3,
        Red = 0,
        Green = 5
    }

    [Flags]
    public enum FuzzFlags : ulong
    {
        None = 0,
        A = 1,
        High = 1ul << 63
    }

    [Generate]
    public class FuzzInner
    {
        public int A { get; set; }
        public string? S { get; set; }
        public List<FuzzInner>? Children { get; set; }
    }

    [Generate]
    public class FuzzEvent
    {
        public ulong Id { get; set; }
        public int I { get; set; }
        public long L { get; set; }
        public short Sh { get; set; }
        public byte B { get; set; }
        public sbyte Sb { get; set; }
        public uint U { get; set; }
        public ushort Us { get; set; }
        public bool Bo { get; set; }
        public double D { get; set; }
        public float F { get; set; }
        public decimal M { get; set; }
        public string? S { get; set; }
        public Guid G { get; set; }
        public DateTime Dt { get; set; }
        public TimeSpan Ts { get; set; }
        public byte[]? Bytes { get; set; }
        public FuzzColor C { get; set; }
        public FuzzFlags Fl { get; set; }
        public FuzzColor? NC { get; set; }
        public int? NI { get; set; }
        public List<int>? Li { get; set; }
        public List<string?>? Ls { get; set; }
        public IList<long?>? Ln { get; set; }
        public IDictionary<string, int>? Dict { get; set; }
        public Dictionary<int, FuzzInner?>? DictObj { get; set; }
        public FuzzInner? Obj { get; set; }
        public List<FuzzInner?>? Objs { get; set; }
        public HashSet<string>? Set { get; set; }
        public List<List<int>?>? Nested { get; set; }
    }

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 200).Select(seed => new object[] { seed });

    static readonly string[] Strings = ["", "a", "Žluťoučký kůň", "\0", "😀", new('x', 300), "￿"];

    static readonly double[] Doubles =
    [
        0, -0.0, 1.5, double.NaN, double.PositiveInfinity, double.NegativeInfinity, double.Epsilon, double.MaxValue,
        double.MinValue, -1e-300
    ];

    static readonly decimal[] Decimals =
        [0m, -0m, 1m, 1.0m, 1.00m, -1.5m, decimal.MaxValue, decimal.MinValue, 0.0000000000000000000000000001m, 79228162514264337593543950335m / 3];

    static string? RandomString(Random r) => r.Next(5) == 0 ? null : Strings[r.Next(Strings.Length)];

    static FuzzInner? RandomInner(Random r, int depth) => r.Next(4) == 0 || depth > 3
        ? null
        : new()
        {
            A = r.Next(int.MinValue, int.MaxValue), S = RandomString(r),
            Children = r.Next(3) == 0
                ? null
                : Enumerable.Range(0, r.Next(3)).Select(_ => RandomInner(r, depth + 1)).OfType<FuzzInner>().ToList()
        };

    static List<T>? RandomList<T>(Random r, Func<T> item) =>
        r.Next(4) == 0 ? null : Enumerable.Range(0, r.Next(0, 6)).Select(_ => item()).ToList();

    static FuzzEvent RandomEvent(Random r)
    {
        var bytes = new byte[8];
        long Long()
        {
            r.NextBytes(bytes);
            return r.Next(3) switch { 0 => BitConverter.ToInt64(bytes), 1 => r.Next(-300, 300), _ => long.MinValue };
        }

        return new()
        {
            Id = (ulong)Long(), I = (int)Long(), L = Long(), Sh = (short)Long(), B = (byte)Long(), Sb = (sbyte)Long(),
            U = (uint)Long(), Us = (ushort)Long(), Bo = r.Next(2) == 0,
            D = r.Next(2) == 0 ? Doubles[r.Next(Doubles.Length)] : BitConverter.Int64BitsToDouble(Long()),
            F = r.Next(2) == 0 ? (float)Doubles[r.Next(Doubles.Length)] : BitConverter.Int32BitsToSingle((int)Long()),
            M = Decimals[r.Next(Decimals.Length)], S = RandomString(r), G = r.Next(3) == 0 ? Guid.Empty : Guid.NewGuid(),
            Dt = new DateTime(r.NextInt64(DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks),
                (DateTimeKind)r.Next(3)),
            Ts = new TimeSpan(Long()), Bytes = r.Next(3) == 0 ? null : RandomBytes(r),
            C = new[] { FuzzColor.Blue, FuzzColor.Red, FuzzColor.Green }[r.Next(3)],
            Fl = (FuzzFlags)(ulong)r.Next(4) | (r.Next(2) == 0 ? FuzzFlags.High : FuzzFlags.None),
            NC = r.Next(2) == 0 ? null : FuzzColor.Green, NI = r.Next(2) == 0 ? null : (int)Long(),
            Li = RandomList(r, () => (int)Long()), Ls = RandomList(r, () => RandomString(r)),
            Ln = RandomList(r, () => r.Next(3) == 0 ? (long?)null : Long()),
            Dict = r.Next(4) == 0
                ? null
                : Enumerable.Range(0, r.Next(5)).Select(_ => RandomString(r) ?? "n").Distinct()
                    .ToDictionary(s => s, s => s.Length),
            DictObj = r.Next(4) == 0
                ? null
                : Enumerable.Range(0, r.Next(5)).Select(_ => r.Next(-5, 5)).Distinct()
                    .ToDictionary(k => k, _ => RandomInner(r, 1)),
            Obj = RandomInner(r, 0), Objs = RandomList(r, () => RandomInner(r, 1)),
            Set = r.Next(4) == 0
                ? null
                : Enumerable.Range(0, r.Next(0, 6)).Select(_ => RandomString(r) ?? "n").ToHashSet(),
            Nested = RandomList(r, () => RandomList(r, () => r.Next()))
        };
    }

    static byte[] RandomBytes(Random r)
    {
        var result = new byte[r.Next(0, 40)];
        r.NextBytes(result);
        return result;
    }

    /// A description that distinguishes every value the event should preserve, including the bits of floats and the
    /// scale of decimals.
    static string Dump(object? value)
    {
        var sb = new StringBuilder();
        Dump(sb, value);
        return sb.ToString();
    }

    static void Dump(StringBuilder sb, object? value)
    {
        switch (value)
        {
            case null:
                sb.Append("null");
                return;
            case string s:
                sb.Append('"').Append(s).Append('"');
                return;
            case double d:
                sb.Append("d").Append(BitConverter.DoubleToInt64Bits(d));
                return;
            case float f:
                sb.Append("f").Append(BitConverter.SingleToInt32Bits(f));
                return;
            case decimal m:
                sb.Append("m").Append(m.ToString(CultureInfo.InvariantCulture));
                return;
            case DateTime dt:
                sb.Append(dt.Ticks).Append(dt.Kind);
                return;
            case byte[] bytes:
                sb.Append(Convert.ToHexString(bytes));
                return;
            case IEnumerable dictionary when dictionary.GetType().GetInterfaces().Any(i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>)):
                sb.Append('{');
                foreach (var (k, v) in dictionary.Cast<object>().Select(e =>
                                 (Dump(e.GetType().GetProperty("Key")!.GetValue(e)),
                                     Dump(e.GetType().GetProperty("Value")!.GetValue(e))))
                             .OrderBy(e => e.Item1, StringComparer.Ordinal))
                    sb.Append(k).Append(':').Append(v).Append(',');
                sb.Append('}');
                return;
            case IEnumerable enumerable when value.GetType().IsGenericType &&
                                             value.GetType().GetGenericTypeDefinition() == typeof(HashSet<>):
                sb.Append("set[").AppendJoin(",", enumerable.Cast<object?>().Select(Dump).Order(StringComparer.Ordinal))
                    .Append(']');
                return;
            case IEnumerable enumerable:
                sb.Append('[').AppendJoin(",", enumerable.Cast<object?>().Select(Dump)).Append(']');
                return;
        }

        var type = value.GetType();
        if (type.IsPrimitive || type.IsEnum || value is Guid or TimeSpan)
        {
            sb.Append(Convert.ToString(value, CultureInfo.InvariantCulture));
            return;
        }

        sb.Append(type.Name).Append('(');
        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            sb.Append(property.Name).Append('=');
            Dump(sb, property.GetValue(value));
            sb.Append(';');
        }

        sb.Append(')');
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void EventSerializerPairRoundTripsRandomEvents(int seed)
    {
        var random = new Random(seed);
        var serializer = new EventSerializer();
        var deserializer = new EventDeserializer();
        for (var i = 0; i < 20; i++)
        {
            var original = RandomEvent(random);
            var data = serializer.Serialize(out var hasMetadata, original).ToAsyncSafe();
            if (hasMetadata)
            {
                serializer.ProcessMetadataLog(data);
                deserializer.ProcessMetadataLog(data);
                data = serializer.Serialize(out hasMetadata, original).ToAsyncSafe();
                Assert.False(hasMetadata);
            }

            Assert.True(deserializer.Deserialize(out var copy, data));
            Assert.Equal(Dump(original), Dump(copy));
        }
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void EventStoreRoundTripsRandomEvents(int seed)
    {
        var random = new Random(seed);
        var file = new MemoryEventFileStorage();
        var appender = new EventStoreManager().AppendToStore(file);
        var batches = Enumerable.Range(0, 5)
            .Select(_ => Enumerable.Range(0, random.Next(1, 5)).Select(_ => (object)RandomEvent(random)).ToArray())
            .ToList();
        foreach (var batch in batches) appender.Store(null, batch);
        var reader = new EventStoreManager().OpenReadOnlyStore(file);
        var observer = new EventStoreTest.StoringEventObserver();
        reader.ReadFromStartToEnd(observer);
        Assert.Equal(batches.Select(Dump), observer.Events.Select(Dump));
    }

    public enum EvoColorV1
    {
        Red,
        Green,
        Blue
    }

    public enum EvoColorV2
    {
        Red,
        Green,
        Blue,
        Purple
    }

    [Generate]
    public class EvoInnerV1
    {
        public int A { get; set; }
        public string? S { get; set; }
        public long Gone { get; set; }
    }

    [Generate]
    public class EvoInnerV2
    {
        public long A { get; set; }
        public string? S { get; set; }
        public int Added { get; set; }
    }

    [Generate]
    public class EvoV1
    {
        public int A { get; set; }
        public byte B { get; set; }
        public short C { get; set; }
        public uint D { get; set; }
        public sbyte E { get; set; }
        public int F { get; set; }
        public int? G { get; set; }
        public List<int>? H { get; set; }
        public List<int>? I { get; set; }
        public IDictionary<string, int>? J { get; set; }
        public EvoInnerV1? K { get; set; }
        public List<EvoInnerV1?>? L { get; set; }
        public EvoColorV1 M { get; set; }
        public string? N { get; set; }
        public long Gone { get; set; }
        public ushort O { get; set; }
        public float P { get; set; }
        public Dictionary<int, EvoInnerV1?>? T { get; set; }
        public List<List<int>?>? V { get; set; }
        public ulong W { get; set; }
    }

    [Generate]
    public class EvoV2
    {
        public long A { get; set; }
        public int B { get; set; }
        public long C { get; set; }
        public ulong D { get; set; }
        public int E { get; set; }
        public int? F { get; set; }
        public int G { get; set; }
        public List<long>? H { get; set; }
        public IList<int?>? I { get; set; }
        public Dictionary<string, long>? J { get; set; }
        public EvoInnerV2? K { get; set; }
        public List<EvoInnerV2?>? L { get; set; }
        public EvoColorV2 M { get; set; }
        public string? N { get; set; }
        public string? Added { get; set; }
        public uint O { get; set; }
        public double P { get; set; }
        public IDictionary<long, EvoInnerV2?>? T { get; set; }
        public List<List<long>?>? V { get; set; }
        public ulong? W { get; set; }
    }

    static EvoInnerV1? RandomEvoInner(Random r) => r.Next(4) == 0
        ? null
        : new() { A = r.Next(int.MinValue, int.MaxValue), S = RandomString(r), Gone = r.NextInt64() };

    static EvoV1 RandomEvo(Random r)
    {
        var e = RandomEvent(r);
        return new()
        {
            A = e.I, B = e.B, C = e.Sh, D = e.U, E = e.Sb, F = (int)e.L, G = e.NI, H = e.Li, I = RandomList(r, r.Next),
            J = e.Dict, K = RandomEvoInner(r), L = RandomList(r, () => RandomEvoInner(r)),
            M = (EvoColorV1)r.Next(3), N = e.S, Gone = e.L, O = e.Us, P = e.F,
            T = r.Next(4) == 0
                ? null
                : Enumerable.Range(0, r.Next(5)).Select(_ => r.Next(-5, 5)).Distinct()
                    .ToDictionary(k => k, _ => RandomEvoInner(r)),
            V = e.Nested, W = e.Id
        };
    }

    static EvoInnerV2? Upgrade(EvoInnerV1? v) => v == null ? null : new() { A = v.A, S = v.S };

    static EvoV2 Upgrade(EvoV1 v) => new()
    {
        A = v.A, B = v.B, C = v.C, D = v.D, E = v.E, F = v.F, G = v.G ?? 0, H = v.H?.Select(x => (long)x).ToList(),
        I = v.I?.Select(x => (int?)x).ToList(), J = v.J?.ToDictionary(p => p.Key, p => (long)p.Value),
        K = Upgrade(v.K), L = v.L?.Select(Upgrade).ToList(), M = (EvoColorV2)(int)v.M, N = v.N, O = v.O, P = v.P,
        T = v.T?.ToDictionary(p => (long)p.Key, p => Upgrade(p.Value)),
        V = v.V?.Select(l => l?.Select(x => (long)x).ToList()).ToList(), W = v.W
    };

    static ITypeNameMapper EvoMapper()
    {
        var parent = new FullNameTypeMapper();
        return new EventStoreTest.OverloadableTypeMapper(typeof(EvoV2), parent.ToName(typeof(EvoV1)),
            new EventStoreTest.OverloadableTypeMapper(typeof(EvoInnerV2), parent.ToName(typeof(EvoInnerV1)),
                new EventStoreTest.OverloadableTypeMapper(typeof(EvoColorV2), parent.ToName(typeof(EvoColorV1)),
                    parent)));
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void EventDeserializerUpgradesRandomEventsToANewVersion(int seed)
    {
        var random = new Random(seed);
        var serializer = new EventSerializer();
        var deserializer = new EventDeserializer(EvoMapper());
        for (var i = 0; i < 20; i++)
        {
            var original = RandomEvo(random);
            var data = serializer.Serialize(out var hasMetadata, original).ToAsyncSafe();
            if (hasMetadata)
            {
                serializer.ProcessMetadataLog(data);
                deserializer.ProcessMetadataLog(data);
                data = serializer.Serialize(out _, original).ToAsyncSafe();
            }

            Assert.True(deserializer.Deserialize(out var copy, data));
            Assert.IsType<EvoV2>(copy);
            Assert.Equal(Dump(Upgrade(original)), Dump(copy));
        }
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void EventStoreUpgradesRandomEventsToANewVersion(int seed)
    {
        var random = new Random(seed);
        var file = new MemoryEventFileStorage();
        var appender = new EventStoreManager().AppendToStore(file);
        var batches = Enumerable.Range(0, 5)
            .Select(_ => Enumerable.Range(0, random.Next(1, 5)).Select(_ => RandomEvo(random)).ToArray()).ToList();
        foreach (var batch in batches) appender.Store(null, batch.Cast<object>().ToArray());
        var manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(EvoMapper());
        var observer = new EventStoreTest.StoringEventObserver();
        manager.OpenReadOnlyStore(file).ReadFromStartToEnd(observer);
        Assert.Equal(batches.Select(b => Dump(b.Select(Upgrade).ToArray())), observer.Events.Select(Dump));
    }
}
