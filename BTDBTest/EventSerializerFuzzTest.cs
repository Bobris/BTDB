using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.Json;
using BTDB;
using Xunit;
using static BTDBTest.EventLayerRoundTripHelper;

namespace BTDBTest.EventSerializerFuzz;

public enum Small : byte { A = 1, B = 200 }
public enum Signed : long { Neg = -5, Big = long.MaxValue }

[Generate]
public class Child { public int X { get; set; } public string? S { get; set; } public Child? Next { get; set; } }

[Generate]
public class Rich
{
    public int I { get; set; }
    public long? NL { get; set; }
    public double D { get; set; }
    public float F { get; set; }
    public decimal M { get; set; }
    public decimal? NM { get; set; }
    public bool B { get; set; }
    public string? S { get; set; }
    public byte[]? Bytes { get; set; }
    public Guid G { get; set; }
    public Guid? NG { get; set; }
    public DateTime Dt { get; set; }
    public DateTimeOffset Dto { get; set; }
    public TimeSpan? Ts { get; set; }
    public Small E { get; set; }
    public Signed? NE { get; set; }
    public List<int?>? LNI { get; set; }
    public List<string?>? LS { get; set; }
    public List<List<int>?>? LL { get; set; }
    public int[]? Arr { get; set; }
    public Dictionary<string, int?>? DSI { get; set; }
    public Dictionary<int, List<string>?>? DIL { get; set; }
    public Dictionary<Guid, Child?>? DGC { get; set; }
    public HashSet<long>? HS { get; set; }
    public IList<Child?>? IC { get; set; }
    public IDictionary<string, string?>? ID { get; set; }
    public (int, string?) T2 { get; set; }
    public List<(int, string)>? LT { get; set; }
    public Child? C { get; set; }
    public Child? Same { get; set; }
    public object? O { get; set; }
    public List<byte[]?>? LB { get; set; }
    public ushort US { get; set; }
    public sbyte SB { get; set; }
    public Version? V { get; set; }
    public IPAddress? Ip { get; set; }
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
/// Random values of a class with most member shapes events support, round-tripped through every event layer with and
/// without emitted IL, compared as JSON, with an object referenced twice kept as one.
public class EventSerializerFuzzTest
{
    static string? Str(Random r) => r.Next(5) switch { 0 => null, 1 => "", 2 => "ž😀\0x", _ => new string('a', r.Next(1, 40)) };
    static T? Maybe<T>(Random r, Func<T> f) where T : class => r.Next(4) == 0 ? null : f();
    static Child Kid(Random r, int depth) => new() { X = r.Next(), S = Str(r), Next = depth < 3 && r.Next(2) == 0 ? Kid(r, depth + 1) : null };

    static Rich Make(Random r)
    {
        var v = new Rich
        {
            I = r.Next(int.MinValue, int.MaxValue), NL = r.Next(3) == 0 ? null : r.NextInt64(long.MinValue, long.MaxValue),
            D = r.Next(4) switch { 0 => double.NaN, 1 => -0.0, 2 => double.PositiveInfinity, _ => r.NextDouble() * 1e300 },
            F = (float)r.NextDouble(), M = r.Next(3) switch { 0 => decimal.MaxValue, 1 => -0.0001m, _ => (decimal)r.NextDouble() },
            NM = r.Next(2) == 0 ? null : 12.5m, B = r.Next(2) == 0, S = Str(r),
            Bytes = Maybe(r, () => Enumerable.Range(0, r.Next(5)).Select(i => (byte)i).ToArray()),
            G = Guid.NewGuid(), NG = r.Next(2) == 0 ? null : Guid.NewGuid(),
            Dt = new DateTime(r.NextInt64(0, DateTime.MaxValue.Ticks), (DateTimeKind)r.Next(3)),
            Dto = new DateTimeOffset(r.NextInt64(DateTime.MinValue.AddDays(2).Ticks, DateTime.MaxValue.AddDays(-2).Ticks), TimeSpan.FromMinutes(r.Next(-840, 840))),
            Ts = r.Next(2) == 0 ? null : TimeSpan.FromTicks(r.NextInt64(long.MinValue, long.MaxValue)),
            E = r.Next(2) == 0 ? Small.A : Small.B, NE = r.Next(3) switch { 0 => null, 1 => Signed.Neg, _ => Signed.Big },
            LNI = Maybe(r, () => Enumerable.Range(0, r.Next(5)).Select(i => r.Next(2) == 0 ? (int?)null : i).ToList()),
            LS = Maybe(r, () => Enumerable.Range(0, r.Next(5)).Select(_ => Str(r)).ToList()),
            LL = Maybe(r, () => Enumerable.Range(0, r.Next(4)).Select(_ => r.Next(3) == 0 ? null : Enumerable.Range(0, r.Next(3)).ToList()).ToList()),
            Arr = Maybe(r, () => Enumerable.Range(0, r.Next(5)).ToArray()),
            DSI = Maybe(r, () => Enumerable.Range(0, r.Next(4)).ToDictionary(i => "k" + i, i => r.Next(2) == 0 ? (int?)null : i)),
            DIL = Maybe(r, () => Enumerable.Range(0, r.Next(4)).ToDictionary(i => i, i => r.Next(2) == 0 ? null : new List<string> { "v" + i })),
            DGC = Maybe(r, () => Enumerable.Range(0, r.Next(3)).ToDictionary(_ => Guid.NewGuid(), _ => r.Next(2) == 0 ? null : Kid(r, 0))),
            HS = Maybe(r, () => Enumerable.Range(0, r.Next(5)).Select(i => (long)i * 1000).ToHashSet()),
            IC = Maybe(r, () => (IList<Child?>)Enumerable.Range(0, r.Next(3)).Select(_ => r.Next(3) == 0 ? null : Kid(r, 0)).ToList()),
            ID = Maybe(r, () => (IDictionary<string, string?>)Enumerable.Range(0, r.Next(3)).ToDictionary(i => "r" + i, _ => Str(r))),
            T2 = (r.Next(), Str(r)),
            LT = Maybe(r, () => Enumerable.Range(0, r.Next(3)).Select(i => (i, "t" + i)).ToList()),
            C = Maybe(r, () => Kid(r, 0)),
            O = r.Next(5) switch { 0 => null, 1 => Kid(r, 0), 2 => "str", 3 => new List<int> { 1 }, _ => 42L },
            LB = Maybe(r, () => Enumerable.Range(0, r.Next(3)).Select(i => r.Next(2) == 0 ? null : new byte[] { (byte)i }).ToList()),
            US = (ushort)r.Next(0, 65536), SB = (sbyte)r.Next(-128, 128),
            V = r.Next(3) switch { 0 => null, 1 => new Version(1, 2), _ => new Version(1, 2, 3, 4) },
            Ip = r.Next(3) switch { 0 => null, 1 => IPAddress.Parse("10.1.2.3"), _ => IPAddress.Parse("fe80::1%3") },
        };
        v.Same = r.Next(2) == 0 ? v.C : null;
        return v;
    }

    static readonly JsonSerializerOptions Options = new()
    {
        IncludeFields = true, NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowNamedFloatingPointLiterals,
        Converters = { new IpConverter(), new ObjectConverter() }
    };

    sealed class IpConverter : System.Text.Json.Serialization.JsonConverter<IPAddress>
    {
        public override IPAddress Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => throw new NotSupportedException();
        public override void Write(Utf8JsonWriter writer, IPAddress value, JsonSerializerOptions options) => writer.WriteStringValue(value + "/" + (value.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 ? value.ScopeId : 0));
    }

    sealed class ObjectConverter : System.Text.Json.Serialization.JsonConverter<object>
    {
        public override object Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => throw new NotSupportedException();
        public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
        {
            if (value is System.Collections.IEnumerable e and not string) { writer.WriteStartArray(); foreach (var i in e) writer.WriteStringValue(i?.ToString()); writer.WriteEndArray(); }
            else if (value is Child c) JsonSerializer.Serialize(writer, c, options);
            else writer.WriteStringValue(value.GetType().Name + ":" + value);
        }
    }

    [Theory]
    [InlineData(EventLayer.Store2, true)]
    [InlineData(EventLayer.Store1, true)]
    [InlineData(EventLayer.Mapping, true)]
    [InlineData(EventLayer.Store2, false)]
    [InlineData(EventLayer.Store1, false)]
    [InlineData(EventLayer.Mapping, false)]
    public void RandomValuesRoundTrip(EventLayer layer, bool noEmit)
    {
        var failures = new List<string>();
        RunWithEmitMode(noEmit, () => Body(layer, failures));
        Assert.True(failures.Count == 0, string.Join("\n", failures));
    }

    static void Body(EventLayer layer, List<string> failures)
    {
        for (var seed = 0; seed < 200 && failures.Count < 4; seed++)
        {
            var v = Make(new Random(seed));
            try
            {
                var r = (Rich)RoundTrip(layer, v)!;
                var a = JsonSerializer.Serialize(v, Options); var b = JsonSerializer.Serialize(r, Options);
                if (v.Same != null && !ReferenceEquals(r.Same, r.C)) failures.Add($"seed {seed}: identity lost");
                if (a != b)
                {
                    var i = 0; while (i < a.Length && i < b.Length && a[i] == b[i]) i++;
                    var st = Math.Max(0, i - 60);
                    failures.Add($"seed {seed}: ...{a.Substring(st, Math.Min(140, a.Length - st))}\n   vs ...{b.Substring(st, Math.Min(140, b.Length - st))}");
                }
            }
            catch (Exception e) { failures.Add($"seed {seed}: {e.GetType().Name} {e.Message.Split('\n')[0]}"); }
        }
    }
}
