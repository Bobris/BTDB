using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.Json;
using BTDB;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

[Collection("IFieldHandler.UseNoEmitForRelations")]
/// Random rows with most member shapes relations support, stored, reopened and loaded with and without emitted IL,
/// compared as JSON. A null lazy IDictionary loads as an empty one, as lazy dictionaries always exist.
public class ObjectDbRelationFuzzTest : ObjectDbTestBase
{
    public ObjectDbRelationFuzzTest(ITestOutputHelper output) : base(output) { }

    public enum Small : byte { A = 1, B = 200 }

    public class Kid { public int X { get; set; } public string? S { get; set; } public Kid? Next { get; set; } }

    public class Row
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("Name")] public string? Name { get; set; }
        public int I { get; set; }
        public long? NL { get; set; }
        public double D { get; set; }
        public float F { get; set; }
        public decimal M { get; set; }
        public decimal? NM { get; set; }
        public bool B { get; set; }
        public byte[]? Bytes { get; set; }
        public Guid G { get; set; }
        public Guid? NG { get; set; }
        public DateTime Dt { get; set; }
        public DateTimeOffset Dto { get; set; }
        public TimeSpan? Ts { get; set; }
        public Small E { get; set; }
        public Small? NE { get; set; }
        public List<int?>? LNI { get; set; }
        public List<string?>? LS { get; set; }
        public List<List<int>?>? LL { get; set; }
        public Dictionary<string, int?>? DSI { get; set; }
        public Dictionary<int, List<string>?>? DIL { get; set; }
        public IDictionary<int, Kid?>? Lazy { get; set; }
        public IList<Kid?>? IC { get; set; }
        public (int, string?) T2 { get; set; }
        public Kid? C { get; set; }
        public List<byte[]?>? LB { get; set; }
        public Version? V { get; set; }
        public IPAddress? Ip { get; set; }
        public ushort US { get; set; }
        public sbyte SB { get; set; }
    }

    public interface IRowTable : IRelation<Row> { Row FindById(ulong id); IEnumerable<Row> ListByName(string? name); }

    static string? Str(Random r) => r.Next(5) switch { 0 => null, 1 => "", 2 => "ž😀\0x", _ => new string('a', r.Next(1, 40)) };
    static T? Maybe<T>(Random r, Func<T> f) where T : class => r.Next(4) == 0 ? null : f();
    static Kid MakeKid(Random r, int depth) => new() { X = r.Next(), S = Str(r), Next = depth < 3 && r.Next(2) == 0 ? MakeKid(r, depth + 1) : null };

    static Row Make(Random r, ulong id) => new()
    {
        Id = id, Name = Str(r), I = r.Next(int.MinValue, int.MaxValue), NL = r.Next(3) == 0 ? null : r.NextInt64(long.MinValue, long.MaxValue),
        D = r.Next(4) switch { 0 => double.NaN, 1 => -0.0, 2 => double.PositiveInfinity, _ => r.NextDouble() * 1e300 },
        F = (float)r.NextDouble(), M = r.Next(3) switch { 0 => decimal.MaxValue, 1 => -0.0001m, _ => (decimal)r.NextDouble() },
        NM = r.Next(2) == 0 ? null : 12.5m, B = r.Next(2) == 0,
        Bytes = Maybe(r, () => Enumerable.Range(0, r.Next(5)).Select(i => (byte)i).ToArray()),
        G = Guid.NewGuid(), NG = r.Next(2) == 0 ? null : Guid.NewGuid(),
        Dt = new DateTime(r.NextInt64(0, DateTime.MaxValue.Ticks), r.Next(2) == 0 ? DateTimeKind.Utc : DateTimeKind.Local),
        Dto = new DateTimeOffset(r.NextInt64(DateTime.MinValue.AddDays(2).Ticks, DateTime.MaxValue.AddDays(-2).Ticks), TimeSpan.FromMinutes(r.Next(-840, 840))),
        Ts = r.Next(2) == 0 ? null : TimeSpan.FromTicks(r.NextInt64(long.MinValue, long.MaxValue)),
        E = r.Next(2) == 0 ? Small.A : Small.B, NE = r.Next(3) switch { 0 => null, 1 => Small.A, _ => Small.B },
        LNI = Maybe(r, () => Enumerable.Range(0, r.Next(5)).Select(i => r.Next(2) == 0 ? (int?)null : i).ToList()),
        LS = Maybe(r, () => Enumerable.Range(0, r.Next(5)).Select(_ => Str(r)).ToList()),
        LL = Maybe(r, () => Enumerable.Range(0, r.Next(4)).Select(_ => r.Next(3) == 0 ? null : Enumerable.Range(0, r.Next(3)).ToList()).ToList()),
        DSI = Maybe(r, () => Enumerable.Range(0, r.Next(4)).ToDictionary(i => "k" + i, i => r.Next(2) == 0 ? (int?)null : i)),
        DIL = Maybe(r, () => Enumerable.Range(0, r.Next(4)).ToDictionary(i => i, i => r.Next(2) == 0 ? null : new List<string> { "v" + i })),
        Lazy = Maybe(r, () => (IDictionary<int, Kid?>)Enumerable.Range(0, r.Next(3)).ToDictionary(i => i, _ => r.Next(2) == 0 ? null : MakeKid(r, 0))),
        IC = Maybe(r, () => (IList<Kid?>)Enumerable.Range(0, r.Next(3)).Select(_ => r.Next(3) == 0 ? null : MakeKid(r, 0)).ToList()),
        T2 = (r.Next(), Str(r)), C = Maybe(r, () => MakeKid(r, 0)),
        LB = Maybe(r, () => Enumerable.Range(0, r.Next(3)).Select(i => r.Next(2) == 0 ? null : new byte[] { (byte)i }).ToList()),
        V = r.Next(3) switch { 0 => null, 1 => new Version(1, 2), _ => new Version(1, 2, 3, 4) },
        Ip = r.Next(3) switch { 0 => null, 1 => IPAddress.Parse("10.1.2.3"), _ => IPAddress.Parse("fe80::1%3") },
        US = (ushort)r.Next(0, 65536), SB = (sbyte)r.Next(-128, 128),
    };

    sealed class IpConverter : System.Text.Json.Serialization.JsonConverter<IPAddress>
    {
        public override IPAddress Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => throw new NotSupportedException();
        public override void Write(Utf8JsonWriter writer, IPAddress value, JsonSerializerOptions options) =>
            writer.WriteStringValue(value + "/" + (value.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 ? value.ScopeId : 0));
    }

    static readonly JsonSerializerOptions Options = new()
    {
        IncludeFields = true, NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowNamedFloatingPointLiterals,
        Converters = { new IpConverter() }
    };

    static string Json(Row row)
    {
        var lazy = row.Lazy;
        row.Lazy = null;
        var s = JsonSerializer.Serialize(row, Options) + "|" + JsonSerializer.Serialize((lazy ?? new Dictionary<int, Kid?>()).OrderBy(p => p.Key).ToList(), Options);
        row.Lazy = lazy;
        return s;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RandomRowsRoundTrip(bool noEmit)
    {
        var failures = new List<string>();
        EventLayerRoundTripHelper.RunWithEmitMode(noEmit, () =>
        {
            var expected = new Dictionary<ulong, string>();
            using (var tr = _db.StartTransaction())
            {
                var t = tr.GetRelation<IRowTable>();
                for (var seed = 0; seed < 200; seed++)
                {
                    var v = Make(new Random(seed), (ulong)seed);
                    expected[v.Id] = Json(v);
                    try { t.Upsert(v); } catch (Exception e) { failures.Add($"seed {seed} upsert: {e.GetType().Name} {e.Message.Split('\n')[0]}"); }
                }
                tr.Commit();
            }
            ReopenDb();
            using (var tr = _db.StartReadOnlyTransaction())
            {
                var t = tr.GetRelation<IRowTable>();
                foreach (var (id, a) in expected)
                {
                    if (failures.Count > 4) break;
                    try
                    {
                        var b = Json(t.FindById(id));
                        if (a != b)
                        {
                            var i = 0; while (i < a.Length && i < b.Length && a[i] == b[i]) i++;
                            var st = Math.Max(0, i - 60);
                            failures.Add($"id {id}: ...{a.Substring(st, Math.Min(140, a.Length - st))}\n   vs ...{b.Substring(st, Math.Min(140, b.Length - st))}");
                        }
                    }
                    catch (Exception e) { failures.Add($"id {id} load: {e.GetType().Name} {e.Message.Split('\n')[0]}"); }
                }
            }
        });
        Assert.True(failures.Count == 0, string.Join("\n", failures.Take(6)));
    }
}
