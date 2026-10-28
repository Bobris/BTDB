using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using BTDB;
using BTDB.Bon;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest.SerializationTests.BonSerializerFuzz;

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
    public bool B { get; set; }
    public string? S { get; set; }
    public byte[]? Bytes { get; set; }
    public Guid G { get; set; }
    public Guid? NG { get; set; }
    public DateTime Dt { get; set; }
    public Small E { get; set; }
    public Signed? NE { get; set; }
    public List<int?>? LNI { get; set; }
    public List<string?>? LS { get; set; }
    public List<List<int>?>? LL { get; set; }
    public int[]? Arr { get; set; }
    public string?[]? SArr { get; set; }
    public Dictionary<string, int?>? DSI { get; set; }
    public Dictionary<int, List<string>?>? DIL { get; set; }
    public Dictionary<Guid, Child?>? DGC { get; set; }
    public HashSet<long>? HS { get; set; }
    public IList<Child?>? IC { get; set; }
    public IReadOnlyDictionary<string, string?>? ROD { get; set; }
    public (int, string?) T2 { get; set; }
    public (long, Child?, double)? NT3 { get; set; }
    public List<(int, string)>? LT { get; set; }
    public Child? C { get; set; }
    public object? O { get; set; }
    public List<byte[]?>? LB { get; set; }
    public char Ch { get; set; }
    public ushort US { get; set; }
    public sbyte SB { get; set; }
    public IReadOnlyList<Child>? ROL { get; set; }
    public IEnumerable<int?>? EN { get; set; }
    public ISet<string>? ISS { get; set; }
    public Child?[]? CArr { get; set; }
    public Dictionary<string, object?>? DSO { get; set; }
    public List<object?>? LO { get; set; }
    public Tuple<int, string?>? TUP { get; set; }
    public Dictionary<int, List<(long, string?)>>? DLT { get; set; }
    public IReadOnlySet<int>? ROS { get; set; }
    public List<Small>? LE { get; set; }
    public Dictionary<Small, Signed?>? DE { get; set; }
}

/// Random values of a class with most member shapes Bon supports, serialized and deserialized, compared as JSON.
/// It found that a null class or tuple item failed a nullable tuple, and the stack overflow of recursive classes first
/// reached as a member type.
public class BonSerializerFuzzTest
{
    static string? Str(Random r) => r.Next(5) switch { 0 => null, 1 => "", 2 => "ž😀\0x", _ => new string('a', r.Next(1, 40)) };
    static T? Maybe<T>(Random r, Func<T> f) where T : class => r.Next(4) == 0 ? null : f();
    static Child Kid(Random r, int depth) => new() { X = r.Next(), S = Str(r), Next = depth < 3 && r.Next(2) == 0 ? Kid(r, depth + 1) : null };

    static Rich Make(Random r) => new()
    {
        I = r.Next(int.MinValue, int.MaxValue), NL = r.Next(3) == 0 ? null : r.NextInt64(long.MinValue, long.MaxValue),
        D = r.Next(4) switch { 0 => double.NaN, 1 => -0.0, 2 => double.PositiveInfinity, _ => r.NextDouble() * 1e300 },
        F = (float)r.NextDouble(), B = r.Next(2) == 0, S = Str(r),
        Bytes = Maybe(r, () => Enumerable.Range(0, r.Next(5)).Select(i => (byte)i).ToArray()),
        G = Guid.NewGuid(), NG = r.Next(2) == 0 ? null : Guid.NewGuid(),
        Dt = new DateTime(r.NextInt64(0, DateTime.MaxValue.Ticks), (DateTimeKind)r.Next(3)),
        E = r.Next(2) == 0 ? Small.A : Small.B, NE = r.Next(3) switch { 0 => null, 1 => Signed.Neg, _ => Signed.Big },
        LNI = Maybe(r, () => Enumerable.Range(0, r.Next(5)).Select(i => r.Next(2) == 0 ? (int?)null : i).ToList()),
        LS = Maybe(r, () => Enumerable.Range(0, r.Next(5)).Select(_ => Str(r)).ToList()),
        LL = Maybe(r, () => Enumerable.Range(0, r.Next(4)).Select(_ => r.Next(3) == 0 ? null : Enumerable.Range(0, r.Next(3)).ToList()).ToList()),
        Arr = Maybe(r, () => Enumerable.Range(0, r.Next(5)).ToArray()),
        SArr = Maybe(r, () => Enumerable.Range(0, r.Next(4)).Select(_ => Str(r)).ToArray()),
        DSI = Maybe(r, () => Enumerable.Range(0, r.Next(4)).ToDictionary(i => "k" + i, i => r.Next(2) == 0 ? (int?)null : i)),
        DIL = Maybe(r, () => Enumerable.Range(0, r.Next(4)).ToDictionary(i => i, i => r.Next(2) == 0 ? null : new List<string> { "v" + i })),
        DGC = Maybe(r, () => Enumerable.Range(0, r.Next(3)).ToDictionary(_ => Guid.NewGuid(), _ => r.Next(2) == 0 ? null : Kid(r, 0))),
        HS = Maybe(r, () => Enumerable.Range(0, r.Next(5)).Select(i => (long)i * 1000).ToHashSet()),
        IC = Maybe(r, () => (IList<Child?>)Enumerable.Range(0, r.Next(3)).Select(_ => r.Next(3) == 0 ? null : Kid(r, 0)).ToList()),
        ROD = Maybe(r, () => (IReadOnlyDictionary<string, string?>)Enumerable.Range(0, r.Next(3)).ToDictionary(i => "r" + i, _ => Str(r))),
        T2 = (r.Next(), Str(r)),
        NT3 = r.Next(2) == 0 ? null : (r.NextInt64(), r.Next(2) == 0 ? null : Kid(r, 0), r.NextDouble()),
        LT = Maybe(r, () => Enumerable.Range(0, r.Next(3)).Select(i => (i, "t" + i)).ToList()),
        C = Maybe(r, () => Kid(r, 0)),
        O = r.Next(4) switch { 0 => null, 1 => Kid(r, 0), 2 => "str", _ => 42L },
        LB = Maybe(r, () => Enumerable.Range(0, r.Next(3)).Select(i => r.Next(2) == 0 ? null : new byte[] { (byte)i }).ToList()),
        Ch = (char)r.Next(0, 0xD7FF), US = (ushort)r.Next(0, 65536), SB = (sbyte)r.Next(-128, 128),
        ROL = Maybe(r, () => (IReadOnlyList<Child>)Enumerable.Range(0, r.Next(3)).Select(_ => Kid(r, 0)).ToArray()),
        EN = Maybe(r, () => (IEnumerable<int?>)Enumerable.Range(0, r.Next(3)).Select(i => r.Next(2) == 0 ? (int?)null : i).ToList()),
        ISS = Maybe(r, () => (ISet<string>)Enumerable.Range(0, r.Next(3)).Select(i => "s" + i).ToHashSet()),
        CArr = Maybe(r, () => Enumerable.Range(0, r.Next(3)).Select(_ => r.Next(2) == 0 ? null : Kid(r, 0)).ToArray()),
        DSO = Maybe(r, () => Enumerable.Range(0, r.Next(3)).ToDictionary(i => "o" + i, i => r.Next(3) switch { 0 => null, 1 => (object)"x", _ => Kid(r, 0) })),
        LO = Maybe(r, () => Enumerable.Range(0, r.Next(3)).Select(i => r.Next(3) switch { 0 => null, 1 => (object)"y", _ => Kid(r, 0) }).ToList()),
        TUP = Maybe(r, () => Tuple.Create(r.Next(), Str(r))),
        DLT = Maybe(r, () => Enumerable.Range(0, r.Next(3)).ToDictionary(i => i, i => Enumerable.Range(0, r.Next(3)).Select(j => ((long)j, Str(r))).ToList())),
        ROS = Maybe(r, () => (IReadOnlySet<int>)Enumerable.Range(0, r.Next(4)).ToHashSet()),
        LE = Maybe(r, () => Enumerable.Range(0, r.Next(3)).Select(i => i % 2 == 0 ? Small.A : Small.B).ToList()),
        DE = Maybe(r, () => new Dictionary<Small, Signed?> { [Small.A] = r.Next(2) == 0 ? null : Signed.Neg })
    };

    static readonly JsonSerializerOptions Options = new() { IncludeFields = true, NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowNamedFloatingPointLiterals };

    static string Json(Rich v)
    {
        var (o, dso, lo) = (v.O, v.DSO, v.LO);
        (v.O, v.DSO, v.LO) = (null, null, null);
        static string Obj(object? x) => x is Child c ? JsonSerializer.Serialize(c, Options) : x?.ToString() ?? "null";
        var s = JsonSerializer.Serialize(v, Options) + "|" + Obj(o) + "|" +
                (dso == null ? "null" : string.Join(",", dso.Select(p => p.Key + "=" + Obj(p.Value)))) + "|" +
                (lo == null ? "null" : string.Join(",", lo.Select(Obj)));
        (v.O, v.DSO, v.LO) = (o, dso, lo);
        return s;
    }

    [Fact]
    public void RandomValuesRoundTrip()
    {
        var failures = new List<string>();
        for (var seed = 0; seed < 300 && failures.Count < 5; seed++)
        {
            var v = Make(new Random(seed));
            try
            {
                var builder = new BonBuilder();
                BonSerializerFactory.Serialize(ref builder, v);
                var bon = new Bon(builder.FinishAsMemory());
                var r = (Rich)BonSerializerFactory.Deserialize(ref bon)!;
                var a = Json(v); var b = Json(r);
                if (a != b)
                {
                    var i = 0; while (i < a.Length && i < b.Length && a[i] == b[i]) i++;
                    failures.Add($"seed {seed}: ...{a.Substring(Math.Max(0, i - 60), Math.Min(120, a.Length - Math.Max(0, i - 60)))}\n   vs ...{b.Substring(Math.Max(0, i - 60), Math.Min(120, b.Length - Math.Max(0, i - 60)))}");
                }
            }
            catch (Exception e) { failures.Add($"seed {seed}: {e.GetType().Name} {e.Message}"); }
        }
        Assert.True(failures.Count == 0, string.Join("\n", failures));
    }
}
