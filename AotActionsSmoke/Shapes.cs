using System.Globalization;
using BTDB;
using BTDB.BackendActions;

namespace AotActionsSmoke;

public enum Shade : short
{
    Dark = -1,
    Light = 7
}

[Flags]
public enum Rights : byte
{
    None = 0,
    Read = 1,
    Write = 2
}

[Generate]
public class Detail
{
    public string Label { get; set; } = "";
    public int? Score { get; set; }
    public DateTime When { get; set; }
}

/// <summary>Every field shape the serializers support; it travels in events and is stored inline in ObjectDB.</summary>
[Generate]
public class Payload
{
    public sbyte SByte { get; set; }
    public byte Byte { get; set; }
    public short Short { get; set; }
    public ushort UShort { get; set; }
    public int Int { get; set; }
    public uint UInt { get; set; }
    public long Long { get; set; }
    public ulong ULong { get; set; }
    public bool Bool { get; set; }
    public float Float { get; set; }
    public double Double { get; set; }
    public decimal Decimal { get; set; }
    public string? String { get; set; }
    public Guid Guid { get; set; }
    public DateTime DateTime { get; set; }
    public DateTimeOffset DateTimeOffset { get; set; }
    public TimeSpan TimeSpan { get; set; }
    public Version? Version { get; set; }
    public byte[]? Bytes { get; set; }
    public Shade Shade { get; set; }
    public Rights Rights { get; set; }
    public Shade? MaybeShade { get; set; }
    public int? MaybeInt { get; set; }
    public long? MaybeLong { get; set; }
    public double? MaybeDouble { get; set; }
    public decimal? MaybeDecimal { get; set; }
    public bool? MaybeBool { get; set; }
    public Guid? MaybeGuid { get; set; }
    public DateTime? MaybeDateTime { get; set; }
    public TimeSpan? MaybeTimeSpan { get; set; }
    public int? MissingInt { get; set; }
    public List<int>? Ints { get; set; }
    public List<string>? Strings { get; set; }
    public List<DateTime>? Dates { get; set; }
    public List<int?>? MaybeInts { get; set; }
    public List<Shade>? Shades { get; set; }
    public List<Detail>? Details { get; set; }
    public IList<long>? Longs { get; set; }
    public Dictionary<string, int>? Counts { get; set; }
    public Dictionary<ulong, Detail>? DetailsById { get; set; }
    public Dictionary<Guid, DateTime>? DatesById { get; set; }
    public Dictionary<int, string>? Names { get; set; }
    public Dictionary<string, List<int>>? Groups { get; set; }
    public (int, string) Pair { get; set; }
    public (long, DateTime, bool) Triple { get; set; }
    public Detail? Nested { get; set; }
    public object? Polymorphic { get; set; }
}

public class Echo : ActionInput<EchoResult>
{
    public Payload Payload { get; set; } = new();
}

[Generate]
public class EchoResult
{
    public Payload Payload { get; set; } = new();
}

/// <summary>Returns its input, so the payload crosses both serializers both ways before OnFinished compares it.</summary>
public class EchoHandler : IActionHandler<Echo, EchoResult>
{
    public Task<EchoResult> ExecuteAsync(Echo input, IActionContext context) =>
        Task.FromResult(new EchoResult { Payload = input.Payload });

    public void OnFinished(Echo input, ActionOutcome<EchoResult> outcome)
    {
        var expected = Shapes.Describe(Shapes.Sample());
        var differences = Shapes.Differences(expected, Shapes.Describe(input.Payload), "input")
            .Concat(Shapes.Differences(expected, Shapes.Describe(outcome.Result?.Payload), "result")).ToList();
        Program.Outcomes.Enqueue(differences.Count == 0 ? $"echo {outcome.Kind} same"
            : $"echo {outcome.Kind} differs: " + string.Join("; ", differences));
    }
}

static class Shapes
{
    static readonly DateTime When = new(2026, 11, 1, 10, 30, 15, 123, DateTimeKind.Utc);

    internal static Payload Sample() => new()
    {
        SByte = -5, Byte = 200, Short = -30000, UShort = 60000, Int = -2_000_000_000, UInt = 4_000_000_000,
        Long = long.MinValue + 1, ULong = ulong.MaxValue - 1, Bool = true, Float = 1.5f, Double = -2.25e100,
        Decimal = 12345.6789m, String = "žluťoučký kůň", Guid = new("7c9e6679-7425-40de-944b-e07fc1f90ae7"),
        DateTime = When, DateTimeOffset = new(2026, 11, 1, 10, 30, 0, TimeSpan.FromHours(2)),
        TimeSpan = TimeSpan.FromMilliseconds(98765), Version = new(1, 2, 3, 4), Bytes = [1, 2, 3, 255],
        Shade = Shade.Dark, Rights = Rights.Read | Rights.Write, MaybeShade = Shade.Light, MaybeInt = 42,
        MaybeLong = -7, MaybeDouble = 0.125, MaybeDecimal = -0.5m, MaybeBool = false,
        MaybeGuid = new("00000000-0000-0000-0000-000000000001"), MaybeDateTime = When.AddDays(-1),
        MaybeTimeSpan = TimeSpan.FromHours(-3), MissingInt = null, Ints = [3, 1, 2], Strings = ["a", "", "c"],
        Dates = [When, When.AddYears(1)], MaybeInts = [1, null, 3], Shades = [Shade.Light, Shade.Dark],
        Details = [new() { Label = "one", Score = 1, When = When }, new() { Label = "two", When = When }],
        Longs = new List<long> { 10, 20 }, Counts = new() { ["x"] = 1, ["y"] = 2 },
        DetailsById = new() { [5] = new() { Label = "five", Score = 5, When = When } },
        DatesById = new() { [new("11111111-2222-3333-4444-555555555555")] = When },
        Names = new() { [1] = "first", [2] = "second" },
        Groups = new() { ["even"] = [2, 4], ["odd"] = [1] }, Pair = (7, "seven"), Triple = (8, When, true),
        Nested = new() { Label = "nested", Score = -1, When = When },
        Polymorphic = new Detail { Label = "polymorphic", When = When }
    };

    internal static string[] Describe(Payload? p)
    {
        if (p == null) return ["null"];
        var c = CultureInfo.InvariantCulture;
        return
        [
            $"SByte={p.SByte}", $"Byte={p.Byte}", $"Short={p.Short}", $"UShort={p.UShort}", $"Int={p.Int}",
            $"UInt={p.UInt}", $"Long={p.Long}", $"ULong={p.ULong}", $"Bool={p.Bool}", $"Float={p.Float.ToString("R", c)}",
            $"Double={p.Double.ToString("R", c)}", $"Decimal={p.Decimal.ToString(c)}", $"String={p.String}",
            $"Guid={p.Guid}", $"DateTime={p.DateTime:O}", $"DateTimeOffset={p.DateTimeOffset:O}",
            $"TimeSpan={p.TimeSpan}", $"Version={p.Version}", $"Bytes={Join(p.Bytes)}", $"Shade={p.Shade}",
            $"Rights={p.Rights}", $"MaybeShade={p.MaybeShade}", $"MaybeInt={p.MaybeInt}", $"MaybeLong={p.MaybeLong}",
            $"MaybeDouble={p.MaybeDouble?.ToString("R", c)}", $"MaybeDecimal={p.MaybeDecimal?.ToString(c)}",
            $"MaybeBool={p.MaybeBool}", $"MaybeGuid={p.MaybeGuid}", $"MaybeDateTime={p.MaybeDateTime:O}",
            $"MaybeTimeSpan={p.MaybeTimeSpan}", $"MissingInt={p.MissingInt?.ToString() ?? "null"}",
            $"Ints={Join(p.Ints)}", $"Strings={Join(p.Strings)}", $"Dates={Join(p.Dates?.Select(d => d.ToString("O")))}",
            $"MaybeInts={Join(p.MaybeInts?.Select(i => i?.ToString() ?? "null"))}", $"Shades={Join(p.Shades)}",
            $"Details={Join(p.Details?.Select(Describe))}", $"Longs={Join(p.Longs)}",
            $"Counts={Join(p.Counts?.OrderBy(e => e.Key, StringComparer.Ordinal).Select(e => e.Key + ":" + e.Value))}",
            $"DetailsById={Join(p.DetailsById?.OrderBy(e => e.Key).Select(e => e.Key + ":" + Describe(e.Value)))}",
            $"DatesById={Join(p.DatesById?.OrderBy(e => e.Key).Select(e => e.Key + ":" + e.Value.ToString("O")))}",
            $"Names={Join(p.Names?.OrderBy(e => e.Key).Select(e => e.Key + ":" + e.Value))}",
            $"Groups={Join(p.Groups?.OrderBy(e => e.Key, StringComparer.Ordinal).Select(e => e.Key + ":" + Join(e.Value)))}",
            $"Pair={p.Pair.Item1},{p.Pair.Item2}", $"Triple={p.Triple.Item1},{p.Triple.Item2:O},{p.Triple.Item3}",
            $"Nested={Describe(p.Nested)}", $"Polymorphic={(p.Polymorphic as Detail is { } d ? Describe(d) : p.Polymorphic?.GetType().Name ?? "null")}"
        ];
    }

    static string Describe(Detail? d) => d == null ? "null" : $"{d.Label}/{d.Score?.ToString() ?? "null"}/{d.When:O}/{d.When.Kind}";

    static string Join<T>(IEnumerable<T>? items) => items == null ? "null" : "[" + string.Join("|", items) + "]";

    internal static IEnumerable<string> Differences(string[] expected, string[] actual, string what) =>
        expected.Length != actual.Length ? [$"{what}: {string.Join(", ", actual)}"]
            : expected.Zip(actual).Where(p => p.First != p.Second).Select(p => $"{what} {p.Second} (expected {p.First})");
}
