using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using BTDB;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.EventStoreLayer;
using Microsoft.Extensions.Primitives;
using Xunit;

namespace BTDBTest;

/// Catalog of round-trip scenarios for EventStoreLayer and EventStore2Layer.
/// They are executed by EventLayerScenarioNoEmitTest and EventLayerScenarioEmitTest.
public static class EventLayerScenarios
{
    public record Scenario(
        string Name,
        Func<object> Create,
        Func<RenamingTypeMapper>? WriteMapper = null,
        Func<RenamingTypeMapper>? ReadMapper = null,
        Action<object, object?>? Verify = null,
        bool NeedsCipher = false);

    const string Prefix = "EventLayerScenarios.";

    public enum ByteEnum : byte
    {
        A = 1,
        B = 200
    }

    public enum SByteEnum : sbyte
    {
        M = -5,
        P = 7
    }

    public enum ShortEnum : short
    {
        M = -300,
        P = 300
    }

    public enum UShortEnum : ushort
    {
        A = 1,
        B = 60000
    }

    public enum IntEnum
    {
        Zero,
        One,
        Two
    }

    public enum IntEnumV2
    {
        Zero,
        One,
        Two,
        Three
    }

    public enum UIntEnum : uint
    {
        A = 1,
        Big = 4000000000
    }

    public enum LongEnum : long
    {
        M = -5000000000,
        P = 5000000000
    }

    public enum ULongEnum : ulong
    {
        A = 1,
        Big = 18000000000000000000
    }

    [Flags]
    public enum FlagsEnum
    {
        None = 0,
        A = 1,
        B = 2,
        C = 4
    }

    [Generate]
    public class Item
    {
        public string? Name { get; set; }
        public int Value { get; set; }
    }

    [Generate]
    public class ItemV2
    {
        public string? Name { get; set; }
        public long Value { get; set; }
        public string? Extra { get; set; }
    }

    [Generate]
    public class DerivedItem : Item
    {
        public double Weight { get; set; }
    }

    [Generate]
    public class Primitives
    {
        public string? Str { get; set; }
        public string? NullStr { get; set; }
        public bool Bool { get; set; }
        public byte U8 { get; set; }
        public sbyte I8 { get; set; }
        public short I16 { get; set; }
        public ushort U16 { get; set; }
        public int I32 { get; set; }
        public uint U32 { get; set; }
        public long I64 { get; set; }
        public ulong U64 { get; set; }
        public float F32 { get; set; }
        public double F64 { get; set; }
        public decimal Dec { get; set; }
        public DateTime Dt { get; set; }
        public DateTimeOffset Dto { get; set; }
        public TimeSpan Ts { get; set; }
        public Guid G { get; set; }
        public Version? Ver { get; set; }
        public IPAddress? Ip { get; set; }
        public StringValues Sv { get; set; }
    }

    [Generate]
    public class Enums
    {
        public ByteEnum B { get; set; }
        public SByteEnum SB { get; set; }
        public ShortEnum S { get; set; }
        public UShortEnum US { get; set; }
        public IntEnum I { get; set; }
        public UIntEnum UI { get; set; }
        public LongEnum L { get; set; }
        public ULongEnum UL { get; set; }
        public FlagsEnum F { get; set; }
        public FlagsEnum FZero { get; set; }
        public IntEnum Undefined { get; set; }
    }

    [Generate]
    public class Nullables
    {
        public int? Int { get; set; }
        public int? NullInt { get; set; }
        public long? Long { get; set; }
        public Guid? G { get; set; }
        public decimal? Dec { get; set; }
        public DateTime? Dt { get; set; }
        public bool? B { get; set; }
        public IntEnum? E { get; set; }
        public IntEnum? NullE { get; set; }
        public (int, string)? Tuple { get; set; }
        public (int, string)? NullTuple { get; set; }
        public List<int?>? List { get; set; }
        public Dictionary<int, double?>? Dict { get; set; }
    }

    [Generate]
    public class Lists
    {
        public List<int>? IntList { get; set; }
        public List<int>? EmptyList { get; set; }
        public List<int>? NullList { get; set; }
        public int[]? IntArray { get; set; }
        public HashSet<string>? Set { get; set; }
        public IList<string?>? IList { get; set; }
        public ISet<int>? ISet { get; set; }
        public ICollection<long>? Collection { get; set; }
        public IEnumerable<int>? Enumerable { get; set; }
        public List<List<int>>? Nested { get; set; }
        public List<Item?>? Items { get; set; }
        public Item[]? ItemArray { get; set; }
        public List<object?>? Objects { get; set; }
        public List<byte[]>? Blobs { get; set; }
        public List<IntEnum>? EnumList { get; set; }
        public List<string>? Strings { get; set; }
    }

    [Generate]
    public class Dictionaries
    {
        public Dictionary<int, string>? IntString { get; set; }
        public IDictionary<string, List<int>>? StringList { get; set; }
        public Dictionary<string, int?>? StringNullable { get; set; }
        public Dictionary<IntEnum, Item>? EnumItem { get; set; }
        public Dictionary<long, Dictionary<string, string>>? Nested { get; set; }
        public Dictionary<int, string>? Empty { get; set; }
        public Dictionary<int, string>? Null { get; set; }
        public Dictionary<string, object?>? Objects { get; set; }
        public Dictionary<Guid, decimal>? GuidDecimal { get; set; }
        public Dictionary<Item, int>? ItemKeys { get; set; }
        public Dictionary<int, (string, int)>? TupleValues { get; set; }
    }

    [Generate]
    public class Tuples
    {
        public (int, string) Pair { get; set; }
        public (long, Item) WithObject { get; set; }
        public Tuple<int, string>? RefTuple { get; set; }
        public (int, (string, bool)) NestedTuple { get; set; }
        public (int, int, int, int, int, int, int) Seven { get; set; }
        public List<(int, string)>? List { get; set; }
        public (int, object?) WithObj { get; set; }
        public Tuple<string, List<int>>? RefWithList { get; set; }
        public (IntEnum, Guid?) EnumAndNullable { get; set; }
    }

    [Generate]
    public class Node
    {
        public string? Name { get; set; }
        public Node? Next { get; set; }
        public object? Payload { get; set; }
    }

    [Generate]
    public class Graph
    {
        public Item? A { get; set; }
        public Item? B { get; set; }
        public object? Polymorphic { get; set; }
        public object? BoxedInt { get; set; }
        public object? BoxedEnum { get; set; }
        public object? Str { get; set; }
        public List<Item>? Shared { get; set; }
        public Node? Cycle { get; set; }
    }

    [Generate]
    public class Bytes
    {
        public byte[]? Array { get; set; }
        public byte[]? EmptyArray { get; set; }
        public byte[]? NullArray { get; set; }
        public ReadOnlyMemory<byte> Memory { get; set; }
        public ByteBuffer Buffer { get; set; }
    }

    [Generate]
    public class Secrets
    {
        public EncryptedString Secret { get; set; }
        public EncryptedString Empty { get; set; }
        public List<EncryptedString>? List { get; set; }
        public Dictionary<int, EncryptedString>? Dict { get; set; }
    }

    // Upgrade scenarios: Writer types and Reader types mapped to same name

    [Generate]
    public class Everything
    {
        public int Keep { get; set; }
        public string? S { get; set; }
        public int? N { get; set; }
        public int? NNull { get; set; }
        public IntEnum E { get; set; }
        public LongEnum LE { get; set; }
        public (int, string) T { get; set; }
        public Tuple<int, Item>? RT { get; set; }
        public List<int>? L { get; set; }
        public List<Item>? LI { get; set; }
        public Dictionary<int, string>? D { get; set; }
        public Dictionary<string, Item>? DI { get; set; }
        public Item? O { get; set; }
        public object? Obj { get; set; }
        public byte[]? B { get; set; }
        public EncryptedString Enc { get; set; }
        public Guid? NG { get; set; }
        public (int, string)? NT { get; set; }
        public List<(int, Item)>? LT { get; set; }
        public Dictionary<int, List<Item>>? DL { get; set; }
        public object? BoxedEnum { get; set; }
    }

    [Generate]
    public class OnlyKeep
    {
        public int Keep { get; set; }
    }

    [Generate]
    public class Narrow
    {
        public int I2L { get; set; }
        public int I2N { get; set; }
        public int? N2L { get; set; }
        public int? N2N { get; set; }
        public List<int>? L2A { get; set; }
        public List<int>? L2S { get; set; }
        public List<int>? L2C { get; set; }
        public int[]? A2L { get; set; }
        public Dictionary<int, string>? D2I { get; set; }
        public IntEnum E2I { get; set; }
        public IntEnum E2V2 { get; set; }
        public IntEnum? NE2NE { get; set; }
        public (int, string) T2T { get; set; }
        public (int, string) T2R { get; set; }
        public (int, string, bool) T2Less { get; set; }
        public (int, string) T2More { get; set; }
        public Item? I2V2 { get; set; }
        public List<Item>? LI2LV2 { get; set; }
        public Dictionary<int, Item>? DI2DV2 { get; set; }
        public uint U2L { get; set; }
        public List<int?>? LN2LL { get; set; }
    }

    [Generate]
    public class Wide
    {
        public long I2L { get; set; }
        public int? I2N { get; set; }
        public long? N2L { get; set; }
        public int? N2N { get; set; }
        public long[]? L2A { get; set; }
        public HashSet<long>? L2S { get; set; }
        public IList<int>? L2C { get; set; }
        public List<int>? A2L { get; set; }
        public IDictionary<long, string>? D2I { get; set; }
        public int E2I { get; set; }
        public IntEnumV2 E2V2 { get; set; }
        public IntEnumV2? NE2NE { get; set; }
        public (long, string) T2T { get; set; }
        public Tuple<long, string>? T2R { get; set; }
        public (int, string) T2Less { get; set; }
        public (int, string, bool) T2More { get; set; }
        public ItemV2? I2V2 { get; set; }
        public List<ItemV2>? LI2LV2 { get; set; }
        public Dictionary<int, ItemV2>? DI2DV2 { get; set; }
        public long U2L { get; set; }
        public List<long?>? LN2LL { get; set; }
    }

    [Generate]
    public class NarrowScalars
    {
        public int I2L { get; set; }
        public int I2N { get; set; }
        public int? N2L { get; set; }
        public int? N2N { get; set; }
        public IntEnum E2I { get; set; }
        public IntEnum E2V2 { get; set; }
        public IntEnum? NE2NE { get; set; }
        public (int, string) T2Less { get; set; }
        public (int, string) T2More { get; set; }
        public (int, string) T2R { get; set; }
        public Item? I2V2 { get; set; }
        public List<Item>? LI2LV2 { get; set; }
        public List<int>? L2C { get; set; }
        public int[]? A2L { get; set; }
    }

    [Generate]
    public class WideScalars
    {
        public long I2L { get; set; }
        public int? I2N { get; set; }
        public long? N2L { get; set; }
        public int? N2N { get; set; }
        public int E2I { get; set; }
        public IntEnumV2 E2V2 { get; set; }
        public IntEnumV2? NE2NE { get; set; }
        public (int, string, bool) T2More { get; set; }
        public (int, string) T2Less { get; set; }
        public Tuple<int, string>? T2R { get; set; }
        public ItemV2? I2V2 { get; set; }
        public List<ItemV2>? LI2LV2 { get; set; }
        public IList<int>? L2C { get; set; }
        public List<int>? A2L { get; set; }
    }

    static Item NewItem(string name, int value) => new() { Name = name, Value = value };

    public static Primitives CreatePrimitives() => new()
    {
        Str = "Hello \"world\"", NullStr = null, Bool = true, U8 = 250, I8 = -100, I16 = -30000, U16 = 65000,
        I32 = int.MinValue, U32 = uint.MaxValue, I64 = long.MinValue, U64 = ulong.MaxValue, F32 = 1.5f,
        F64 = -2.25, Dec = 123456.789m, Dt = new(2020, 2, 29, 13, 14, 15, DateTimeKind.Utc),
        Dto = new(2021, 1, 2, 3, 4, 5, TimeSpan.FromHours(2)), Ts = TimeSpan.FromMinutes(-90),
        G = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"), Ver = new(1, 2, 3, 4),
        Ip = IPAddress.Parse("10.20.30.40"), Sv = new(["a", "b"])
    };

    public static Enums CreateEnums() => new()
    {
        B = ByteEnum.B, SB = SByteEnum.M, S = ShortEnum.M, US = UShortEnum.B, I = IntEnum.Two, UI = UIntEnum.Big,
        L = LongEnum.M, UL = ULongEnum.Big, F = FlagsEnum.A | FlagsEnum.C, FZero = FlagsEnum.None,
        Undefined = (IntEnum)42
    };

    public static Nullables CreateNullables() => new()
    {
        Int = 42, Long = -1, G = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"), Dec = 1.25m,
        Dt = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc), B = false, E = IntEnum.One, Tuple = (7, "seven"),
        List = [1, null, 3], Dict = new() { [1] = 1.5, [2] = null }
    };

    public static Lists CreateLists() => new()
    {
        IntList = [1, 2, 3], EmptyList = [], IntArray = [4, 5], Set = ["x", "y"], IList = ["a", null, "c"],
        ISet = new HashSet<int> { 7, 8 }, Collection = new List<long> { 9, 10 }, Enumerable = new List<int> { 11 },
        Nested = [[1], [], [2, 3]], Items = [NewItem("a", 1), null, NewItem("b", 2)],
        ItemArray = [NewItem("c", 3)], Objects = [NewItem("d", 4), 5, "six", null, IntEnum.Two],
        Blobs = [[1, 2], []], EnumList = [IntEnum.One, IntEnum.Zero], Strings = ["s1", "s2"]
    };

    public static Dictionaries CreateDictionaries() => new()
    {
        IntString = new() { [1] = "one", [2] = null! }, StringList = new Dictionary<string, List<int>>
        {
            ["a"] = [1, 2], ["b"] = []
        },
        StringNullable = new() { ["x"] = 1, ["y"] = null },
        EnumItem = new() { [IntEnum.One] = NewItem("one", 1), [IntEnum.Two] = NewItem("two", 2) },
        Nested = new() { [5] = new() { ["k"] = "v" } }, Empty = new(),
        Objects = new() { ["i"] = 1, ["s"] = "str", ["o"] = NewItem("o", 0), ["n"] = null },
        GuidDecimal = new() { [Guid.Parse("00112233-4455-6677-8899-aabbccddeeff")] = 3.5m },
        ItemKeys = new() { [NewItem("key", 9)] = 9 },
        TupleValues = new() { [1] = ("a", 1) }
    };

    public static Tuples CreateTuples() => new()
    {
        Pair = (1, "one"), WithObject = (2, NewItem("two", 2)), RefTuple = Tuple.Create(3, "three"),
        NestedTuple = (4, ("four", true)), Seven = (1, 2, 3, 4, 5, 6, 7), List = [(5, "five"), (6, "six")],
        WithObj = (7, NewItem("seven", 7)), RefWithList = Tuple.Create("x", new List<int> { 1, 2 }),
        EnumAndNullable = (IntEnum.Two, Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"))
    };

    public static Graph CreateGraph()
    {
        var shared = NewItem("shared", 1);
        var n1 = new Node { Name = "n1" };
        var n2 = new Node { Name = "n2", Next = n1, Payload = shared };
        n1.Next = n2;
        return new()
        {
            A = shared, B = shared, Polymorphic = new DerivedItem { Name = "d", Value = 2, Weight = 1.5 },
            BoxedInt = 42, BoxedEnum = IntEnum.One, Str = "s", Shared = [shared, shared], Cycle = n1
        };
    }

    public static Bytes CreateBytes() => new()
    {
        Array = [1, 2, 3], EmptyArray = [], Memory = new byte[] { 4, 5 },
        Buffer = ByteBuffer.NewAsync(new byte[] { 6, 7, 8 })
    };

    public static Secrets CreateSecrets() => new()
    {
        Secret = "password", Empty = new(), List = ["a", "b"], Dict = new() { [1] = "one" }
    };

    static readonly Item SharedItem = NewItem("o", 5);

    public static Everything CreateEverything() => new()
    {
        Keep = 42, S = "s", N = 1, E = IntEnum.Two, LE = LongEnum.M, T = (1, "t"), RT = Tuple.Create(2, NewItem("rt", 2)),
        L = [1, 2], LI = [NewItem("li", 3)], D = new() { [1] = "d" }, DI = new() { ["k"] = NewItem("di", 4) },
        O = SharedItem, Obj = SharedItem, BoxedEnum = IntEnum.One, B = [1, 2], Enc = "secret",
        NG = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"), NT = (3, "nt"), LT = [(4, NewItem("lt", 7))],
        DL = new() { [5] = [NewItem("dl", 8)] }
    };

    public static Narrow CreateNarrow() => new()
    {
        I2L = -5, I2N = 6, N2L = 7, N2N = null, L2A = [1, 2], L2S = [3, 4], L2C = [5], A2L = [6, 7],
        D2I = new() { [1] = "one" }, E2I = IntEnum.Two, E2V2 = IntEnum.One, NE2NE = IntEnum.Two, T2T = (8, "eight"),
        T2R = (9, "nine"), T2Less = (10, "ten", true), T2More = (11, "eleven"), I2V2 = NewItem("v2", 12),
        LI2LV2 = [NewItem("l", 13)], DI2DV2 = new() { [14] = NewItem("d", 14) }, U2L = uint.MaxValue,
        LN2LL = [1, null]
    };

    public static NarrowScalars CreateNarrowScalars() => new()
    {
        I2L = -5, I2N = 6, N2L = 7, N2N = null, E2I = IntEnum.Two, E2V2 = IntEnum.One, NE2NE = IntEnum.Two,
        T2Less = (10, "ten"), T2More = (11, "eleven"), T2R = (12, "twelve"), I2V2 = NewItem("v2", 12),
        LI2LV2 = [NewItem("l", 13)], L2C = [5], A2L = [6, 7]
    };

    static string N(string typeName) => Prefix + typeName;

    static readonly ISymmetricCipher Cipher = new AesGcmSymmetricCipher(new byte[32]);

    public static ISymmetricCipher? CipherFor(Scenario s) => s.NeedsCipher ? Cipher : null;

    static RenamingTypeMapper HideAll(params string[] names)
    {
        var m = new RenamingTypeMapper();
        foreach (var name in names) m.Hide(name);
        return m;
    }

    static readonly string[] AllClassNames =
    [
        "Item", "DerivedItem", "Primitives", "Enums", "Nullables", "Lists", "Dictionaries", "Tuples", "Node", "Graph",
        "Bytes", "Secrets", "Everything"
    ];

    static readonly string[] AllEnumNames =
    [
        "ByteEnum", "SByteEnum", "ShortEnum", "UShortEnum", "IntEnum", "UIntEnum", "LongEnum", "ULongEnum",
        "FlagsEnum"
    ];

    static RenamingTypeMapper HideEverything() =>
        HideAll(AllClassNames.Concat(AllEnumNames).Select(n => typeof(EventLayerScenarios).FullName + "+" + n)
            .ToArray());

    public static readonly Dictionary<string, Scenario> All = Build().ToDictionary(s => s.Name);

    static IEnumerable<Scenario> Build()
    {
        yield return new("Primitives", CreatePrimitives);
        yield return new("Enums", CreateEnums);
        yield return new("Nullables", CreateNullables);
        yield return new("Lists", CreateLists);
        yield return new("Dictionaries", CreateDictionaries);
        yield return new("Tuples", CreateTuples);
        yield return new("Graph", CreateGraph, Verify: VerifyGraph);
        yield return new("Bytes", CreateBytes);
        yield return new("Secrets", CreateSecrets, NeedsCipher: true);
        yield return new("Everything", CreateEverything, NeedsCipher: true);

        // Dynamic reads (reader does not know any of the types)
        yield return new("DynamicPrimitives", CreatePrimitives, ReadMapper: HideEverything);
        yield return new("DynamicEnums", CreateEnums, ReadMapper: HideEverything);
        yield return new("DynamicNullables", CreateNullables, ReadMapper: HideEverything);
        yield return new("DynamicLists", CreateLists, ReadMapper: HideEverything);
        yield return new("DynamicDictionaries", CreateDictionaries, ReadMapper: HideEverything);
        yield return new("DynamicTuples", CreateTuples, ReadMapper: HideEverything);
        yield return new("DynamicGraph", CreateGraph, ReadMapper: HideEverything, Verify: VerifyDynamicGraph);
        yield return new("DynamicBytes", CreateBytes, ReadMapper: HideEverything);
        yield return new("DynamicSecrets", CreateSecrets, ReadMapper: HideEverything, NeedsCipher: true);
        yield return new("DynamicEverything", CreateEverything, ReadMapper: HideEverything, NeedsCipher: true);

        // Upgrade scenarios
        yield return new("RemovedFields", CreateEverything,
            () => new RenamingTypeMapper().Map(typeof(Everything), "Upgrade"),
            () => new RenamingTypeMapper().Map(typeof(OnlyKeep), "Upgrade"),
            (_, result) => Assert.Equal(42, Assert.IsType<OnlyKeep>(result).Keep), NeedsCipher: true);
        yield return new("ChangedFieldTypes", CreateNarrow,
            () => new RenamingTypeMapper().Map(typeof(Narrow), "Upgrade").Map(typeof(Item), "Item")
                .Map(typeof(IntEnum), "Enum"),
            () => new RenamingTypeMapper().Map(typeof(Wide), "Upgrade").Map(typeof(ItemV2), "Item")
                .Map(typeof(IntEnumV2), "Enum"),
            VerifyWide);
        yield return new("ChangedScalarTypes", CreateNarrowScalars,
            () => new RenamingTypeMapper().Map(typeof(NarrowScalars), "Upgrade").Map(typeof(Item), "Item")
                .Map(typeof(IntEnum), "Enum"),
            () => new RenamingTypeMapper().Map(typeof(WideScalars), "Upgrade").Map(typeof(ItemV2), "Item")
                .Map(typeof(IntEnumV2), "Enum"),
            VerifyWideScalars);
    }

    static void VerifyWideScalars(object original, object? result)
    {
        var w = Assert.IsType<WideScalars>(result);
        Assert.Equal(-5L, w.I2L);
        Assert.Equal(6, w.I2N);
        Assert.Equal(7L, w.N2L);
        Assert.Null(w.N2N);
        Assert.Equal(2, w.E2I);
        Assert.Equal(IntEnumV2.One, w.E2V2);
        Assert.Equal(IntEnumV2.Two, w.NE2NE);
        Assert.Equal((10, "ten"), w.T2Less);
        Assert.Equal((11, "eleven", false), w.T2More);
        Assert.Equal(Tuple.Create(12, "twelve"), w.T2R);
        Assert.Equal("v2", w.I2V2!.Name);
        Assert.Equal(12L, w.I2V2.Value);
        Assert.Null(w.I2V2.Extra);
        Assert.Equal("l", Assert.Single(w.LI2LV2!).Name);
        Assert.Equal([5], w.L2C);
        Assert.Equal([6, 7], w.A2L);
    }

    static void VerifyGraph(object original, object? result)
    {
        var g = Assert.IsType<Graph>(result);
        Assert.Same(g.A, g.B);
        Assert.Equal("shared", g.A!.Name);
        Assert.Equal(2, g.Shared!.Count);
        Assert.Same(g.A, g.Shared[0]);
        Assert.Same(g.A, g.Shared[1]);
        var d = Assert.IsType<DerivedItem>(g.Polymorphic);
        Assert.Equal(("d", 2, 1.5), (d.Name, d.Value, d.Weight));
        Assert.Equal(42, g.BoxedInt);
        Assert.Equal(IntEnum.One, g.BoxedEnum);
        Assert.Equal("s", g.Str);
        Assert.Equal("n1", g.Cycle!.Name);
        Assert.Equal("n2", g.Cycle.Next!.Name);
        Assert.Same(g.Cycle, g.Cycle.Next.Next);
        Assert.Same(g.A, g.Cycle.Next.Payload);
    }

    public static Dictionary<string, object?> Fields(object? obj)
    {
        var d = Assert.IsType<ObjectTypeDescriptor.DynamicObject>(obj);
        return d.ToDictionary(p => p.Key, p => (object?)p.Value);
    }

    static void VerifyDynamicGraph(object original, object? result)
    {
        var g = Fields(result);
        Assert.Same(g["A"], g["B"]);
        Assert.Equal("shared", Fields(g["A"])["Name"]);
        var shared = Assert.IsAssignableFrom<IList<object?>>(g["Shared"]);
        Assert.Same(g["A"], shared[0]);
        Assert.Same(g["A"], shared[1]);
        var d = Fields(g["Polymorphic"]);
        Assert.Equal("d", d["Name"]);
        Assert.Equal(2, d["Value"]);
        Assert.Equal(1.5, d["Weight"]);
        Assert.Equal(42, g["BoxedInt"]);
        Assert.Equal("One", g["BoxedEnum"]!.ToString());
        Assert.Equal("s", g["Str"]);
        var n1 = Fields(g["Cycle"]);
        Assert.Equal("n1", n1["Name"]);
        var n2 = Fields(n1["Next"]);
        Assert.Equal("n2", n2["Name"]);
        Assert.Same(g["Cycle"], n2["Next"]);
        Assert.Same(g["A"], n2["Payload"]);
    }

    static void VerifyWide(object original, object? result)
    {
        var w = Assert.IsType<Wide>(result);
        Assert.Equal(-5L, w.I2L);
        Assert.Equal(6, w.I2N);
        Assert.Equal(7L, w.N2L);
        Assert.Null(w.N2N);
        Assert.Equal([1L, 2L], w.L2A);
        Assert.Equal([3L, 4L], w.L2S!.OrderBy(x => x));
        Assert.Equal([5], w.L2C);
        Assert.Equal([6, 7], w.A2L);
        Assert.Equal("one", w.D2I![1L]);
        Assert.Single(w.D2I);
        Assert.Equal(2, w.E2I);
        Assert.Equal(IntEnumV2.One, w.E2V2);
        Assert.Equal(IntEnumV2.Two, w.NE2NE);
        Assert.Equal((8L, "eight"), w.T2T);
        Assert.Equal(Tuple.Create(9L, "nine"), w.T2R);
        Assert.Equal((10, "ten"), w.T2Less);
        Assert.Equal((11, "eleven", false), w.T2More);
        Assert.Equal("v2", w.I2V2!.Name);
        Assert.Equal(12L, w.I2V2.Value);
        Assert.Null(w.I2V2.Extra);
        Assert.Equal("l", Assert.Single(w.LI2LV2!).Name);
        Assert.Equal(13L, w.LI2LV2![0].Value);
        Assert.Equal("d", w.DI2DV2![14].Name);
        Assert.Equal(uint.MaxValue, w.U2L);
        Assert.Equal([1L, null], w.LN2LL);
    }
}
