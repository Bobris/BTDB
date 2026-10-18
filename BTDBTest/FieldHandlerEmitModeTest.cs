using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using BTDB;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using Microsoft.Extensions.Primitives;
using Xunit;

namespace BTDBTest;

/// Runs the same scenarios with IL emitted (UseNoEmit* = false) and with delegate (NoEmit) based field handlers,
/// so both implementations of every field handler are verified to produce identical results.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class FieldHandlerEmitModeTest : FieldHandlerCoverageTestBase
{
    IDisposable Mode(bool emit) => EmitMode(emit);

    public enum Color
    {
        Red,
        Green,
        Blue
    }

    [Flags]
    public enum Perm : ushort
    {
        None = 0,
        Read = 1,
        Write = 2
    }

    [Generate]
    public class Item
    {
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    [Generate]
    public class AllTypes
    {
        public string? Str { get; set; }
        public byte U8 { get; set; }
        public sbyte I8 { get; set; }
        public short I16 { get; set; }
        public ushort U16 { get; set; }
        public int I32 { get; set; }
        public uint U32 { get; set; }
        public long I64 { get; set; }
        public ulong U64 { get; set; }
        public bool Bool { get; set; }
        public float F { get; set; }
        public double D { get; set; }
        public decimal Dec { get; set; }
        public DateTime Dt { get; set; }
        public DateTimeOffset Dto { get; set; }
        public TimeSpan Ts { get; set; }
        public Guid G { get; set; }
        public IPAddress? Ip { get; set; }
        public Version? Ver { get; set; }
        public StringValues Sv { get; set; }
        public EncryptedString Secret { get; set; }
        public byte[]? Bytes { get; set; }
        public ByteBuffer Buf { get; set; }
        public ReadOnlyMemory<byte> Mem { get; set; }
        public Color Col { get; set; }
        public Perm Perms { get; set; }
        public int? NInt { get; set; }
        public Color? NCol { get; set; }
        public DateTime? NDt { get; set; }
        public List<int>? Ints { get; set; }
        public IList<string>? Strs { get; set; }
        public HashSet<string>? Set { get; set; }
        public ISet<int>? ISet { get; set; }
        public IList<byte[]>? ListOfBytes { get; set; }
        public List<Color>? Colors { get; set; }
        public List<int?>? NInts { get; set; }
        public Dictionary<int, string>? Dict { get; set; }
        public Dictionary<string, List<int>>? DictOfLists { get; set; }
        public IDictionary<int, string> OdbDict { get; set; } = null!;
        public IOrderedDictionary<string, Item> OdbOrderedDict { get; set; } = null!;
        public IOrderedSet<ulong> OdbSet { get; set; } = null!;
        public Tuple<int, string>? Tuple { get; set; }
        public (int, string) VTuple { get; set; }
        public (long, Color, string?) VTuple3 { get; set; }
        public Item? Nested { get; set; }
        public List<Item>? Items { get; set; }
        public Dictionary<int, Item>? ItemsById { get; set; }
        public IRoaringBitmap Bits { get; set; } = null!;
        public ILazyUlongList Lazy { get; set; } = null!;
    }

    static void Fill(AllTypes o)
    {
        o.Str = "hello";
        o.U8 = 200;
        o.I8 = -100;
        o.I16 = -30000;
        o.U16 = 60000;
        o.I32 = int.MinValue;
        o.U32 = uint.MaxValue;
        o.I64 = long.MinValue;
        o.U64 = ulong.MaxValue;
        o.Bool = true;
        o.F = 1.5f;
        o.D = -2.25;
        o.Dec = 123.456m;
        o.Dt = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        o.Dto = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.FromHours(2));
        o.Ts = TimeSpan.FromMinutes(90);
        o.G = new Guid("01234567-89ab-cdef-0123-456789abcdef");
        o.Ip = IPAddress.Parse("10.20.30.40");
        o.Ver = new Version(1, 2, 3, 4);
        o.Sv = new StringValues(new[] { "a", "b" });
        o.Secret = "secret";
        o.Bytes = new byte[] { 1, 2, 3 };
        o.Buf = ByteBuffer.NewAsync(new byte[] { 4, 5 });
        o.Mem = new byte[] { 6, 7, 8 };
        o.Col = Color.Blue;
        o.Perms = Perm.Read | Perm.Write;
        o.NInt = 42;
        o.NCol = Color.Green;
        o.NDt = null;
        o.Ints = new List<int> { 3, 1, 2 };
        o.Strs = new List<string> { "x", "y" };
        o.Set = new HashSet<string> { "s1", "s2" };
        o.ISet = new HashSet<int> { 7 };
        o.ListOfBytes = new List<byte[]> { new byte[] { 9 }, Array.Empty<byte>() };
        o.Colors = new List<Color> { Color.Green, Color.Red };
        o.NInts = new List<int?> { 1, null, 3 };
        o.Dict = new Dictionary<int, string> { { 1, "one" }, { 2, "two" } };
        o.DictOfLists = new Dictionary<string, List<int>> { { "a", new List<int> { 1 } }, { "b", new List<int>() } };
        o.OdbDict[5] = "five";
        o.OdbOrderedDict["k"] = new Item { Name = "in dict", Value = 1 };
        o.OdbSet.Add(10);
        o.OdbSet.Add(5);
        o.Tuple = System.Tuple.Create(1, "t");
        o.VTuple = (2, "vt");
        o.VTuple3 = (3L, Color.Blue, null);
        o.Nested = new Item { Name = "nested", Value = 7 };
        o.Items = new List<Item> { new() { Name = "i1", Value = 1 }, new() { Name = "i2", Value = 2 } };
        o.ItemsById = new Dictionary<int, Item> { { 1, new Item { Name = "byId", Value = 3 } } };
        o.Bits.Set(3, true);
        o.Bits.Set(100000, true);
        o.Bits.Flush();
        o.Lazy.Add(11);
        o.Lazy.Add(22);
        o.Lazy.Flush();
    }

    static void Check(AllTypes o)
    {
        Assert.Equal("hello", o.Str);
        Assert.Equal(200, o.U8);
        Assert.Equal(-100, o.I8);
        Assert.Equal(-30000, o.I16);
        Assert.Equal(60000, o.U16);
        Assert.Equal(int.MinValue, o.I32);
        Assert.Equal(uint.MaxValue, o.U32);
        Assert.Equal(long.MinValue, o.I64);
        Assert.Equal(ulong.MaxValue, o.U64);
        Assert.True(o.Bool);
        Assert.Equal(1.5f, o.F);
        Assert.Equal(-2.25, o.D);
        Assert.Equal(123.456m, o.Dec);
        Assert.Equal(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc), o.Dt);
        Assert.Equal(new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.FromHours(2)), o.Dto);
        Assert.Equal(TimeSpan.FromMinutes(90), o.Ts);
        Assert.Equal(new Guid("01234567-89ab-cdef-0123-456789abcdef"), o.G);
        Assert.Equal(IPAddress.Parse("10.20.30.40"), o.Ip);
        Assert.Equal(new Version(1, 2, 3, 4), o.Ver);
        Assert.Equal(new[] { "a", "b" }, o.Sv.ToArray());
        Assert.Equal("secret", o.Secret);
        Assert.Equal(new byte[] { 1, 2, 3 }, o.Bytes);
        Assert.Equal(new byte[] { 4, 5 }, o.Buf.ToByteArray());
        Assert.Equal(new byte[] { 6, 7, 8 }, o.Mem.ToArray());
        Assert.Equal(Color.Blue, o.Col);
        Assert.Equal(Perm.Read | Perm.Write, o.Perms);
        Assert.Equal(42, o.NInt);
        Assert.Equal(Color.Green, o.NCol);
        Assert.Null(o.NDt);
        Assert.Equal(new[] { 3, 1, 2 }, o.Ints);
        Assert.Equal(new[] { "x", "y" }, o.Strs);
        Assert.Equal(new HashSet<string> { "s1", "s2" }, o.Set);
        Assert.Equal(new[] { 7 }, o.ISet);
        Assert.Equal(new[] { "09", "" }, o.ListOfBytes!.Select(Convert.ToHexString));
        Assert.Equal(new[] { Color.Green, Color.Red }, o.Colors);
        Assert.Equal(new int?[] { 1, null, 3 }, o.NInts);
        Assert.Equal(new Dictionary<int, string> { { 1, "one" }, { 2, "two" } }, o.Dict);
        Assert.Equal(new[] { 1 }, o.DictOfLists!["a"]);
        Assert.Empty(o.DictOfLists["b"]);
        Assert.Equal("five", o.OdbDict[5]);
        Assert.Equal("in dict", o.OdbOrderedDict["k"].Name);
        Assert.Equal(new ulong[] { 5, 10 }, o.OdbSet);
        Assert.Equal(System.Tuple.Create(1, "t"), o.Tuple);
        Assert.Equal((2, "vt"), o.VTuple);
        Assert.Equal((3L, Color.Blue, (string?)null), o.VTuple3);
        Assert.Equal("nested", o.Nested!.Name);
        Assert.Equal(7, o.Nested.Value);
        Assert.Equal(new[] { "i1", "i2" }, o.Items!.Select(i => i.Name));
        Assert.Equal("byId", o.ItemsById![1].Name);
        Assert.Equal(new ulong[] { 3, 100000 }, o.Bits);
        Assert.Equal(new ulong[] { 11, 22 }, o.Lazy);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AllTypesRoundTripInSingleton(bool emit)
    {
        using var _ = Mode(emit);
        using (var tr = _db.StartTransaction())
        {
            var o = tr.Singleton<AllTypes>();
            Fill(o);
            tr.Store(o);
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            Check(tr.Singleton<AllTypes>());
        }

        Assert.Empty(LoggedMessages);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AllTypesRoundTripAsStoredObjectAndSkipWhenAllFieldsRemoved(bool emit)
    {
        using var _ = Mode(emit);
        _db.RegisterType(typeof(AllTypes), "AllTypesObj");
        using (var tr = _db.StartTransaction())
        {
            var o = tr.New<AllTypes>();
            Fill(o);
            tr.Store(o);
            // reference Tuple<> has no null representation, so it has to be filled
            tr.Store(new AllTypes { Tuple = System.Tuple.Create(0, "") });
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(AllTypes), "AllTypesObj");
        using (var tr = _db.StartTransaction())
        {
            var all = tr.Enumerate<AllTypes>().ToList();
            Assert.Equal(2, all.Count);
            Check(all.Single(o => o.Str == "hello"));
            var empty = all.Single(o => o.Str == null);
            Assert.Null(empty.Bytes);
            Assert.Null(empty.Ints);
            Assert.Equal(default, empty.VTuple);
        }

        ReopenDb();
        _db.RegisterType(typeof(Item), "AllTypesObj");
        using (var tr = _db.StartTransaction())
        {
            // all fields are skipped because no one matches by name
            var all = tr.Enumerate<Item>().ToList();
            Assert.Equal(2, all.Count);
            Assert.All(all, i => Assert.Equal("", i.Name));
        }

        Assert.Empty(LoggedMessages);
    }

    public enum EU8 : byte { A = 1, Max = byte.MaxValue }
    public enum EI8 : sbyte { Min = sbyte.MinValue, A = 1 }
    public enum EU16 : ushort { A = 1, Max = ushort.MaxValue }
    public enum EI16 : short { Min = short.MinValue, A = 1 }
    public enum EU32 : uint { A = 1, Max = uint.MaxValue }
    public enum EI64 : long { Min = long.MinValue, A = 1 }
    public enum EU64 : ulong { A = 1, Max = ulong.MaxValue }

    [Flags]
    public enum EFlags64 : ulong
    {
        None = 0,
        Low = 1,
        High = 1ul << 63
    }

    [Generate]
    public class EnumsRoot
    {
        public EU8 U8 { get; set; }
        public EI8 I8 { get; set; }
        public EU16 U16 { get; set; }
        public EI16 I16 { get; set; }
        public EU32 U32 { get; set; }
        public EI64 I64 { get; set; }
        public EU64 U64 { get; set; }
        public EFlags64 Flags { get; set; }
        public List<EU64>? List { get; set; }
        public Dictionary<EI8, EU16>? Dict { get; set; }
        public EI16? Nullable { get; set; }
        public IOrderedDictionary<EI64, EU8> Ordered { get; set; } = null!;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void EnumsWithAllUnderlyingTypesRoundTrip(bool emit)
    {
        using var _ = Mode(emit);
        using (var tr = _db.StartTransaction())
        {
            var o = tr.Singleton<EnumsRoot>();
            o.U8 = EU8.Max;
            o.I8 = EI8.Min;
            o.U16 = EU16.Max;
            o.I16 = EI16.Min;
            o.U32 = EU32.Max;
            o.I64 = EI64.Min;
            o.U64 = EU64.Max;
            o.Flags = EFlags64.Low | EFlags64.High;
            o.List = new List<EU64> { EU64.A, EU64.Max };
            o.Dict = new Dictionary<EI8, EU16> { { EI8.Min, EU16.Max } };
            o.Nullable = EI16.A;
            o.Ordered[EI64.A] = EU8.A;
            o.Ordered[EI64.Min] = EU8.Max;
            tr.Store(o);
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var o = tr.Singleton<EnumsRoot>();
            Assert.Equal(EU8.Max, o.U8);
            Assert.Equal(EI8.Min, o.I8);
            Assert.Equal(EU16.Max, o.U16);
            Assert.Equal(EI16.Min, o.I16);
            Assert.Equal(EU32.Max, o.U32);
            Assert.Equal(EI64.Min, o.I64);
            Assert.Equal(EU64.Max, o.U64);
            Assert.Equal(EFlags64.Low | EFlags64.High, o.Flags);
            Assert.Equal(new[] { EU64.A, EU64.Max }, o.List);
            Assert.Equal(EU16.Max, o.Dict![EI8.Min]);
            Assert.Equal(EI16.A, o.Nullable);
            Assert.Equal(new[] { EI64.Min, EI64.A }, o.Ordered.Keys);
            Assert.Equal(EU8.Max, o.Ordered[EI64.Min]);
        }

        Assert.Empty(LoggedMessages);
    }

    [Generate]
    public class IndirectRoot
    {
        public IIndirect<Item>? Ind { get; set; }
        public IIndirect<Item>? Ind2 { get; set; }
        public string? Name { get; set; }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void IndirectReferencesRoundTrip(bool emit)
    {
        using var _ = Mode(emit);
        using (var tr = _db.StartTransaction())
        {
            var o = tr.Singleton<IndirectRoot>();
            o.Ind = new DBIndirect<Item>(new Item { Name = "ind", Value = 1 });
            o.Ind2 = null;
            o.Name = "root";
            tr.Store(o);
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var o = tr.Singleton<IndirectRoot>();
            Assert.Null(o.Ind2?.Value);
            Assert.NotEqual(0ul, o.Ind!.Oid);
            // store again without materialization keeps the reference
            o.Name = "changed";
            tr.Store(o);
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var o = tr.Singleton<IndirectRoot>();
            Assert.Equal("changed", o.Name);
            Assert.Equal("ind", o.Ind!.Value.Name);
            Assert.Equal(1, o.Ind.Value.Value);
        }

        Assert.Empty(LoggedMessages);
    }

    [Generate]
    public class ByteKeysRoot
    {
        public IOrderedDictionary<byte[], int> Bytes { get; set; } = null!;
        public IOrderedDictionary<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>> Memory { get; set; } = null!;
        public IOrderedDictionary<ByteBuffer, ByteBuffer> Buffers { get; set; } = null!;
    }

    [Generate]
    public class TupleKeysRoot
    {
        public IOrderedSet<(int, byte[])> TupleSet { get; set; } = null!;
        public IOrderedDictionary<(int, string), int> TupleDict { get; set; } = null!;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TupleKeysInOrderedCollectionsSurviveReopen(bool emit)
    {
        using var _ = Mode(emit);
        using (var tr = _db.StartTransaction())
        {
            var o = tr.Singleton<TupleKeysRoot>();
            o.TupleSet.Add((2, new byte[] { 1 }));
            o.TupleSet.Add((1, new byte[] { 2, 3 }));
            o.TupleDict[(2, "a")] = 1;
            o.TupleDict[(1, "b")] = 2;
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var o = tr.Singleton<TupleKeysRoot>();
            Assert.Equal(new[] { 1, 2 }, o.TupleSet.Select(t => t.Item1));
            Assert.Equal(new byte[] { 2, 3 }, o.TupleSet.First().Item2);
            Assert.Equal(new[] { (1, "b"), (2, "a") }, o.TupleDict.Keys);
            Assert.Equal(2, o.TupleDict[(1, "b")]);
        }

        Assert.Empty(LoggedMessages);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ByteArrayKeysInOrderedCollectionsUseLastFieldEncoding(bool emit)
    {
        using var _ = Mode(emit);
        using (var tr = _db.StartTransaction())
        {
            var o = tr.Singleton<ByteKeysRoot>();
            o.Bytes[new byte[] { 2 }] = 2;
            o.Bytes[new byte[] { 1, 5 }] = 15;
            o.Bytes[Array.Empty<byte>()] = 0;
            o.Memory[new byte[] { 9, 9 }] = new byte[] { 1 };
            o.Memory[new byte[] { 3 }] = new byte[] { 2, 2 };
            o.Buffers[ByteBuffer.NewAsync(new byte[] { 7 })] = ByteBuffer.NewAsync(new byte[] { 8 });
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var o = tr.Singleton<ByteKeysRoot>();
            // last key field is stored raw, so keys are ordered lexicographically
            Assert.Equal(new[] { "", "0105", "02" }, o.Bytes.Keys.Select(Convert.ToHexString));
            Assert.Equal(15, o.Bytes[new byte[] { 1, 5 }]);
            Assert.Equal(new[] { "03", "0909" }, o.Memory.Keys.Select(k => Convert.ToHexString(k.Span)));
            Assert.Equal(new byte[] { 2, 2 }, o.Memory[new byte[] { 3 }].ToArray());
            var buf = o.Buffers.Single();
            Assert.Equal(new byte[] { 7 }, buf.Key.ToByteArray());
            Assert.Equal(new byte[] { 8 }, buf.Value.ToByteArray());
        }

        Assert.Empty(LoggedMessages);
    }
}
