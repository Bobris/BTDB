using System;
using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

/// Data written with one field type and read back with another, in both IL emitted and NoEmit modes.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class FieldHandlerUpgradeConversionTest : FieldHandlerCoverageTestBase
{
    public enum ColorV1
    {
        Red = 1,
        Green = 2,
        Blue = 3
    }

    public enum ColorV2 : long
    {
        Red = 1,
        Green = 2,
        Blue = 3,
        Black = 4
    }

    [Generate]
    public class ItemV1
    {
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    [Generate]
    public class NumbersV1
    {
        public int IntToLong { get; set; }
        public long LongToInt { get; set; }
        public uint UIntToULong { get; set; }
        public int IntToNullableInt { get; set; }
        public int? NullableIntToInt { get; set; }
        public int? NullIntToInt { get; set; }
        public int IntToNullableLong { get; set; }
        public int? NullableIntToLong { get; set; }
        public float FloatToDouble { get; set; }
        public double DoubleToFloat { get; set; }
        public ColorV1 EnumToEnum { get; set; }
        public ColorV1 EnumToNullableEnum { get; set; }
        public ColorV1? NullableEnumToEnum { get; set; }
        public DateTime DateToNullableDate { get; set; }
        public byte ByteToInt { get; set; }
        public short ShortToLong { get; set; }
        public ushort UShortToUInt { get; set; }
    }

    [Generate]
    public class NumbersV2
    {
        public long IntToLong { get; set; }
        public int LongToInt { get; set; }
        public ulong UIntToULong { get; set; }
        public int? IntToNullableInt { get; set; }
        public int NullableIntToInt { get; set; }
        public int NullIntToInt { get; set; }
        public long? IntToNullableLong { get; set; }
        public long NullableIntToLong { get; set; }
        public double FloatToDouble { get; set; }
        public float DoubleToFloat { get; set; }
        public ColorV2 EnumToEnum { get; set; }
        public ColorV2? EnumToNullableEnum { get; set; }
        public ColorV2 NullableEnumToEnum { get; set; }
        public DateTime? DateToNullableDate { get; set; }
        public int ByteToInt { get; set; }
        public long ShortToLong { get; set; }
        public uint UShortToUInt { get; set; }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void NumericEnumAndNullableConversions(bool emit)
    {
        using var _ = EmitMode(emit);
        var date = new DateTime(2020, 5, 6, 7, 8, 9, DateTimeKind.Utc);
        var v2 = StoreAndReloadAs<NumbersV1, NumbersV2>(new NumbersV1
        {
            IntToLong = -5,
            LongToInt = 1234567,
            UIntToULong = uint.MaxValue,
            IntToNullableInt = 7,
            NullableIntToInt = 8,
            NullIntToInt = null,
            IntToNullableLong = -9,
            NullableIntToLong = 10,
            FloatToDouble = 1.5f,
            DoubleToFloat = 2.5,
            EnumToEnum = ColorV1.Green,
            EnumToNullableEnum = ColorV1.Blue,
            NullableEnumToEnum = ColorV1.Red,
            DateToNullableDate = date,
            ByteToInt = 200,
            ShortToLong = -300,
            UShortToUInt = 60000
        }, "Numbers");
        Assert.Equal(-5, v2.IntToLong);
        Assert.Equal(1234567, v2.LongToInt);
        Assert.Equal(uint.MaxValue, v2.UIntToULong);
        Assert.Equal(7, v2.IntToNullableInt);
        Assert.Equal(8, v2.NullableIntToInt);
        Assert.Equal(0, v2.NullIntToInt);
        Assert.Equal(-9, v2.IntToNullableLong);
        Assert.Equal(10, v2.NullableIntToLong);
        Assert.Equal(1.5, v2.FloatToDouble);
        Assert.Equal(2.5f, v2.DoubleToFloat);
        Assert.Equal(ColorV2.Green, v2.EnumToEnum);
        Assert.Equal(ColorV2.Blue, v2.EnumToNullableEnum);
        Assert.Equal(ColorV2.Red, v2.NullableEnumToEnum);
        Assert.Equal(date, v2.DateToNullableDate);
        Assert.Equal(200, v2.ByteToInt);
        Assert.Equal(-300, v2.ShortToLong);
        Assert.Equal(60000u, v2.UShortToUInt);
        Assert.Empty(LoggedMessages);
    }

    [Generate]
    public class ExtremesV1
    {
        public uint UIntToLong { get; set; }
        public int IntToULong { get; set; }
        public short ShortToULong { get; set; }
        public long LongToULong { get; set; }
        public ulong ULongToLong { get; set; }
        public int IntToUInt { get; set; }
        public uint UIntToDouble { get; set; }
        public ulong ULongToDouble { get; set; }
        public ulong ULongToFloat { get; set; }
        public long LongToDouble { get; set; }
    }

    [Generate]
    public class ExtremesV2
    {
        public long UIntToLong { get; set; }
        public ulong IntToULong { get; set; }
        public ulong ShortToULong { get; set; }
        public ulong LongToULong { get; set; }
        public long ULongToLong { get; set; }
        public uint IntToUInt { get; set; }
        public double UIntToDouble { get; set; }
        public double ULongToDouble { get; set; }
        public float ULongToFloat { get; set; }
        public double LongToDouble { get; set; }
    }

    /// Values at the edges of their types read back as other numeric types like C# casts, or are reported.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ExtremeNumericConversions(bool emit)
    {
        using var _ = EmitMode(emit);
        var v1 = new ExtremesV1
        {
            UIntToLong = uint.MaxValue, IntToULong = -1, ShortToULong = short.MinValue, LongToULong = long.MinValue,
            ULongToLong = ulong.MaxValue, IntToUInt = -1, UIntToDouble = uint.MaxValue, ULongToDouble = ulong.MaxValue,
            ULongToFloat = ulong.MaxValue, LongToDouble = long.MinValue
        };
        var v2 = StoreAndReloadAs<ExtremesV1, ExtremesV2>(v1, "Extremes");
        var logged = string.Join(Environment.NewLine, LoggedMessages);
        var actual = new object[]
        {
            v2.UIntToLong, v2.IntToULong, v2.ShortToULong, v2.LongToULong, v2.ULongToLong, v2.IntToUInt,
            v2.UIntToDouble, v2.ULongToDouble, v2.ULongToFloat, v2.LongToDouble
        };
        var expected = new object[]
        {
            (long)v1.UIntToLong, unchecked((ulong)v1.IntToULong), unchecked((ulong)v1.ShortToULong),
            unchecked((ulong)v1.LongToULong), unchecked((long)v1.ULongToLong), unchecked((uint)v1.IntToUInt),
            (double)v1.UIntToDouble, (double)v1.ULongToDouble, (float)v1.ULongToFloat, (double)v1.LongToDouble
        };
        Assert.True(expected.SequenceEqual(actual),
            $"expected {string.Join(", ", expected)}{Environment.NewLine}actual {string.Join(", ", actual)}{Environment.NewLine}{logged}");
    }

    [Generate]
    public class SpecialV1
    {
        public byte[]? BytesToBuffer { get; set; }
        public ByteBuffer BufferToBytes { get; set; }
        public byte[]? BytesToMemory { get; set; }
        public ReadOnlyMemory<byte> MemoryToBytes { get; set; }
        public string? StringToEncrypted { get; set; }
        public EncryptedString EncryptedToString { get; set; }
        public Version? VersionToString { get; set; }
        public string? StringToVersion { get; set; }
        public int IntToString { get; set; }
        public bool BoolToString { get; set; }
        public double DoubleToString { get; set; }
        public ulong ULongToString { get; set; }
        public decimal DecimalToString { get; set; }
        public int IntToDecimal { get; set; }
        public uint UIntToDecimal { get; set; }
        public double DoubleToDecimal { get; set; }
        public int IntToBool { get; set; }
    }

    [Generate]
    public class SpecialV2
    {
        public ByteBuffer BytesToBuffer { get; set; }
        public byte[]? BufferToBytes { get; set; }
        public ReadOnlyMemory<byte> BytesToMemory { get; set; }
        public byte[]? MemoryToBytes { get; set; }
        public EncryptedString StringToEncrypted { get; set; }
        public string? EncryptedToString { get; set; }
        public string? VersionToString { get; set; }
        public Version? StringToVersion { get; set; }
        public string? IntToString { get; set; }
        public string? BoolToString { get; set; }
        public string? DoubleToString { get; set; }
        public string? ULongToString { get; set; }
        public string? DecimalToString { get; set; }
        public decimal IntToDecimal { get; set; }
        public decimal UIntToDecimal { get; set; }
        public decimal DoubleToDecimal { get; set; }
        public bool IntToBool { get; set; }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SpecialTypeConversions(bool emit)
    {
        using var _ = EmitMode(emit);
        var v2 = StoreAndReloadAs<SpecialV1, SpecialV2>(new SpecialV1
        {
            BytesToBuffer = new byte[] { 1, 2 },
            BufferToBytes = ByteBuffer.NewAsync(new byte[] { 3 }),
            BytesToMemory = new byte[] { 4, 5, 6 },
            MemoryToBytes = new byte[] { 7 },
            StringToEncrypted = "plain",
            EncryptedToString = "secret",
            VersionToString = new Version(1, 2),
            StringToVersion = "3.4.5",
            IntToString = -42,
            BoolToString = true,
            DoubleToString = 1.25,
            ULongToString = ulong.MaxValue,
            DecimalToString = 1.5m,
            IntToDecimal = -7,
            UIntToDecimal = 7,
            DoubleToDecimal = 0.5,
            IntToBool = 3
        }, "Special");
        // integers are loaded as long and there is no long -> bool conversion
        Assert.Single(LoggedMessages);
        Assert.Contains("SignedFieldHandler", LoggedMessages[0]);
        Assert.False(v2.IntToBool);
        LoggedMessages.Clear();
        Assert.Equal(new byte[] { 1, 2 }, v2.BytesToBuffer.ToByteArray());
        Assert.Equal(new byte[] { 3 }, v2.BufferToBytes);
        Assert.Equal(new byte[] { 4, 5, 6 }, v2.BytesToMemory.ToArray());
        Assert.Equal(new byte[] { 7 }, v2.MemoryToBytes);
        Assert.Equal("plain", v2.StringToEncrypted);
        Assert.Equal("secret", v2.EncryptedToString);
        Assert.Equal("1.2", v2.VersionToString);
        Assert.Equal(new Version(3, 4, 5), v2.StringToVersion);
        Assert.Equal("-42", v2.IntToString);
        Assert.Equal("1", v2.BoolToString);
        Assert.Equal("1.25", v2.DoubleToString);
        Assert.Equal(ulong.MaxValue.ToString(), v2.ULongToString);
        Assert.Equal("1.5", v2.DecimalToString);
        Assert.Equal(-7m, v2.IntToDecimal);
        Assert.Equal(7m, v2.UIntToDecimal);
        Assert.Equal(0.5m, v2.DoubleToDecimal);
        Assert.Empty(LoggedMessages);
    }

    [Generate]
    public class CollectionsV1
    {
        public List<int>? ListIntToLong { get; set; }
        public List<int>? ListToIList { get; set; }
        public IList<string>? IListToList { get; set; }
        public List<int>? ListToSet { get; set; }
        public HashSet<int>? SetToList { get; set; }
        public int SingleToList { get; set; }
        public string? SingleStringToList { get; set; }
        public string? NullStringToList { get; set; }
        public List<byte[]>? ListBytesToMemory { get; set; }
        public List<ColorV1>? ListEnumUpgrade { get; set; }
        public List<int?>? ListNullableToInt { get; set; }
        public Dictionary<int, int>? DictIntToLong { get; set; }
        public Dictionary<int, List<int>>? DictOfLists { get; set; }
        public Dictionary<string, byte[]>? DictBytesToBuffer { get; set; }
        public IDictionary<int, string> OdbToDictionary { get; set; } = null!;
        public IDictionary<int, ItemV1> OdbOfObjectsToDictionary { get; set; } = null!;
        public IDictionary<int, string> OdbToOrdered { get; set; } = null!;
        public IOrderedSet<int> SetUpgrade { get; set; } = null!;
        public int? NullableToNullableLong { get; set; }
    }

    [Generate]
    public class CollectionsV2
    {
        public List<long>? ListIntToLong { get; set; }
        public IList<int>? ListToIList { get; set; }
        public List<string>? IListToList { get; set; }
        public HashSet<int>? ListToSet { get; set; }
        public List<int>? SetToList { get; set; }
        public List<int>? SingleToList { get; set; }
        public IList<string>? SingleStringToList { get; set; }
        public List<string>? NullStringToList { get; set; }
        public List<ReadOnlyMemory<byte>>? ListBytesToMemory { get; set; }
        public List<ColorV2>? ListEnumUpgrade { get; set; }
        public List<int>? ListNullableToInt { get; set; }
        public Dictionary<long, long>? DictIntToLong { get; set; }
        public Dictionary<long, List<long>>? DictOfLists { get; set; }
        public Dictionary<string, ByteBuffer>? DictBytesToBuffer { get; set; }
        public Dictionary<int, string>? OdbToDictionary { get; set; }
        public Dictionary<int, ItemV1>? OdbOfObjectsToDictionary { get; set; }
        public IOrderedDictionary<int, string> OdbToOrdered { get; set; } = null!;
        public IOrderedSet<long> SetUpgrade { get; set; } = null!;
        public long? NullableToNullableLong { get; set; }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CollectionConversions(bool emit)
    {
        using var _ = EmitMode(emit);
        _db.RegisterType(typeof(CollectionsV1), "Collections");
        using (var tr = _db.StartTransaction())
        {
            var v1 = tr.New<CollectionsV1>();
            v1.ListIntToLong = new List<int> { 1, -2 };
            v1.ListToIList = new List<int> { 3 };
            v1.IListToList = new List<string> { "a", "b" };
            v1.ListToSet = new List<int> { 4, 4, 5 };
            v1.SetToList = new HashSet<int> { 6 };
            v1.SingleToList = 7;
            v1.SingleStringToList = "single";
            v1.NullStringToList = null;
            v1.ListBytesToMemory = new List<byte[]> { new byte[] { 8 } };
            v1.ListEnumUpgrade = new List<ColorV1> { ColorV1.Blue };
            v1.ListNullableToInt = new List<int?> { 9, null };
            v1.DictIntToLong = new Dictionary<int, int> { { 1, 10 } };
            v1.DictOfLists = new Dictionary<int, List<int>> { { 2, new List<int> { 11 } } };
            v1.DictBytesToBuffer = new Dictionary<string, byte[]> { { "k", new byte[] { 12 } } };
            v1.OdbToDictionary[13] = "thirteen";
            v1.OdbOfObjectsToDictionary[14] = new ItemV1 { Name = "fourteen", Value = 14 };
            v1.OdbToOrdered[2] = "two";
            v1.OdbToOrdered[1] = "one";
            v1.SetUpgrade.Add(15);
            v1.NullableToNullableLong = 16;
            tr.Store(v1);
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(CollectionsV2), "Collections");
        using (var tr = _db.StartTransaction())
        {
            var v2 = tr.Enumerate<CollectionsV2>().Single();
            Assert.Equal(new long[] { 1, -2 }, v2.ListIntToLong);
            Assert.Equal(new[] { 3 }, v2.ListToIList);
            Assert.Equal(new[] { "a", "b" }, v2.IListToList);
            Assert.Equal(new HashSet<int> { 4, 5 }, v2.ListToSet);
            Assert.Equal(new[] { 6 }, v2.SetToList);
            Assert.Equal(new[] { 7 }, v2.SingleToList);
            Assert.Equal(new[] { "single" }, v2.SingleStringToList);
            Assert.Empty(v2.NullStringToList!);
            Assert.Equal(new byte[] { 8 }, v2.ListBytesToMemory!.Single().ToArray());
            Assert.Equal(new[] { ColorV2.Blue }, v2.ListEnumUpgrade);
            Assert.Equal(new[] { 9, 0 }, v2.ListNullableToInt);
            Assert.Equal(new Dictionary<long, long> { { 1, 10 } }, v2.DictIntToLong);
            Assert.Equal(new long[] { 11 }, v2.DictOfLists![2]);
            Assert.Equal(new byte[] { 12 }, v2.DictBytesToBuffer!["k"].ToByteArray());
            Assert.Equal(new Dictionary<int, string> { { 13, "thirteen" } }, v2.OdbToDictionary);
            Assert.Equal("fourteen", v2.OdbOfObjectsToDictionary![14].Name);
            Assert.Equal(new[] { 1, 2 }, v2.OdbToOrdered.Keys);
            Assert.Equal(new[] { "two", "one" }, v2.OdbToOrdered.GetReverseEnumerator().Select(p => p.Value));
            Assert.Equal(new long[] { 15 }, v2.SetUpgrade);
            Assert.Equal(16, v2.NullableToNullableLong);
        }

        Assert.Empty(LoggedMessages);
    }

    [Generate]
    public class TuplesV1
    {
        public (int, string) Widen { get; set; }
        public (int, string) AddItem { get; set; }
        public (int, string, int) RemoveItem { get; set; }
        public Tuple<int, int> RefToValue { get; set; } = Tuple.Create(0, 0);
        public (int, int) ValueToRef { get; set; }
        public List<(int, string)>? ListOfTuples { get; set; }
        public (int, string)? NullableTuple { get; set; }
    }

    [Generate]
    public class TuplesV2
    {
        public (long, string) Widen { get; set; }
        public (int, string, int) AddItem { get; set; }
        public (int, string) RemoveItem { get; set; }
        public (int, int) RefToValue { get; set; }
        public Tuple<int, int> ValueToRef { get; set; } = null!;
        public List<(long, string)>? ListOfTuples { get; set; }
        public (int, string)? NullableTuple { get; set; }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TupleConversions(bool emit)
    {
        using var _ = EmitMode(emit);
        var v2 = StoreAndReloadAs<TuplesV1, TuplesV2>(new TuplesV1
        {
            Widen = (1, "a"),
            AddItem = (2, "b"),
            RemoveItem = (3, "c", 4),
            RefToValue = Tuple.Create(5, 6),
            ValueToRef = (7, 8),
            ListOfTuples = new List<(int, string)> { (9, "d") },
            NullableTuple = (10, "e")
        }, "Tuples");
        Assert.Equal((1L, "a"), v2.Widen);
        Assert.Equal((2, "b", 0), v2.AddItem);
        Assert.Equal((3, "c"), v2.RemoveItem);
        Assert.Equal((5, 6), v2.RefToValue);
        Assert.Equal(Tuple.Create(7, 8), v2.ValueToRef);
        Assert.Equal(new[] { (9L, "d") }, v2.ListOfTuples);
        Assert.Equal((10, "e"), v2.NullableTuple);
        Assert.Empty(LoggedMessages);
    }

    [Generate]
    public class IncompatibleV1
    {
        public string? Name { get; set; }
        public string? StringToInt { get; set; }
        public List<string>? ListToInt { get; set; }
        public Dictionary<int, string>? DictToList { get; set; }
        public (int, string) TupleToString { get; set; }
    }

    [Generate]
    public class IncompatibleV2
    {
        public string? Name { get; set; }
        public int StringToInt { get; set; }
        public int ListToInt { get; set; }
        public List<int>? DictToList { get; set; }
        public string? TupleToString { get; set; }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void IncompatibleChangesAreSkippedAndReported(bool emit)
    {
        using var _ = EmitMode(emit);
        var v2 = StoreAndReloadAs<IncompatibleV1, IncompatibleV2>(new IncompatibleV1
        {
            Name = "keep",
            StringToInt = "x",
            ListToInt = new List<string> { "y" },
            DictToList = new Dictionary<int, string> { { 1, "z" } },
            TupleToString = (1, "t")
        }, "Incompatible");
        Assert.Equal("keep", v2.Name);
        Assert.Equal(0, v2.StringToInt);
        Assert.Equal(0, v2.ListToInt);
        Assert.Null(v2.DictToList);
        Assert.Null(v2.TupleToString);
        Assert.Equal(4, LoggedMessages.Count);
        Assert.All(LoggedMessages, m => Assert.StartsWith("Cannot load ", m));
    }

    [Generate]
    public class WideV1
    {
        public DateTimeOffset? NullableOffsetToOffset { get; set; }
        public Guid? NullableGuidToGuid { get; set; }
        public DateTimeOffset? NullOffsetToOffset { get; set; }
    }

    [Generate]
    public class WideV2
    {
        public DateTimeOffset NullableOffsetToOffset { get; set; }
        public Guid NullableGuidToGuid { get; set; }
        public DateTimeOffset NullOffsetToOffset { get; set; }
    }

    // Before the fix a value type larger than Int128, like DateTimeOffset?, had no staging for its conversion, so the
    // field was reported and loaded as default.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void WideNullableToPlainConversions(bool emit)
    {
        using var _ = EmitMode(emit);
        var offset = new DateTimeOffset(2020, 1, 2, 3, 4, 5, TimeSpan.FromHours(2));
        var guid = Guid.Parse("01020304-0506-0708-090a-0b0c0d0e0f10");
        var v2 = StoreAndReloadAs<WideV1, WideV2>(new WideV1
        {
            NullableOffsetToOffset = offset, NullableGuidToGuid = guid, NullOffsetToOffset = null
        }, "Wide");
        Assert.Equal(offset, v2.NullableOffsetToOffset);
        Assert.Equal(offset.Offset, v2.NullableOffsetToOffset.Offset);
        Assert.Equal(guid, v2.NullableGuidToGuid);
        Assert.Equal(default, v2.NullOffsetToOffset);
        Assert.Empty(LoggedMessages);
    }
}
