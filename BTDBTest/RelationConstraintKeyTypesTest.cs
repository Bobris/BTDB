using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

// Generic Constraint.Exact<T> and Constraint<T>.Any for all integer widths, enums with all underlying types and
// nullable key types, compared against a LINQ reference over the same data, in both relation implementation modes.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class RelationConstraintKeyTypesTest : ObjectDbTestBase
{
    public RelationConstraintKeyTypesTest(ITestOutputHelper output) : base(output)
    {
    }

    public enum E8 : sbyte { Min = sbyte.MinValue, Zero = 0, Max = sbyte.MaxValue }
    public enum E16 : short { Min = short.MinValue, Zero = 0, Max = short.MaxValue }
    public enum E64 : long { Min = long.MinValue, Zero = 0, Max = long.MaxValue }
    public enum EU8 : byte { Zero = 0, Max = byte.MaxValue }
    public enum EU32 : uint { Zero = 0, Max = uint.MaxValue }
    public enum EU64 : ulong { Zero = 0, Max = ulong.MaxValue }

    public class IntKeys
    {
        [PrimaryKey(1)] public sbyte I8 { get; set; }
        [PrimaryKey(2)] public short I16 { get; set; }
        [PrimaryKey(3)] public int I32 { get; set; }
        [PrimaryKey(4)] public byte U8 { get; set; }
        [PrimaryKey(5)] public ushort U16 { get; set; }
        [PrimaryKey(6)] public uint U32 { get; set; }

        [SecondaryKey("Enums", Order = 1)] public E8 A { get; set; }
        [SecondaryKey("Enums", Order = 2)] public E16 B { get; set; }
        [SecondaryKey("Enums", Order = 3)] public E64 C { get; set; }
        [SecondaryKey("Enums", Order = 4)] public EU8 D { get; set; }
        [SecondaryKey("Enums", Order = 5)] public EU32 E { get; set; }
        [SecondaryKey("Enums", Order = 6)] public EU64 F { get; set; }

        public override string ToString() => $"{I8}|{I16}|{I32}|{U8}|{U16}|{U32}|{A}|{B}|{C}|{D}|{E}|{F}";
    }

    public interface IIntKeysTable : IRelation<IntKeys>
    {
        IEnumerable<IntKeys> ScanById(Constraint<sbyte> i8, Constraint<short> i16, Constraint<int> i32,
            Constraint<byte> u8, Constraint<ushort> u16, Constraint<uint> u32);

        IEnumerable<IntKeys> ScanByEnums(Constraint<E8> a, Constraint<E16> b, Constraint<E64> c, Constraint<EU8> d,
            Constraint<EU32> e, Constraint<EU64> f);
    }

    public class NullableKeys
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("N", Order = 1)] public int? NI { get; set; }
        [SecondaryKey("N", Order = 2)] public bool? NB { get; set; }
        [SecondaryKey("N", Order = 3)] public DateTime? ND { get; set; }
        [SecondaryKey("N", Order = 4)] public Guid? NG { get; set; }
        [SecondaryKey("N", Order = 5)] public E16? NE { get; set; }
        [SecondaryKey("N", Order = 6)] public ulong? NU { get; set; }

        [SecondaryKey("M", Order = 1)] public sbyte? M8 { get; set; }
        [SecondaryKey("M", Order = 2)] public short? M16 { get; set; }
        [SecondaryKey("M", Order = 3)] public long? M64 { get; set; }
        [SecondaryKey("M", Order = 4)] public byte? MU8 { get; set; }
        [SecondaryKey("M", Order = 5)] public ushort? MU16 { get; set; }
        [SecondaryKey("M", Order = 6)] public uint? MU32 { get; set; }

        [SecondaryKey("O", Order = 1)] public E8? O8 { get; set; }
        [SecondaryKey("O", Order = 2)] public E64? O64 { get; set; }
        [SecondaryKey("O", Order = 3)] public EU8? OU8 { get; set; }
        [SecondaryKey("O", Order = 4)] public EU32? OU32 { get; set; }
        [SecondaryKey("O", Order = 5)] public EU64? OU64 { get; set; }
        [SecondaryKey("O", Order = 6)] public int? OI { get; set; }

        public override string ToString() =>
            $"{Id}|{NI}|{NB}|{ND:O}|{NG}|{NE}|{NU}|{M8}|{M16}|{M64}|{MU8}|{MU16}|{MU32}|{O8}|{O64}|{OU8}|{OU32}|{OU64}|{OI}";
    }

    public interface INullableKeysTable : IRelation<NullableKeys>
    {
        IEnumerable<NullableKeys> ScanByN(Constraint<int?> ni, Constraint<bool?> nb, Constraint<DateTime?> nd,
            Constraint<Guid?> ng, Constraint<E16?> ne, Constraint<ulong?> nu);

        IEnumerable<NullableKeys> ScanByM(Constraint<sbyte?> m8, Constraint<short?> m16, Constraint<long?> m64,
            Constraint<byte?> mu8, Constraint<ushort?> mu16, Constraint<uint?> mu32);

        IEnumerable<NullableKeys> ScanByO(Constraint<E8?> o8, Constraint<E64?> o64, Constraint<EU8?> ou8,
            Constraint<EU32?> ou32, Constraint<EU64?> ou64, Constraint<int?> oi);
    }

    static void RunInMode(bool noEmit, Action action)
    {
        var old = IFieldHandler.UseNoEmitForRelations;
        try
        {
            IFieldHandler.UseNoEmitForRelations = noEmit;
            ObjectDB.ResetAllMetadataCaches();
            action();
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = old;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    // Returns constraint and reference predicate: Any, Exact for value from data and Exact for value maybe missing
    static (Constraint<T> Constraint, Func<T, bool> Predicate)[] Choices<T>(T present, T other)
    {
        var cmp = EqualityComparer<T>.Default;
        return
        [
            (Constraint<T>.Any, _ => true),
            (Constraint.Exact(present), v => cmp.Equals(v, present)),
            (Constraint.Exact(other), v => cmp.Equals(v, other))
        ];
    }

    static T Pick<T>(Random rnd, T[] values) => values[rnd.Next(values.Length)];

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void IntegerAndEnumKeysWithGenericConstraints(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            var rnd = new Random(42);
            var rows = new Dictionary<string, IntKeys>();
            for (var i = 0; i < 150; i++)
            {
                var row = new IntKeys
                {
                    I8 = Pick(rnd, new sbyte[] { sbyte.MinValue, -1, 0, sbyte.MaxValue }),
                    I16 = Pick(rnd, new short[] { short.MinValue, 0, short.MaxValue }),
                    I32 = Pick(rnd, new[] { int.MinValue, 0, int.MaxValue }),
                    U8 = Pick(rnd, new byte[] { 0, byte.MaxValue }),
                    U16 = Pick(rnd, new ushort[] { 0, ushort.MaxValue }),
                    U32 = Pick(rnd, new[] { 0u, uint.MaxValue }),
                    A = Pick(rnd, Enum.GetValues<E8>()),
                    B = Pick(rnd, Enum.GetValues<E16>()),
                    C = Pick(rnd, Enum.GetValues<E64>()),
                    D = Pick(rnd, Enum.GetValues<EU8>()),
                    E = Pick(rnd, Enum.GetValues<EU32>()),
                    F = Pick(rnd, Enum.GetValues<EU64>()),
                };
                rows.TryAdd($"{row.I8}|{row.I16}|{row.I32}|{row.U8}|{row.U16}|{row.U32}", row);
            }

            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IIntKeysTable>();
            foreach (var row in rows.Values) table.Upsert(row);
            var data = rows.Values.ToList();
            var pkOrder = data.OrderBy(r => r.I8).ThenBy(r => r.I16).ThenBy(r => r.I32).ThenBy(r => r.U8)
                .ThenBy(r => r.U16).ThenBy(r => r.U32).ToList();
            var skOrder = data.OrderBy(r => r.A).ThenBy(r => r.B).ThenBy(r => r.C).ThenBy(r => r.D).ThenBy(r => r.E)
                .ThenBy(r => r.F).ThenBy(r => r.I8).ThenBy(r => r.I16).ThenBy(r => r.I32).ThenBy(r => r.U8)
                .ThenBy(r => r.U16).ThenBy(r => r.U32).ToList();
            for (var i = 0; i < 60; i++)
            {
                var sample = data[rnd.Next(data.Count)];
                var c8 = Pick(rnd, Choices(sample.I8, (sbyte)5));
                var c16 = Pick(rnd, Choices(sample.I16, (short)-5));
                var c32 = Pick(rnd, Choices(sample.I32, 7));
                var cu8 = Pick(rnd, Choices(sample.U8, (byte)7));
                var cu16 = Pick(rnd, Choices(sample.U16, (ushort)7));
                var cu32 = Pick(rnd, Choices(sample.U32, 7u));
                var expected = pkOrder.Where(r => c8.Predicate(r.I8) && c16.Predicate(r.I16) &&
                                                  c32.Predicate(r.I32) && cu8.Predicate(r.U8) &&
                                                  cu16.Predicate(r.U16) && cu32.Predicate(r.U32));
                Assert.Equal(string.Join("\n", expected),
                    string.Join("\n", table.ScanById(c8.Constraint, c16.Constraint, c32.Constraint, cu8.Constraint,
                        cu16.Constraint, cu32.Constraint)));

                var ca = Pick(rnd, Choices(sample.A, (E8)3));
                var cb = Pick(rnd, Choices(sample.B, (E16)3));
                var cc = Pick(rnd, Choices(sample.C, (E64)3));
                var cd = Pick(rnd, Choices(sample.D, (EU8)3));
                var ce = Pick(rnd, Choices(sample.E, (EU32)3));
                var cf = Pick(rnd, Choices(sample.F, (EU64)3));
                var expectedSk = skOrder.Where(r => ca.Predicate(r.A) && cb.Predicate(r.B) && cc.Predicate(r.C) &&
                                                    cd.Predicate(r.D) && ce.Predicate(r.E) && cf.Predicate(r.F));
                Assert.Equal(string.Join("\n", expectedSk),
                    string.Join("\n", table.ScanByEnums(ca.Constraint, cb.Constraint, cc.Constraint, cd.Constraint,
                        ce.Constraint, cf.Constraint)));
            }
        });
    }

    static IOrderedEnumerable<NullableKeys> ThenByNullable<T>(IOrderedEnumerable<NullableKeys> source,
        Func<NullableKeys, T?> selector) where T : struct =>
        source.ThenBy(r => selector(r).HasValue).ThenBy(r => selector(r).GetValueOrDefault());

    static T? MaybeNull<T>(Random rnd, T[] values) where T : struct =>
        rnd.Next(3) == 0 ? null : values[rnd.Next(values.Length)];

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void NullableKeysWithGenericConstraints(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            var rnd = new Random(7);
            var dates = new[]
            {
                new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc)
            };
            var guids = new[] { Guid.Empty, new Guid("01000000-0000-0000-0000-000000000000") };
            var data = new List<NullableKeys>();
            for (var i = 0; i < 120; i++)
            {
                data.Add(new()
                {
                    Id = (ulong)i,
                    NI = MaybeNull(rnd, new[] { int.MinValue, 0, 1 }),
                    NB = MaybeNull(rnd, new[] { false, true }),
                    ND = MaybeNull(rnd, dates),
                    NG = MaybeNull(rnd, guids),
                    NE = MaybeNull(rnd, Enum.GetValues<E16>()),
                    NU = MaybeNull(rnd, new[] { 0ul, ulong.MaxValue }),
                    M8 = MaybeNull(rnd, new sbyte[] { 1, 2 }),
                    M16 = MaybeNull(rnd, new short[] { -1, 1 }),
                    M64 = MaybeNull(rnd, new[] { long.MinValue, 1 }),
                    MU8 = MaybeNull(rnd, new byte[] { 0, 1 }),
                    MU16 = MaybeNull(rnd, new ushort[] { 0, 1 }),
                    MU32 = MaybeNull(rnd, new uint[] { 0, 1 }),
                    O8 = MaybeNull(rnd, Enum.GetValues<E8>()),
                    O64 = MaybeNull(rnd, Enum.GetValues<E64>()),
                    OU8 = MaybeNull(rnd, Enum.GetValues<EU8>()),
                    OU32 = MaybeNull(rnd, Enum.GetValues<EU32>()),
                    OU64 = MaybeNull(rnd, Enum.GetValues<EU64>()),
                    OI = MaybeNull(rnd, new[] { 1, 2 }),
                });
            }

            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<INullableKeysTable>();
            foreach (var row in data) table.Upsert(row);
            var nOrder = ThenByNullable(ThenByNullable(ThenByNullable(ThenByNullable(ThenByNullable(ThenByNullable(
                                data.OrderBy(_ => 0), r => r.NI), r => r.NB), r => r.ND), r => r.NG), r => r.NE),
                            r => r.NU)
                .ThenBy(r => r.Id).ToList();
            var mOrder = ThenByNullable(ThenByNullable(ThenByNullable(ThenByNullable(ThenByNullable(ThenByNullable(
                                data.OrderBy(_ => 0), r => r.M8), r => r.M16), r => r.M64), r => r.MU8),
                            r => r.MU16), r => r.MU32)
                .ThenBy(r => r.Id).ToList();
            var oOrder = ThenByNullable(ThenByNullable(ThenByNullable(ThenByNullable(ThenByNullable(ThenByNullable(
                                data.OrderBy(_ => 0), r => r.O8), r => r.O64), r => r.OU8), r => r.OU32),
                            r => r.OU64), r => r.OI)
                .ThenBy(r => r.Id).ToList();
            for (var i = 0; i < 80; i++)
            {
                var s = data[rnd.Next(data.Count)];
                var cni = Pick(rnd, Choices(s.NI, null));
                var cnb = Pick(rnd, Choices(s.NB, null));
                var cnd = Pick(rnd, Choices(s.ND, null));
                var cng = Pick(rnd, Choices(s.NG, null));
                var cne = Pick(rnd, Choices(s.NE, null));
                var cnu = Pick(rnd, Choices(s.NU, 5ul));
                var expected = nOrder.Where(r => cni.Predicate(r.NI) && cnb.Predicate(r.NB) && cnd.Predicate(r.ND) &&
                                                 cng.Predicate(r.NG) && cne.Predicate(r.NE) && cnu.Predicate(r.NU));
                Assert.True(string.Join("\n", expected) == string.Join("\n",
                    table.ScanByN(cni.Constraint, cnb.Constraint, cnd.Constraint, cng.Constraint, cne.Constraint,
                        cnu.Constraint)), $"ScanByN {s} {i}");

                var cm8 = Pick(rnd, Choices(s.M8, null));
                var cm16 = Pick(rnd, Choices(s.M16, null));
                var cm64 = Pick(rnd, Choices(s.M64, null));
                var cmu8 = Pick(rnd, Choices(s.MU8, null));
                var cmu16 = Pick(rnd, Choices(s.MU16, null));
                var cmu32 = Pick(rnd, Choices(s.MU32, 7u));
                var expectedM = mOrder.Where(r => cm8.Predicate(r.M8) && cm16.Predicate(r.M16) &&
                                                  cm64.Predicate(r.M64) && cmu8.Predicate(r.MU8) &&
                                                  cmu16.Predicate(r.MU16) && cmu32.Predicate(r.MU32));
                Assert.Equal(string.Join("\n", expectedM), string.Join("\n",
                    table.ScanByM(cm8.Constraint, cm16.Constraint, cm64.Constraint, cmu8.Constraint,
                        cmu16.Constraint, cmu32.Constraint)));

                var co8 = Pick(rnd, Choices(s.O8, null));
                var co64 = Pick(rnd, Choices(s.O64, null));
                var cou8 = Pick(rnd, Choices(s.OU8, null));
                var cou32 = Pick(rnd, Choices(s.OU32, null));
                var cou64 = Pick(rnd, Choices(s.OU64, null));
                var coi = Pick(rnd, Choices(s.OI, 3));
                var expectedO = oOrder.Where(r => co8.Predicate(r.O8) && co64.Predicate(r.O64) &&
                                                  cou8.Predicate(r.OU8) && cou32.Predicate(r.OU32) &&
                                                  cou64.Predicate(r.OU64) && coi.Predicate(r.OI));
                Assert.Equal(string.Join("\n", expectedO), string.Join("\n",
                    table.ScanByO(co8.Constraint, co64.Constraint, cou8.Constraint, cou32.Constraint,
                        cou64.Constraint, coi.Constraint)));
            }
        });
    }

    [Fact]
    public void UnsupportedConstraintTypesThrow()
    {
        Assert.Throws<NotSupportedException>(() => Constraint.Exact(1.5));
        Assert.Throws<NotSupportedException>(() => Constraint.Exact<double?>(1.5));
        Assert.Throws<NotImplementedException>(() => Constraint<double>.Any.IsSimpleExact());
        Assert.Throws<NotImplementedException>(() => Constraint<double?>.Any.IsSimpleExact());
        Assert.True(Constraint<List<string>>.Any.IsAnyConstraint());
        Assert.True(Constraint<IList<ulong>>.Any.IsAnyConstraint());
        Assert.True(Constraint<bool?>.Any.IsAnyConstraint());
        Assert.False(Constraint<bool?>.Any.IsSimpleExact());
        Assert.True(Constraint.Exact<bool?>(null).IsSimpleExact());
        Assert.True(Constraint.Exact<bool?>(true).IsSimpleExact());
        Assert.False(Constraint.First(Constraint.Unsigned.Any).IsAnyConstraint());
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Constraint.DateTime.Exact(new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Unspecified)));
        var local = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Local);
        Assert.Throws<ArgumentOutOfRangeException>(() => Constraint.DateTime.ForbidUnspecifiedKind(local));
        Assert.NotEqual(DateTimeKind.Unspecified, Constraint.DateTime.ForbidUnspecifiedKind(DateTime.MinValue).Kind);
        Assert.NotEqual(DateTimeKind.Unspecified, Constraint.DateTime.ForbidUnspecifiedKind(DateTime.MaxValue).Kind);
    }
}
