using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.Serialization;
using BTDB.StreamLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

/// Field handlers used in ordered (primary/secondary key) position of relations, in both IL emitted and NoEmit modes.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class RelationKeyFieldHandlerTest : FieldHandlerCoverageTestBase
{
    public enum Kind : short
    {
        A = -1,
        B = 5
    }

    public class ByteKeyRow
    {
        [PrimaryKey(1)] public byte[] Key { get; set; } = null!;
        [SecondaryKey("Mem")] public ReadOnlyMemory<byte> Mem { get; set; }
        public byte[]? Value { get; set; }
    }

    public interface IByteKeyTable : IRelation<ByteKeyRow>
    {
        void Insert(ByteKeyRow row);
        ByteKeyRow? FindByIdOrDefault(byte[] key);
        bool RemoveById(byte[] key);
        IEnumerable<ByteKeyRow> FindByMem(ReadOnlyMemory<byte> mem);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ByteArrayAndReadOnlyMemoryKeys(bool emit)
    {
        using var _ = EmitMode(emit);
        var keys = new[] { new byte[] { 2 }, new byte[] { 1, 2 }, Array.Empty<byte>(), new byte[] { 1 } };
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IByteKeyTable>();
            foreach (var key in keys)
                table.Insert(new ByteKeyRow { Key = key, Mem = key.Concat(new byte[] { 9 }).ToArray(), Value = key });
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IByteKeyTable>();
            Assert.Equal(4, table.Count);
            foreach (var key in keys)
            {
                var row = table.FindByIdOrDefault(key);
                Assert.NotNull(row);
                Assert.Equal(key, row!.Value);
                Assert.Equal(key, table.FindByMem(key.Concat(new byte[] { 9 }).ToArray()).Single().Key);
            }

            Assert.Null(table.FindByIdOrDefault(new byte[] { 3 }));
            Assert.True(table.RemoveById(new byte[] { 1 }));
            Assert.False(table.RemoveById(new byte[] { 1 }));
            Assert.Equal(3, table.Count);
            Assert.Equal(3, table.Select(r => Convert.ToHexString(r.Key)).Distinct().Count());
        }
    }

    public class SecretKeyRow
    {
        [PrimaryKey(1)] public ulong Tenant { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        [SecondaryKey("Other")] public EncryptedString Other { get; set; }
        [SecondaryKey("Pair", Order = 2)] public EncryptedString Pair2 { get; set; }
        [SecondaryKey("Pair", Order = 1)] public int Pair1 { get; set; }
        public int Value { get; set; }
    }

    public interface ISecretKeyTable : IRelation<SecretKeyRow>
    {
        void Insert(SecretKeyRow row);
        IEnumerable<SecretKeyRow> FindByOther(EncryptedString other);
        IEnumerable<SecretKeyRow> FindByPair(int pair1, EncryptedString pair2);
        IEnumerable<SecretKeyRow> FindByPair(int pair1);
        bool RemoveById(ulong tenant, ulong id);
    }

    public class SecretKeyRowAsString
    {
        [PrimaryKey(1)] public ulong Tenant { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        [SecondaryKey("Other")] public string Other { get; set; } = "";
        public int Value { get; set; }
    }

    public interface ISecretKeyTableAsString : IRelation<SecretKeyRowAsString>
    {
        IEnumerable<SecretKeyRowAsString> FindByOther(string other);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void EncryptedStringSecondaryKeys(bool emit)
    {
        using var _ = EmitMode(emit);
        using (var tr = _db.StartTransaction())
        {
            var table = tr.InitRelation<ISecretKeyTable>("SecretKeys")(tr);
            for (var i = 0; i < 5; i++)
                table.Insert(new SecretKeyRow
                    { Tenant = 1, Id = (ulong)i, Other = "o" + i, Pair1 = i % 2, Pair2 = "p" + i, Value = i });
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var table = tr.InitRelation<ISecretKeyTable>("SecretKeys")(tr);
            Assert.Equal(4, table.FindByOther("o4").Single().Value);
            Assert.Empty(table.FindByOther("o9"));
            Assert.Equal(3, table.FindByPair(1, "p3").Single().Value);
            Assert.Equal(new[] { 0, 2, 4 }, table.FindByPair(0).Select(r => r.Value).OrderBy(v => v));
            Assert.True(table.RemoveById(1, 2));
            Assert.Equal(2, table.FindByPair(0).Count());
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var table = tr.InitRelation<ISecretKeyTableAsString>("SecretKeys")(tr);
            var row = table.FindByOther("o3").Single();
            Assert.Equal("o3", row.Other);
            Assert.Equal(3, row.Value);
        }
    }

    public class ComplexKeyRow
    {
        [PrimaryKey(1)] public (int, string) Pair { get; set; }
        [PrimaryKey(2)] public int Id { get; set; }
        public List<int> Path { get; set; } = null!;
        [SecondaryKey("Tags")] public List<string> Tags { get; set; } = null!;
        [SecondaryKey("Kind")] public Kind Kind { get; set; }
        [SecondaryKey("Maybe")] public int? Maybe { get; set; }
        public int Value { get; set; }
    }

    public interface IComplexKeyTable : IRelation<ComplexKeyRow>
    {
        void Insert(ComplexKeyRow row);
        ComplexKeyRow? FindByIdOrDefault((int, string) pair, int id);
        IEnumerable<ComplexKeyRow> ListById(AdvancedEnumeratorParam<(int, string)> param);
        IEnumerable<ComplexKeyRow> FindByTags(List<string> tags);
        IEnumerable<ComplexKeyRow> FindByKind(Kind kind);
        IEnumerable<ComplexKeyRow> FindByMaybe(int? maybe);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TupleListEnumAndNullableKeys(bool emit)
    {
        using var _ = EmitMode(emit);
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IComplexKeyTable>();
            table.Insert(new ComplexKeyRow
            {
                Pair = (2, "b"), Id = 1, Path = new List<int> { 1, 2 }, Tags = new List<string> { "x" }, Kind = Kind.A,
                Maybe = null, Value = 1
            });
            table.Insert(new ComplexKeyRow
            {
                Pair = (1, "a"), Id = 1, Path = new List<int>(), Tags = new List<string> { "y", "z" }, Kind = Kind.B,
                Maybe = 5, Value = 2
            });
            table.Insert(new ComplexKeyRow
            {
                Pair = (1, "a"), Id = 2, Path = new List<int> { 3 }, Tags = new List<string>(), Kind = Kind.B, Maybe = -5,
                Value = 3
            });
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IComplexKeyTable>();
            Assert.Equal(1, table.FindByIdOrDefault((2, "b"), 1)!.Value);
            Assert.Equal(2, table.FindByIdOrDefault((1, "a"), 1)!.Value);
            Assert.Null(table.FindByIdOrDefault((1, "a"), 4));
            Assert.Equal(new[] { 2, 3, 1 },
                table.ListById(new AdvancedEnumeratorParam<(int, string)>()).Select(r => r.Value));
            Assert.Equal(new[] { 1 },
                table.ListById(new AdvancedEnumeratorParam<(int, string)>(EnumerationOrder.Ascending, (1, "a"),
                    KeyProposition.Excluded, default, KeyProposition.Ignored)).Select(r => r.Value));
            Assert.Equal(2, table.FindByTags(new List<string> { "y", "z" }).Single().Value);
            Assert.Equal(1, table.FindByKind(Kind.A).Single().Value);
            Assert.Equal(2, table.FindByKind(Kind.B).Count());
            Assert.Equal(1, table.FindByMaybe(null).Single().Value);
            Assert.Equal(3, table.FindByMaybe(-5).Single().Value);
            var row = table.First(r => r.Value == 1);
            Assert.Equal(new[] { 1, 2 }, row.Path);
            Assert.Equal(new[] { "x" }, row.Tags);
        }
    }

    [Fact]
    public void FloatsAreForbiddenInOrderableFields()
    {
        // source generator already reports BTDB0035 for float/double in indexes, runtime check is the last defence
        var factory = _db.FieldHandlerFactory;
        Assert.Throws<NotSupportedException>(() => factory.CreateFromType(typeof(double), FieldHandlerOptions.Orderable));
        Assert.Throws<NotSupportedException>(() => factory.CreateFromType(typeof(float), FieldHandlerOptions.Orderable));
        // A decimal key would sort by its scale first, and 1.0 and 1.00 would be different keys.
        Assert.Throws<NotSupportedException>(() => factory.CreateFromType(typeof(decimal), FieldHandlerOptions.Orderable));
        Assert.Throws<NotSupportedException>(() =>
            factory.CreateFromType(typeof(decimal?), FieldHandlerOptions.Orderable));
        Assert.NotNull(factory.CreateFromType(typeof(decimal), FieldHandlerOptions.None));
        Assert.NotNull(factory.CreateFromType(typeof(decimal?), FieldHandlerOptions.None));
        Assert.NotNull(factory.CreateFromType(typeof(double), FieldHandlerOptions.None));
        var handler = new ForbidOrderableFloatsFieldHandler();
        Assert.Equal("ForbidOrderableFloats", handler.Name);
        Assert.Null(handler.Configuration);
        Assert.Null(handler.HandledType());
        Assert.False(handler.NeedsCtx());
        Assert.False(handler.IsCompatibleWith(typeof(int), FieldHandlerOptions.Orderable));
        Assert.False(handler.IsCompatibleWith(typeof(double), FieldHandlerOptions.None));
        Assert.Throws<NotSupportedException>(() => handler.Load(null!, _ => { }, null));
        Assert.Throws<NotSupportedException>(() => handler.Skip(null!, _ => { }, null));
        Assert.Throws<NotSupportedException>(() => handler.Save(null!, _ => { }, null, _ => { }));
        Assert.Throws<NotSupportedException>(() => handler.Load(typeof(double), DefaultTypeConverterFactory.Instance));
        Assert.Throws<NotSupportedException>(() => handler.Save(typeof(double), DefaultTypeConverterFactory.Instance));
        Assert.Throws<NotSupportedException>(() =>
        {
            var reader = new MemReader();
            handler.Skip(ref reader, null);
        });
        Assert.Throws<NotSupportedException>(() =>
        {
            var reader = new MemReader();
            handler.FreeContent(ref reader, null);
        });
        Assert.Throws<NotSupportedException>(() => handler.DoesNeedFreeContent(new HashSet<Type>()));
        Assert.Throws<NotSupportedException>(() => handler.SpecializeLoadForType(typeof(double), null, null));
        Assert.Throws<NotSupportedException>(() => handler.SpecializeSaveForType(typeof(double)));
    }
}
