using Xunit;

namespace BTDB.SourceGenerator.Tests;

/// Covers metadata generation and relation item attribute parsing edge cases.
public class MetadataCoverageTests : GeneratorTestsBase
{
    [Fact]
    public void AttributeArgumentsInAllSupportedForms()
    {
        var sources = GenerateAndCompile("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public static class Consts
            {
                public const uint Two = 2;
                public const string IndexName = "ByName";
            }

            public class Item
            {
                [PrimaryKey(1u)] public ulong Id { get; set; }
                [PrimaryKey(Consts.Two, false)] public ulong SubId { get; set; }
                [PrimaryKey(order: 3, inKeyValue: true)] public ulong Version { get; set; }
                [SecondaryKey(nameof(Consts.IndexName), Order = 3u)] public string Name { get; set; } = null!;
                [SecondaryKey(nameof(Consts.IndexName), Order = Consts.Two, IncludePrimaryKeyOrder = 1)]
                public int Age { get; set; }
            }

            public interface IItemTable : IRelation<Item>
            {
                Item FindByIndexName(ulong id, int age, string name);
            }
            """);
        var relation = GetSource(sources, "TestNamespace.IItemTable.g.cs");
        Assert.Contains("//           PrimaryIndex: 1\n", relation);
        Assert.Contains("//           PrimaryIndex: 2\n", relation);
        Assert.Contains("//           PrimaryIndex: 3 InKeyValue\n", relation);
        Assert.Contains("//           SecondaryIndex IndexName: 3\n", relation);
        Assert.Contains("//           SecondaryIndex IndexName: 2 IncludePrimaryKeyOrder 1\n", relation);
        var metadata = GetSource(sources, "TestNamespace.Item.g.cs");
        Assert.Contains("metadata.IndexOfInKeyValue = 2;", metadata);
        Assert.Contains("metadata.SecondaryKeys = [(\"IndexName\", [0, 4, 3, 1])];", metadata);
    }

    [Fact]
    public void RelationWithDescendingInKeyValuesAndAdvancedListById()
    {
        var sources = GenerateAndCompile("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Item
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                [PrimaryKey(2)] public ulong SubId { get; set; }
                [InKeyValue(4)] public int B { get; set; }
                [InKeyValue(3)] public int A { get; set; }
            }

            public interface IItemTable : IRelation<Item>
            {
                IEnumerable<Item> ListById(ulong id, AdvancedEnumeratorParam<ulong> subId);
            }
            """);
        var metadata = GetSource(sources, "TestNamespace.Item.g.cs");
        Assert.Contains("metadata.PrimaryKeyFields = [0, 1, 3, 2];", metadata);
        Assert.Contains("metadata.IndexOfInKeyValue = 2;", metadata);
    }

    [Fact]
    public void ContractAttributesFromInterfaceArePreferred()
    {
        var sources = GenerateAndCompile("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public interface IKeyed
            {
                [PrimaryKey(1)] ulong Id { get; set; }
            }

            public class Item : IKeyed
            {
                public ulong Id { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IItemTable : IRelation<Item>
            {
                Item FindById(ulong id);
            }
            """);
        var metadata = GetSource(sources, "TestNamespace.Item.g.cs");
        Assert.Contains("metadata.PrimaryKeyFields = [0];", metadata);
    }

    [Fact]
    public void GenericClassWithNonPublicCallbacksAndInterfaceConstraint()
    {
        // Accessors of non-public callbacks in generic classes are declared in nested Activator class, so they must be
        // public, otherwise generated code does not compile (CS0122).
        var sources = GenerateAndCompile("""
            using System;
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Holder<T> where T : class, IComparable, new()
            {
                public T? Value { get; set; }

                [OnSerialize]
                void Serializing()
                {
                }

                [OnBeforeRemove]
                bool BeforeRemove(IObjectDBTransaction transaction)
                {
                    return false;
                }
            }

            public class Data : IComparable
            {
                public int CompareTo(object? obj) => 0;
            }

            [BTDB.Generate]
            public class Root
            {
                public Holder<Data> Holder { get; set; } = null!;
            }
            """);
        var code = GetSource(sources, "TestNamespace.Holder[TestNamespace.Data].g.cs");
        Assert.Contains("where T: class, global::System.IComparable, new()", code);
        Assert.Contains("extern public static void OnSerialize1(global::TestNamespace.Holder<T> @this);", code);
        Assert.Contains(
            "extern public static bool OnBeforeRemove1(global::TestNamespace.Holder<T> @this, global::BTDB.ODBLayer.IObjectDBTransaction transaction);",
            code);
    }

    [Fact]
    public void UpdateByIdValuesOfAllSupportedTypes()
    {
        var sources = GenerateAndCompile("""
            using System;
            using BTDB.Buffer;
            using BTDB.Encrypted;
            using BTDB.ODBLayer;
            using Microsoft.Extensions.Primitives;

            namespace TestNamespace;

            public enum Flags : uint { None, One }

            public class Item
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                public bool Flag { get; set; }
                public byte U8 { get; set; }
                public sbyte S8 { get; set; }
                public ushort U16 { get; set; }
                public string? Note { get; set; }
                public Flags Flags { get; set; }
                public DateTime When { get; set; }
                public DateTimeOffset WhenOffset { get; set; }
                public TimeSpan Duration { get; set; }
                public Guid Guid { get; set; }
                public byte[] Bytes { get; set; } = null!;
                public ByteBuffer Buffer { get; set; }
                public ReadOnlyMemory<byte> Memory { get; set; }
                public StringValues Values { get; set; }
                public EncryptedString Secret { get; set; }
            }

            public interface IItemTable : IRelation<Item>
            {
                void UpdateByIdNote(ulong id, string? note, Flags flags);
                void UpdateByIdSmall(ulong id, bool flag, byte u8, sbyte s8, ushort u16);
                void UpdateByIdAll(ulong id, DateTime when, DateTimeOffset whenOffset, TimeSpan duration, Guid guid,
                    byte[] bytes, ByteBuffer buffer, ReadOnlyMemory<byte> memory, StringValues values,
                    EncryptedString secret);
            }
            """);
        var code = GetSource(sources, "TestNamespace.IItemTable.g.cs");
        Assert.Contains("reader.Skip1Byte();", code);
        Assert.Contains("reader.SkipVUInt64();", code);
        Assert.Contains("writer.WriteString(note);", code);
        Assert.Contains("writer.WriteBool(flag);", code);
        Assert.Contains("writer.WriteUInt8(u8);", code);
        Assert.Contains("writer.WriteInt8(s8);", code);
        Assert.Contains("writer.WriteVUInt64(u16);", code);
        Assert.Contains("writer.WriteVUInt64((ulong)flags);", code);
        Assert.Contains("writer.WriteDateTime(when);", code);
        Assert.Contains("writer.WriteDateTimeOffset(whenOffset);", code);
        Assert.Contains("writer.WriteTimeSpan(duration);", code);
        Assert.Contains("writer.WriteGuid(guid);", code);
        Assert.Contains("writer.WriteByteArray(bytes);", code);
        Assert.Contains("writer.WriteByteArray(buffer);", code);
        Assert.Contains("writer.WriteByteArray(memory);", code);
        Assert.Contains("writer.WriteStringValues(values);", code);
        Assert.Contains("ctx_ctx.WriteEncryptedString(ref writer, secret);", code);
        Assert.DoesNotContain("NotSupportedException", code);
    }

    [Fact]
    public void ExternInterfaceMethodIsNotImplemented()
    {
        var sources = GenerateAndCompile("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Item
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
            }

            public interface IItemTable : IRelation<Item>
            {
                extern void Helper();
            }
            """);
        Assert.DoesNotContain("Helper", GetSource(sources, "TestNamespace.IItemTable.g.cs"));
    }

    [Fact]
    public void LongestSecondaryKeyNameWinsOverOrDefaultSuffix()
    {
        var sources = GenerateAndCompile("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Item
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                [SecondaryKey("NameOrDefault")] public string A { get; set; } = null!;
                [SecondaryKey("Name")] public string B { get; set; } = null!;
            }

            public interface IItemTable : IRelation<Item>
            {
                Item FindByNameOrDefault(string a);
            }
            """);
        var code = GetSource(sources, "TestNamespace.IItemTable.g.cs");
        Assert.Contains("var remappedSecondaryKeyIndex = RemapPrimeSK(0u);", code);
        Assert.Contains("writer.GetSpan(), true, 0);", code);
    }

    [Fact]
    public void UpdateByIdWithValueTypeWithoutWriterThrowsAtRuntime()
    {
        // Documents current behavior: value types other than supported primitives pass validation and generated code
        // throws NotSupportedException when called.
        var sources = GenerateAndCompile("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Item
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                public int[] Numbers { get; set; } = null!;
            }

            public interface IItemTable : IRelation<Item>
            {
                void UpdateById(ulong id, int[] numbers);
            }
            """);
        Assert.Contains("throw new NotSupportedException(\"Value does not support type 'int[]'.\");",
            GetSource(sources, "TestNamespace.IItemTable.g.cs"));
    }
}
