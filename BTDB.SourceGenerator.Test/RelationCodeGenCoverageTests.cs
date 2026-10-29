using System.Threading.Tasks;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

/// Covers code generation paths of relation interface implementations which are not exercised by snapshot tests.
public class RelationCodeGenCoverageTests : GeneratorTestsBase
{
    internal const string RelationSource = """
        using System;
        using System.Collections.Generic;
        using BTDB.ODBLayer;

        namespace TestNamespace;

        public class Person
        {
            [PrimaryKey(1)] public ulong TenantId { get; set; }
            [PrimaryKey(2)] public ulong Id { get; set; }
            [SecondaryKey("Name")] public string Name { get; set; } = null!;
            [SecondaryKey("Age", IncludePrimaryKeyOrder = 1)] public int Age { get; set; }
            public string? Note { get; set; }
        }

        public class PersonName
        {
            public string Name { get; set; } = null!;
        }

        public interface IPersonTable : IRelation<Person>
        {
            Person FindByName(string name);
            Person? FindByNameOrDefault(string name);
            IEnumerable<Person> FindByAge(ulong tenantId, int age);
            IEnumerable<PersonName> FindById(ulong tenantId);
            PersonName FindByIdOrDefault(ulong tenantId, ulong id);
            IEnumerable<PersonName> ListByName(string name);
            IOrderedDictionaryEnumerator<ulong, Person> ListById(ulong tenantId, AdvancedEnumeratorParam<ulong> id);
            IOrderedDictionaryEnumerator<int, Person> ListByAge(ulong tenantId, AdvancedEnumeratorParam<int> age);
            IEnumerable<Person> ListById(AdvancedEnumeratorParam<ulong> tenantId);
            bool AnyById(ulong tenantId);
            bool AnyByAge(ulong tenantId, int age);
            bool AnyByAge(ulong tenantId, AdvancedEnumeratorParam<int> age);
            bool AnyById(AdvancedEnumeratorParam<ulong> tenantId);
            long CountById(ulong tenantId);
            int CountByAge(ulong tenantId, int age);
            uint CountByName(AdvancedEnumeratorParam<string> name);
            ulong CountById(AdvancedEnumeratorParam<ulong> tenantId);
            Person LastById(Constraint<ulong> tenantId);
            Person? LastByIdOrDefault(Constraint<ulong> tenantId, IOrderer[] orderers);
            Person FirstByAge(Constraint<ulong> tenantId, Constraint<int> age);
            ulong GatherByAge(List<Person> items, long skip, long take, Constraint<ulong> tenantId, IOrderer[]? orderers);
            IEnumerable<Person> ScanByName(Constraint<string> name);
            void RemoveById(AdvancedEnumeratorParam<ulong> tenantId);
            ulong RemoveById(ulong tenantId, AdvancedEnumeratorParam<ulong> id);
            int RemoveByIdPartial(ulong tenantId, int maxCount);
            bool ShallowRemoveById(ulong tenantId, ulong id);
            void IterateById(ulong tenantId, Action<PersonName> callback, PersonName value);
        }
        """;

    [Fact]
    public void GeneratesSecondaryKeyAndAdvancedQueries()
    {
        var sources = GenerateAndCompile(RelationSource);
        var code = GetSource(sources, "TestNamespace.IPersonTable.g.cs");
        Assert.Contains(
            "return base.FindBySecondaryKeyOrDefault<global::TestNamespace.Person>(remappedSecondaryKeyIndex, writer.GetSpan(), true, 0);",
            code);
        Assert.Contains(
            "return base.FindBySecondaryKeyOrDefault<global::TestNamespace.Person>(remappedSecondaryKeyIndex, writer.GetSpan(), false, 0);",
            code);
        Assert.Contains(
            "return (global::System.Collections.Generic.IEnumerable<global::TestNamespace.Person>)base.FindBySecondaryKey<global::TestNamespace.Person>(remappedSecondaryKeyIndex, writer.GetSpan(), 0);",
            code);
        Assert.Contains(
            "return (global::System.Collections.Generic.IEnumerable<global::TestNamespace.PersonName>)base.FindByPrimaryKeyPrefix<global::TestNamespace.PersonName>(writer.GetSpan(), 1);",
            code);
        Assert.Contains("return base.FindByIdOrDefault<global::TestNamespace.PersonName>(writer.GetSpan(), false, 1);",
            code);
        Assert.Contains(
            "return new global::BTDB.ODBLayer.RelationAdvancedSecondaryKeyEnumerator<global::TestNamespace.PersonName>(this, writer.GetSpan(), remappedSecondaryKeyIndex, 1);",
            code);
        Assert.Contains(
            "return new global::BTDB.ODBLayer.RelationAdvancedOrderedEnumerator<ulong, global::TestNamespace.Person>(this, id.Order, id.StartProposition, prefixLen, startKeyBytes, id.EndProposition, endKeyBytes, 0, 1);",
            code);
        Assert.Contains(
            "return new global::BTDB.ODBLayer.RelationAdvancedOrderedSecondaryKeyEnumerator<int, global::TestNamespace.Person>(this, age.Order, age.StartProposition, prefixLen, startKeyBytes, age.EndProposition, endKeyBytes, remappedSecondaryKeyIndex, 0, 1);",
            code);
        Assert.Contains(
            "return new global::BTDB.ODBLayer.RelationAdvancedEnumerator<global::TestNamespace.Person>(this, tenantId.Order, tenantId.StartProposition, prefixLen, startKeyBytes, tenantId.EndProposition, endKeyBytes, 0);",
            code);
        Assert.Contains("return base.AnyWithPrefix(writer.GetSpan());", code);
        Assert.Contains(
            "return base.AnyWithProposition(age.StartProposition, prefixLen, startKeyBytes, age.EndProposition, endKeyBytes);",
            code);
        Assert.Contains(
            "return base.AnyWithProposition(tenantId.StartProposition, prefixLen, startKeyBytes, tenantId.EndProposition, endKeyBytes);",
            code);
        Assert.Contains("return base.CountWithPrefix(writer.GetSpan());", code);
        Assert.Contains("return (int)base.CountWithPrefix(writer.GetSpan());", code);
        Assert.Contains(
            "return (uint)base.CountWithProposition(name.StartProposition, prefixLen, startKeyBytes, name.EndProposition, endKeyBytes);",
            code);
        Assert.Contains(
            "return (ulong)base.CountWithProposition(tenantId.StartProposition, prefixLen, startKeyBytes, tenantId.EndProposition, endKeyBytes);",
            code);
        Assert.Contains("return LastByPrimaryKey<global::TestNamespace.Person>(0, c_c, null, null, false);", code);
        Assert.Contains("return LastByPrimaryKey<global::TestNamespace.Person>(0, c_c, null, orderers, true);", code);
        Assert.Contains("return FirstBySecondaryKey<global::TestNamespace.Person>(0, c_c, 1u, null, false);", code);
        Assert.Contains("return GatherBySecondaryKey(0, c_c, items, skip, take, 1u, orderers);", code);
        Assert.Contains("return ScanBySecondaryKeyPrefix<global::TestNamespace.Person>(0, c_c, 0u);", code);
        Assert.Contains(
            "throw new global::BTDB.KVDBLayer.BTDBException(\"Return value in RemoveById must be int, uint, long, or ulong.\");",
            code);
        Assert.Contains(
            "return (ulong)base.RemoveByIdAdvancedParam(id.Order, id.StartProposition, prefixLen, startKeyBytes, id.EndProposition, endKeyBytes);",
            code);
        Assert.Contains("return (int)base.RemoveByPrimaryKeyPrefixPartial(writer.GetSpan(), maxCount);", code);
        Assert.Contains("var removed = base.ShallowRemoveById(writer.GetSpan(), false);", code);
        Assert.Contains(
            "base.IterateByPrimaryKeyPrefix<global::TestNamespace.PersonName>(writer.GetSpan(), 1, 1, callback, value);",
            code);
        Assert.Contains("[typeof(global::TestNamespace.Person), typeof(global::TestNamespace.PersonName)]", code);
    }

    [Fact]
    public void VerifyRelationWithManyKeyTypes()
    {
        var sources = GenerateAndCompile("""
            using System;
            using System.Collections.Generic;
            using System.Net;
            using BTDB.Encrypted;
            using BTDB.ODBLayer;
            using Microsoft.Extensions.Primitives;

            namespace TestNamespace;

            public enum SmallEnum : byte { A, B }
            public enum BigEnum : long { A = -1, B = 1 }

            public class Item
            {
                [PrimaryKey(1)] public sbyte S8 { get; set; }
                [PrimaryKey(2)] public byte U8 { get; set; }
                [PrimaryKey(3)] public short S16 { get; set; }
                [PrimaryKey(4)] public ushort U16 { get; set; }
                [PrimaryKey(5)] public bool Flag { get; set; }
                [PrimaryKey(6)] public DateTime When { get; set; }
                [PrimaryKey(7)] public DateTimeOffset WhenOffset { get; set; }
                [PrimaryKey(8)] public TimeSpan Duration { get; set; }
                [PrimaryKey(9)] public Guid Guid { get; set; }
                [PrimaryKey(10)] public IPAddress Address { get; set; } = null!;
                [PrimaryKey(11)] public Version Version { get; set; } = null!;
                [PrimaryKey(12)] public EncryptedString Secret { get; set; }
                [PrimaryKey(13)] public StringValues Values { get; set; }
                [PrimaryKey(14)] public SmallEnum Small { get; set; }
                [PrimaryKey(15)] public BigEnum Big { get; set; }
                [PrimaryKey(16)] public SmallEnum? MaybeSmall { get; set; }
                [PrimaryKey(17)] public int? MaybeInt { get; set; }
                [PrimaryKey(18)] public string? Text { get; set; }
                [PrimaryKey(19)] public byte[] Bytes { get; set; } = null!;
            }

            public interface IItemTable : IRelation<Item>
            {
                bool Contains(sbyte s8, byte u8, short s16, ushort u16, bool flag, DateTime when,
                    DateTimeOffset whenOffset, TimeSpan duration, Guid guid, IPAddress address, Version version,
                    EncryptedString secret, StringValues values, SmallEnum small, BigEnum big, SmallEnum? maybeSmall,
                    int? maybeInt, string? text, byte[] bytes);
                IEnumerable<Item> ListById(sbyte s8, byte u8, short s16, ushort u16, bool flag, DateTime when,
                    DateTimeOffset whenOffset, TimeSpan duration, Guid guid, IPAddress address, Version version,
                    EncryptedString secret, StringValues values, AdvancedEnumeratorParam<SmallEnum> small);
            }
            """);
        var code = GetSource(sources, "TestNamespace.IItemTable.g.cs");
        Assert.Contains("writer.WriteInt8Ordered(s8);", code);
        Assert.Contains("writer.WriteUInt8(u8);", code);
        Assert.Contains("writer.WriteVInt64(s16);", code);
        Assert.Contains("writer.WriteVUInt64(u16);", code);
        Assert.Contains("writer.WriteBool(flag);", code);
        Assert.Contains("writer.WriteDateTimeForbidUnspecifiedKind(when);", code);
        Assert.Contains("writer.WriteDateTimeOffset(whenOffset);", code);
        Assert.Contains("writer.WriteTimeSpan(duration);", code);
        Assert.Contains("writer.WriteGuid(guid);", code);
        Assert.Contains("writer.WriteIPAddress(address);", code);
        Assert.Contains("writer.WriteVersion(version);", code);
        Assert.Contains("var ctx_ctx = new global::BTDB.ODBLayer.DBWriterCtx(Transaction);", code);
        Assert.Contains("ctx_ctx.WriteOrderedEncryptedString(ref writer, secret);", code);
        Assert.Contains("writer.WriteStringValues(values);", code);
        Assert.Contains("writer.WriteVUInt64((ulong)small);", code);
        Assert.Contains("writer.WriteVInt64((long)big);", code);
        Assert.Contains("writer.WriteVUInt64((ulong)maybeSmall.Value);", code);
        Assert.Contains("writer.WriteVInt64(maybeInt.Value);", code);
        Assert.Contains("writer.WriteStringOrdered(text);", code);
        Assert.Contains("writer.WriteByteArray(bytes);", code);
        Assert.Contains("writer.WriteVUInt64((ulong)small.Start);", code);
        Assert.Contains("writer.WriteVUInt64((ulong)small.End);", code);
        Assert.DoesNotContain("NotSupportedException", code);
    }

    [Fact]
    public void VerifyRelationWithTupleKeys()
    {
        var sources = GenerateAndCompile("""
            using System;
            using System.Collections.Generic;
            using BTDB.Encrypted;
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Item
            {
                [PrimaryKey(1)] public Tuple<int, uint> Reference { get; set; } = null!;
                [PrimaryKey(2)] public (int A, string B) Named { get; set; }
                [PrimaryKey(3)] public (long, (string, ulong)) Nested { get; set; }
                [PrimaryKey(4)] public (EncryptedString Secret, int Order) Secret { get; set; }
                [PrimaryKey(5)] public (int, int, int, int, int, int, int, int) Huge { get; set; }
                public string Value { get; set; } = null!;
            }

            public interface IItemTable : IRelation<Item>
            {
                Item FindById(Tuple<int, uint> reference, (int A, string B) named, (long, (string, ulong)) nested,
                    (EncryptedString Secret, int Order) secret, (int, int, int, int, int, int, int, int) huge);
                IEnumerable<Item> FindById(Tuple<int, uint> reference, (int A, string B) named);
            }
            """);
        var code = GetSource(sources, "TestNamespace.IItemTable.g.cs");
        Assert.Contains("writer.WriteVInt64(reference.Item1);", code);
        Assert.Contains("writer.WriteVUInt64(reference.Item2);", code);
        Assert.Contains("writer.WriteVInt64(named.Item1);", code);
        Assert.Contains("writer.WriteStringOrdered(named.Item2);", code);
        Assert.Contains("writer.WriteVInt64(nested.Item1);", code);
        Assert.Contains("writer.WriteStringOrdered(nested.Item2.Item1);", code);
        Assert.Contains("writer.WriteVUInt64(nested.Item2.Item2);", code);
        Assert.Contains("var ctx_ctx = new global::BTDB.ODBLayer.DBWriterCtx(Transaction);", code);
        Assert.Contains("ctx_ctx.WriteOrderedEncryptedString(ref writer, secret.Item1);", code);
        Assert.Contains("writer.WriteVInt64(secret.Item2);", code);
        // Tuples with more than 7 elements are not supported by BTDB tuple field handler.
        Assert.Contains("Key does not support type '(int, int, int, int, int, int, int, int)'", code);
    }

    [Fact]
    public void OrderedDictionaryEnumeratorUsesGlobalQualifiedTypes()
    {
        // Relation implementations are generated into BTDB.GeneratedRelations namespace, so a type name which is not
        // globally qualified and starts with a name of BTDB sub-namespace (here Collections) binds to BTDB.Collections.
        var sources = GenerateAndCompile("""
            using BTDB.ODBLayer;

            namespace Collections.Model;

            public class Person
            {
                [PrimaryKey(1)] public ulong TenantId { get; set; }
                [PrimaryKey(2)] public ulong Id { get; set; }
            }

            public interface IPersonTable : IRelation<Person>
            {
                IOrderedDictionaryEnumerator<ulong, Person> ListById(ulong tenantId, AdvancedEnumeratorParam<ulong> id);
            }
            """);
        var code = GetSource(sources, "Collections.Model.IPersonTable.g.cs");
        Assert.Contains("RelationAdvancedOrderedEnumerator<ulong, global::Collections.Model.Person>", code);
    }

    [Fact]
    public void DecimalUpdateByIdValuesAreWritten()
    {
        // decimal is displayed by its C# keyword, which was not recognized and generated code threw
        // NotSupportedException at runtime instead of writing the value. Decimal keys are refused (BTDB0035).
        var sources = GenerateAndCompile("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Price
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                public decimal Other { get; set; }
            }

            public interface IPriceTable : IRelation<Price>
            {
                void UpdateById(ulong id, decimal other);
            }
            """);
        var code = GetSource(sources, "TestNamespace.IPriceTable.g.cs");
        Assert.DoesNotContain("NotSupportedException", code);
        Assert.Contains("writer.WriteDecimal(other);", code);
    }

    [Fact]
    public void FindByReturningCollectionsUsesCollectionEnumerators()
    {
        var sources = GenerateAndCompile("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)] public ulong TenantId { get; set; }
                [PrimaryKey(2)] public ulong Id { get; set; }
                [SecondaryKey("Name")] public string Name { get; set; } = null!;
            }

            public interface IPersonTable : IRelation<Person>
            {
                ICollection<Person> FindById(ulong tenantId);
                IReadOnlyCollection<Person> FindByName(string name);
            }
            """);
        var code = GetSource(sources, "TestNamespace.IPersonTable.g.cs");
        Assert.Contains(
            "return new global::BTDB.ODBLayer.RelationAdvancedEnumerator<global::TestNamespace.Person>(this, writer.GetSpan(), 0);",
            code);
        Assert.Contains(
            "return new global::BTDB.ODBLayer.RelationAdvancedSecondaryKeyEnumerator<global::TestNamespace.Person>(this, writer.GetSpan(), remappedSecondaryKeyIndex, 0);",
            code);
    }

    [Fact]
    public void ManipulatorMethodsAreForwardedToBase()
    {
        var sources = GenerateAndCompile("""
            using System.Collections;
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IPersonTable : IRelation<Person>
            {
                bool Insert(Person person);
                new bool Upsert(Person person);
                bool ShallowUpsert(Person person);
                (bool Inserted, uint KeySize, uint OldValueSize, uint NewValueSize) ShallowUpsertWithSizes(Person person, bool allowInsert, bool allowUpdate);
                void Update(Person person);
                void ShallowUpdate(Person person);
                new void RemoveAll();
                new (long Inserted, long Updated) UpsertRange(IEnumerable<Person> items);
                new IEnumerator GetEnumerator();
            }
            """);
        var code = GetSource(sources, "TestNamespace.IPersonTable.g.cs");
        Assert.Contains("return base.Insert(person);", code);
        Assert.Contains("return base.Upsert(person);", code);
        Assert.Contains("return base.ShallowUpsert(person);", code);
        Assert.Contains("return base.ShallowUpsertWithSizes(person, allowInsert, allowUpdate);", code);
        Assert.Contains("base.Update(person);", code);
        Assert.Contains("base.ShallowUpdate(person);", code);
        Assert.Contains("            base.RemoveAll();", code);
        Assert.Contains("return base.UpsertRange(items);", code);
        Assert.Contains("return ((global::System.Collections.IEnumerable)this).GetEnumerator();", code);
        Assert.DoesNotContain("NotImplementedException", code);
    }

    [Fact]
    public void UpdateByIdVariants()
    {
        var sources = GenerateAndCompile("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public enum Kind : short { A, B }

            public class Item
            {
                string _name = "";

                [PrimaryKey(1)] public ulong Id { get; set; }
                [InKeyValue(2)] public int Version { get; set; }

                public string Name
                {
                    get => _name;
                    set => _name = value;
                }

                [SecondaryKey("Upper")] public string Upper => Name.ToUpperInvariant();
                [SecondaryKey("NameKey")]
                public string NameKey
                {
                    get => _name;
                }

                public Kind Kind { get; set; }
                public int? Count { get; set; }
                public (int A, string B) Pair { get; set; }
            }

            public interface IItemTable : IRelation<Item>
            {
                void UpdateById(ulong id, int version);
                bool UpdateByIdVersion(ulong id, int version);
                bool UpdateByIdValues(ulong id, int version, Kind kind, int? count, (int A, string B) pair);
            }
            """);
        var code = GetSource(sources, "TestNamespace.IItemTable.g.cs");
        Assert.Contains("lenOfPkWoInKeyValues = (int)writer.GetCurrentPosition();", code);
        Assert.Contains("_ = base.UpdateByIdInKeyValues(keyBytes, lenOfPkWoInKeyValues, true);", code);
        Assert.Contains("var updated = base.UpdateByIdInKeyValues(keyBytes, lenOfPkWoInKeyValues, false);", code);
        Assert.Contains("writer.WriteVInt64((long)kind);", code);
        Assert.Contains("writer.WriteVInt64(count.Value);", code);
        Assert.Contains("writer.WriteVInt64(pair.Item1);", code);
        Assert.Contains("writer.WriteString(pair.Item2);", code);
        // Computed fields are not stored, so only Name is skipped before written values.
        Assert.Contains("""
                                reader.SkipString();
                                reader.CopyFromPosToWriter(memoPos, ref writer);
                                RelationInfo.ClientVersionValueHandlers[3].Skip(ref reader, ctx_reader);
            """.Replace("\r", ""), code);
        var metadata = GetSource(sources, "TestNamespace.Item.g.cs");
        // Computed property must be described only by getter (no ByteOffset), even when getter just returns a field.
        Assert.Contains("""
                            Name = "NameKey",
                            Type = typeof(string),
                            PropRefGetter = &GenGetter5,
            """, metadata);
    }

    [Fact]
    public void ListSecondaryKeyAcceptsIListParameters()
    {
        // IList<T> parameter exactly matching IList<T> field passed validation, but generated code threw
        // NotSupportedException because only List<T> was recognized.
        var sources = GenerateAndCompile("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Item
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                [SecondaryKey("Indexes")] public IList<ulong> Indexes { get; set; } = null!;
                [SecondaryKey("Tags")] public IList<string> Tags { get; set; } = null!;
            }

            public interface IItemTable : IRelation<Item>
            {
                IEnumerable<Item> ListByIndexes(IList<ulong> indexes);
                IEnumerable<Item> ListByTags(IList<string>? tags);
                IEnumerable<Item> FindByTags(List<string> tags);
            }
            """);
        var code = GetSource(sources, "TestNamespace.IItemTable.g.cs");
        Assert.DoesNotContain("NotSupportedException", code);
        Assert.Contains("foreach (var item in indexes) writer.WriteVUInt64(item);", code);
        Assert.Contains("foreach (var item in tags) writer.WriteString(item);", code);
    }
}
