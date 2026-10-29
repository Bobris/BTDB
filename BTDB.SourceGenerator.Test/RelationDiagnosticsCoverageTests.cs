using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

/// Covers relation interface method validation (DetectErrorsInMethods and helpers). Each case declares members of
/// a relation interface over the same item and expects exactly one generator diagnostic.
public class RelationDiagnosticsCoverageTests : GeneratorTestsBase
{
    const string Prolog = """
        using System;
        using System.Collections.Generic;
        using System.Threading.Tasks;
        using BTDB.ODBLayer;

        namespace TestNamespace;

        public class Person
        {
            [PrimaryKey(1)] public ulong TenantId { get; set; }
            [PrimaryKey(2)] public ulong Id { get; set; }
            [SecondaryKey("Name")] public string Name { get; set; } = null!;
            public int Age { get; set; }
        }

        public class PersonLite
        {
            public string Name { get; set; } = null!;
        }

        public interface IPersonTable : IRelation<Person>
        {

        """;

    const string Epilog = """

        }

        namespace Other
        {
            public interface IOrderedDictionaryEnumerator<TKey, TValue>
            {
            }

            public delegate void Action<T>(T value);
        }
        """;

    static readonly Dictionary<string, (string Members, string ExpectedId)> Cases = new()
    {
        ["IterateByIdMustReturnVoid"] = ("int IterateById(Action<Person> callback, Person value);", "BTDB0048"),
        ["IterateByIdNeedsCallbackAndValue"] = ("void IterateById(Person value);", "BTDB0049"),
        ["IterateByIdValueMustBeClass"] = ("void IterateById(Action<int> callback, int value);", "BTDB0050"),
        ["IterateByIdCallbackMustBeAction"] =
            ("void IterateById(Func<Person, bool> callback, Person value);", "BTDB0051"),
        ["IterateByIdCallbackMustNotBeGenericOfOtherArity"] =
            ("void IterateById(Action<Person, int> callback, Person value);", "BTDB0051"),
        ["IterateByIdCallbackMustBeSystemAction"] =
            ("void IterateById(Other.Action<Person> callback, Person value);", "BTDB0051"),
        ["IterateByIdCallbackMustMatchValueType"] =
            ("void IterateById(Action<PersonLite> callback, Person value);", "BTDB0051"),
        ["IterateByIdValueMustBeSerializable"] = ("void IterateById(Action<Task> callback, Task value);", "BTDB0043"),
        ["IterateByIdTooManyPrefixParameters"] =
            ("void IterateById(ulong tenantId, ulong id, string name, Action<Person> callback, Person value);",
                "BTDB0016"),
        ["IterateByIdPrefixParameterNameMismatch"] =
            ("void IterateById(ulong companyId, Action<Person> callback, Person value);", "BTDB0014"),
        ["IterateByIdPrefixParameterTypeMismatch"] =
            ("void IterateById(string tenantId, Action<Person> callback, Person value);", "BTDB0015"),
        ["FindByEnumerableOfValueType"] = ("IEnumerable<int> FindById(ulong tenantId);", "BTDB0042"),
        ["FindByEnumerableOfTask"] = ("IEnumerable<Task> FindById(ulong tenantId);", "BTDB0043"),
        ["FindByCollectionOfValueType"] = ("ICollection<int> FindById(ulong tenantId);", "BTDB0042"),
        ["FindByCollectionOfTask"] = ("ICollection<Task> FindById(ulong tenantId);", "BTDB0043"),
        ["FindByReadOnlyCollectionOfValueType"] = ("IReadOnlyCollection<int> FindById(ulong tenantId);", "BTDB0042"),
        ["FindByReadOnlyCollectionOfTask"] = ("IReadOnlyCollection<Task> FindById(ulong tenantId);", "BTDB0043"),
        ["FindByReturningTask"] = ("Task FindById(ulong tenantId, ulong id);", "BTDB0043"),
        ["FindByReturningStruct"] = ("int FindById(ulong tenantId, ulong id);", "BTDB0042"),
        ["FindByUnknownIndex"] = ("Person FindByAge(int age);", "BTDB0013"),
        ["ScanByMustReturnEnumerable"] = ("Person ScanById(Constraint<ulong> tenantId);", "BTDB0019"),
        ["ScanByMustReturnEnumerableOfClass"] = ("IEnumerable<int> ScanById(Constraint<ulong> tenantId);", "BTDB0019"),
        ["ScanByUnknownIndex"] = ("IEnumerable<Person> ScanByAge(Constraint<int> age);", "BTDB0013"),
        ["ScanByTooManyParameters"] =
            ("IEnumerable<Person> ScanById(Constraint<ulong> tenantId, Constraint<ulong> id, Constraint<string> name);",
                "BTDB0016"),
        ["ScanByParameterMustBeConstraint"] = ("IEnumerable<Person> ScanById(ulong tenantId);", "BTDB0017"),
        ["ScanByConstraintTypeMismatch"] =
            ("IEnumerable<Person> ScanById(Constraint<string> tenantId);", "BTDB0018"),
        ["GatherByUnknownIndex"] =
            ("ulong GatherByAge(ICollection<Person> items, long skip, long take, Constraint<int> age);", "BTDB0013"),
        ["GatherByTooManyConstraints"] =
            ("ulong GatherById(ICollection<Person> items, long skip, long take, Constraint<ulong> tenantId, Constraint<ulong> id, Constraint<string> name);",
                "BTDB0026"),
        ["GatherByArrayTargetIsNotCollection"] =
            ("ulong GatherById(Person[] items, long skip, long take);", "BTDB0023"),
        ["GatherByConstraintNameMismatch"] =
            ("ulong GatherById(List<Person> items, long skip, long take, Constraint<ulong> companyId);", "BTDB0014"),
        ["GatherByConstraintNameMismatchWithOrderers"] =
            ("ulong GatherById(List<Person> items, long skip, long take, Constraint<ulong> companyId, IOrderer[] orderers);",
                "BTDB0014"),
        ["FirstByUnknownIndex"] = ("Person FirstByAge(Constraint<int> age);", "BTDB0013"),
        ["LastByTooManyConstraints"] =
            ("Person LastById(Constraint<ulong> tenantId, Constraint<ulong> id, Constraint<string> name);", "BTDB0028"),
        ["ListByWithAdvancedParamMustReturnSupportedType"] =
            ("IList<Person> ListById(AdvancedEnumeratorParam<ulong> tenantId);", "BTDB0033"),
        ["ListByOrderedDictionaryEnumeratorKeyMismatch"] =
            ("IOrderedDictionaryEnumerator<string, Person> ListById(AdvancedEnumeratorParam<ulong> tenantId);",
                "BTDB0033"),
        ["ListByForeignOrderedDictionaryEnumerator"] =
            ("Other.IOrderedDictionaryEnumerator<ulong, Person> ListById(AdvancedEnumeratorParam<ulong> tenantId);",
                "BTDB0033"),
        ["ListByUnknownIndex"] = ("IEnumerable<Person> ListByAge(int age);", "BTDB0013"),
        ["ListByAdvancedParamOnUnknownIndex"] =
            ("IEnumerable<Person> ListByAge(AdvancedEnumeratorParam<int> age);", "BTDB0013"),
        ["ListBySecondaryKeyTooManyParametersWithAdvancedParam"] =
            ("IEnumerable<Person> ListByName(string name, ulong tenantId, ulong id, AdvancedEnumeratorParam<int> age);",
                "BTDB0016"),
        ["CountByAdvancedParamTypeMismatchOnSecondaryKey"] =
            ("int CountByName(AdvancedEnumeratorParam<int> name);", "BTDB0031"),
        ["UpdateByIdMustReturnVoidOrBool"] = ("int UpdateById(ulong tenantId, ulong id, string name);", "BTDB0039"),
        ["UpdateByIdNotEnoughParameters"] = ("void UpdateById(ulong tenantId);", "BTDB0040"),
        ["UpdateByIdForbidsListValue"] =
            ("void UpdateById(ulong tenantId, ulong id, List<int> name);", "BTDB0047"),
        ["UpdateByIdForbidsDictionaryValue"] =
            ("void UpdateById(ulong tenantId, ulong id, Dictionary<int, int> name);", "BTDB0047"),
        ["UpdateByIdNullableValueTypeMismatch"] = ("void UpdateById(ulong tenantId, ulong id, int? name);", "BTDB0046"),
        ["RemoveWithSizesByIdMustReturnTriple"] =
            ("(ulong Count, ulong KeySizes) RemoveWithSizesById(Constraint<ulong> tenantId);", "BTDB0036"),
        ["RemoveWithSizesByIdMustReturnUlongTriple"] =
            ("(ulong Count, ulong KeySizes, long ValueSizes) RemoveWithSizesById(Constraint<ulong> tenantId);",
                "BTDB0036"),
        ["RemoveWithSizesByIdMustNotReturnClass"] =
            ("Person RemoveWithSizesById(Constraint<ulong> tenantId);", "BTDB0036"),
        ["RemoveWithSizesByIdValidatesConstraints"] =
            ("(ulong Count, ulong KeySizes, ulong ValueSizes) RemoveWithSizesById(Constraint<ulong> companyId);",
                "BTDB0014"),
        ["ShallowUpsertWithSizesIsNotAllowedWithSecondaryIndexes"] =
            ("(bool Inserted, uint KeySize, uint OldValueSize, uint NewValueSize) ShallowUpsertWithSizes(Person person);",
                "BTDB0037"),
        ["BaseMethodParameterTypeMismatch"] = ("void Update(PersonLite person);", "BTDB0011"),
        ["EnumeratorReturnTypeInMethodWithoutOtherValidation"] = ("IEnumerator<Person> ContainsAny();", "BTDB0009"),
    };

    public static IEnumerable<object[]> CaseNames() => Cases.Keys.Select(k => new object[] { k });

    [Theory]
    [MemberData(nameof(CaseNames))]
    public void ReportsDiagnostic(string caseName)
    {
        var (members, expectedId) = Cases[caseName];
        AssertSingleGeneratorDiagnostic(Prolog + members + Epilog, expectedId);
    }

    [Theory]
    [InlineData("""
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("S", IncludePrimaryKeyOrder = 1)] public string A { get; set; } = null!;
        [SecondaryKey("S", IncludePrimaryKeyOrder = 1, Order = 1)] public string B { get; set; } = null!;
        """, "BTDB0007")]
    [InlineData("""
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("S", Order = 1)] public string A { get; set; } = null!;
        [SecondaryKey("S", Order = 1)] public string B { get; set; } = null!;
        """, "BTDB0008")]
    [InlineData("""
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("S")] public double Score { get; set; }
        """, "BTDB0035")]
    [InlineData("""
        [PrimaryKey(1)] public ulong Id { get; set; }
        [PrimaryKey(2)] public string A { get; set; } = null!;
        [InKeyValue(3)] public string B { get; set; } = null!;
        [PrimaryKey(4)] public string C { get; set; } = null!;
        """, "BTDB0006")]
    public void ReportsDiagnosticForInvalidRelationItem(string itemMembers, string expectedId)
    {
        AssertSingleGeneratorDiagnostic($$"""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Item
            {
            {{itemMembers}}
            }

            public interface IItemTable : IRelation<Item>
            {
            }
            """, expectedId);
    }

    /// UpdateById takes InKeyValue fields after the primary key, so they are checked like the other key parameters.
    [Theory]
    [InlineData("void UpdateById(ulong id, string? name);", "BTDB0014")]
    [InlineData("void UpdateById(ulong id, string version, string? name);", "BTDB0015")]
    public void ReportsUpdateByIdInKeyValueParameterMismatch(string member, string expectedId)
    {
        AssertSingleGeneratorDiagnostic($$"""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Item
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                [InKeyValue(2)] public int Version { get; set; }
                public string? Name { get; set; }
            }

            public interface IItemTable : IRelation<Item>
            {
                {{member}}
            }
            """, expectedId);
    }

    [Fact]
    public void InternalErrorIsReportedAsDiagnostic()
    {
        // Relation interfaces are recognized by name, but without BTDB referenced RelationDBManipulator<T> cannot be
        // found, which is an internal error reported as BTDB0000 instead of crashing the generator.
        var diagnostics = GetGeneratorDiagnostics("""
            namespace BTDB.ODBLayer
            {
                public interface ICovariantRelation<out T> where T : class
                {
                }

                public class PrimaryKeyAttribute : System.Attribute
                {
                    public PrimaryKeyAttribute(uint order = 0, bool inKeyValue = false)
                    {
                    }
                }
            }

            namespace TestNamespace
            {
                public class Person
                {
                    [BTDB.ODBLayer.PrimaryKey(1)] public ulong Id { get; set; }
                }

                public interface IPersonTable : BTDB.ODBLayer.ICovariantRelation<Person>
                {
                    void Insert(Person person);
                }
            }
            """, includeBtdbReference: false);
        var diagnostic = Assert.Single(diagnostics);
        Assert.Equal("BTDB0000", diagnostic.Id);
    }
}
