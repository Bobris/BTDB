using System.Threading.Tasks;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class MetadataTests : GeneratorTestsBase
{
    [Fact]
    public Task VerifyBasicMetadata()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public string Name { get; set; } = "";
                [BTDB.FieldHandler.PersistedNameAttribute("Years")]
                public int Age;
            }
            """);
    }

    [Fact]
    public Task StaticMembersMustBeSkippedInMetadata()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public static string Name { get; set; } = "";
                public static int Age;
            }
            """);
    }

    [Fact]
    public Task PersistedNameAttributeForcesMetadataGenerationEvenForClassesWithoutContent()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.FieldHandler.PersistedNameAttribute("Hello")]
            [BTDB.Generate]
            public class Switch
            {
            }
            """);
    }

    [Fact]
    public Task VerifyCollectionMetadata()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public string Name { get; set; } = "";
                public List<Person> Friends { get; set; } = new();
            }
            """);
    }

    [Fact]
    public Task VerifyDictCollectionMetadata()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public string Name { get; set; } = "";
                public Dictionary<int,string> Id2Name { get; set; } = new();
            }
            """);
    }

    [Fact]
    public Task VerifyNestedCollectionMetadata()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public string Name { get; set; } = "";
                public Dictionary<int,List<string>> Id2Names { get; set; } = new();
            }
            """);
    }

    [Fact]
    public Task VerifyMoreCollections()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public string Name { get; set; } = "";
                public IDictionary<int,IList<string>> Id2Names { get; set; }
                public IReadOnlyDictionary<int,IReadOnlyList<string>> Id2Names2 { get; set; }
                public IEnumerable<string> Names { get; set; }
                public ISet<string> NameSet { get; set; }
                public IReadOnlySet<string> NameSet2 { get; set; }
            }
            """);
    }

    [Fact]
    public Task VerifyArraysFromCollections()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public string Name { get; set; } = "";
                public IDictionary<int,string[]> Id2Names { get; set; }
                public ISet<string[]> NameSet2 { get; set; }
            }
            """);
    }

    [Fact]
    public Task VerifyStructFromCollections()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public string Name { get; set; } = "";
                public IDictionary<int,(string, string)> Id2Names { get; set; }
                public ISet<(string, int)> NameSet2 { get; set; }
            }
            """);
    }

    [Fact]
    public Task VerifyImplementsInterface()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            public interface INamed
            {
                string Name { get; set; }
            }

            [BTDB.Generate]
            public class Person : INamed
            {
                public static int MustBeIgnored;
                public string Name { get; set; } = "";
                public int Age;
            }
            """);
    }

    [Fact]
    public Task VerifyMetadataWithComplexSetter()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                string _name = "";
                int _age;

                public string Name
                {
                    get => _name;
                    set => _name = value.ToUpper();
                }

                public int Age
                {
                    get => _age;
                    set => _age = value + 1;
                }
            }
            """);
    }

    [Fact]
    public Task VerifyMetadataWithComplexGetter()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                string _name = "";
                int _age;

                public string Name
                {
                    get => _name.ToUpper();
                    set => _name = value;
                }

                public int Age
                {
                    get => _age + 1;
                    set => _age = value;
                }
            }
            """);
    }

    [Fact]
    public Task VerifyMetadataWithComplexGetterSetterWhichLooksSimple()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public int Number { get; set; }
                public int Age
                {
                    get => Number;
                    set => Number = value;
                }
            }
            """);
    }

    [Fact]
    public Task VerifyDerivedClassWithoutNewFieldsHasMetadata()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.Generate]
            public interface IChild
            {
                ulong Id { get; set; }
            }

            public class Child : IChild
            {
                public ulong Id { get; set; }
            }

            public class DerivedChild : Child
            {
            }
            """);
    }

    [Fact]
    public Task VerifyIIndirectPropertyIsCorrectlyGenerated()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public BTDB.FieldHandler.IIndirect<Person> Friend { get; set; }
            }
            """);
    }

    [Fact]
    public Task VerifyNestedValueInIDictionaryIsRegistered()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            namespace BTDBTest;

            public class ObjectDbEventSerializeTest
            {
                public class Item
                {
                    public ulong Id { get; set; }
                    public string Name { get; set; }
                }

                public class ObjWithIDictionary
                {
                    [PrimaryKey(1)] public uint TenantId { get; set; }

                    public IDictionary<ulong, Item> Dict { get; set; }
                }

                public interface IObjWithIDictionaryTable : IRelation<ObjWithIDictionary>
                {
                }
            }
            """);
    }

    [Fact]
    public Task VerifyNestedEmptyClassStillGenerateMetadata()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            namespace BTDBTest;

            public class ObjInObjV2
            {
            }

            public class RowObjInObjV2
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                public ObjInObjV2 OO { get; set; }
            }

            public interface IRowObjInObjV2Table : IRelation<RowObjInObjV2>
            {
            }
            """);
    }

    [Fact]
    public Task VerifyOnlyPublicInterfacesAreRegistered()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.Generate]
            public interface IPublicInterface
            {
                string PublicProperty { get; set; }
            }

            internal interface IInternalInterface
            {
                string InternalProperty { get; set; }
            }

            public class Person : IPublicInterface, IInternalInterface
            {
                public string PublicProperty { get; set; } = "";
                public string InternalProperty { get; set; } = "";
            }
            """);
    }

    [Fact]
    public Task VerifyFuncDelegatesDoNotGenerateMetadata()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public string Name { get; set; } = "";
                public Func<string, (int, int)> MyFunc { get; set; } = null!;
                public Action<(string, int)> MyAction { get; set; } = null!;
            }
            """);
    }

    [Fact]
    public Task VerifyTaskTypeDoNotGenerateMetadata()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Threading.Tasks;
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public string Name { get; set; } = "";
                public Task<string> MyTask { get; set; } = null!;
            }
            """);
    }

    [Fact]
    public Task VerifyValueTaskTypeDoNotGenerateMetadata()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Threading.Tasks;
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public string Name { get; set; } = "";
                public ValueTask<string> MyValueTask { get; set; } = default;
            }
            """);
    }
}
