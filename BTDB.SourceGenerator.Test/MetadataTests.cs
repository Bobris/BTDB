using System.Collections.Generic;
using System.Threading.Tasks;
using VerifyXunit;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

[UsesVerify]
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
                public int Age;
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
}
