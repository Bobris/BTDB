using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using VerifyXunit;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

[UsesVerify]
public class MetadataTests
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

    static Task VerifySourceGenerator(string sourceCode)
    {
        var generator = new SourceGenerator();
        var driver = CSharpGeneratorDriver.Create(generator);
        var compilation = CSharpCompilation.Create("test",
            new[] { CSharpSyntaxTree.ParseText(sourceCode) },
            new[]
            {
                MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(GenerateAttribute).Assembly.Location)
            });
        var runResult = driver.RunGenerators(compilation);
        return Verifier.Verify(runResult);
    }
}
