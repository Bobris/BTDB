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
    public Task VerifyMetadataWithComplexSetter()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public string Name { get; set { Name = value.ToUpperCase() } } = "";
                public int Age { get; set { Age = value + 1 } };
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
