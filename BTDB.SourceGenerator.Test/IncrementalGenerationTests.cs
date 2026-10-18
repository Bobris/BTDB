using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

/// Generator driver is reused between edits (as in IDE), so changed inputs must regenerate changed outputs.
public class IncrementalGenerationTests : GeneratorTestsBase
{
    static string GeneratedSource(GeneratorDriver driver, string hintName)
    {
        return driver.GetRunResult().Results.SelectMany(r => r.GeneratedSources)
            .Single(s => s.HintName == hintName).SourceText.ToString();
    }

    static GeneratorDriver RunEdit(GeneratorDriver driver, ref CSharpCompilation compilation, string newSource)
    {
        compilation = compilation.ReplaceSyntaxTree(compilation.SyntaxTrees.First(),
            CSharpSyntaxTree.ParseText(newSource));
        return driver.RunGenerators(compilation);
    }

    [Fact]
    public void AddingPropertyToClassRegeneratesMetadata()
    {
        const string before = """
            namespace TestNamespace;

            [BTDB.Generate]
            public class Person
            {
                public string Name { get; set; } = "";
            }
            """;
        var compilation = CreateCompilation(before);
        GeneratorDriver driver = CSharpGeneratorDriver.Create([new SourceGenerator().AsSourceGenerator()]);
        driver = driver.RunGenerators(compilation);
        Assert.DoesNotContain("\"Age\"", GeneratedSource(driver, "TestNamespace.Person.g.cs"));

        driver = RunEdit(driver, ref compilation, before.Replace("""public string Name { get; set; } = "";""",
            """
            public string Name { get; set; } = "";
                public int Age { get; set; }
            """));

        Assert.Contains("Name = \"Age\"", GeneratedSource(driver, "TestNamespace.Person.g.cs"));
    }

    [Fact]
    public void AddingMethodToRelationRegeneratesImplementation()
    {
        const string before = """
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                public string Name { get; set; } = "";
            }

            public interface IPersonTable : IRelation<Person>
            {
                void Insert(Person person);
            }
            """;
        var compilation = CreateCompilation(before);
        GeneratorDriver driver = CSharpGeneratorDriver.Create([new SourceGenerator().AsSourceGenerator()]);
        driver = driver.RunGenerators(compilation);
        Assert.DoesNotContain("FindById", GeneratedSource(driver, "TestNamespace.IPersonTable.g.cs"));

        driver = RunEdit(driver, ref compilation, before.Replace("void Insert(Person person);", """
            void Insert(Person person);
                Person FindById(ulong id);
            """));

        Assert.Contains("FindById(ulong id)", GeneratedSource(driver, "TestNamespace.IPersonTable.g.cs"));
    }

    [Fact]
    public void MovedErrorIsReportedAtNewLocation()
    {
        const string before = """
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
            }

            public interface IPersonTable : IRelation<Person>
            {
                void Unknown();
            }
            """;
        var compilation = CreateCompilation(before);
        GeneratorDriver driver = CSharpGeneratorDriver.Create([new SourceGenerator().AsSourceGenerator()]);
        driver = driver.RunGenerators(compilation);
        var firstLine = Assert.Single(driver.GetRunResult().Diagnostics).Location.GetLineSpan().StartLinePosition
            .Line;

        driver = RunEdit(driver, ref compilation, before.Replace("void Unknown();", "\n\nvoid Unknown();"));

        var diagnostic = Assert.Single(driver.GetRunResult().Diagnostics);
        Assert.Equal("BTDB0041", diagnostic.Id);
        Assert.Equal(firstLine + 2, diagnostic.Location.GetLineSpan().StartLinePosition.Line);
    }
}
