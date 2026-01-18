using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Primitives;
using Sample3rdPartyLib;
using VerifyXunit;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class GeneratorTestsBase
{
    protected static Task VerifySourceGenerator(string sourceCode)
    {
        var generator = new SourceGenerator();
        var driver = CSharpGeneratorDriver.Create([generator.AsSourceGenerator()],
            driverOptions: new GeneratorDriverOptions(default, trackIncrementalGeneratorSteps: true));
        var compilation = CSharpCompilation.Create("test",
            [CSharpSyntaxTree.ParseText(sourceCode)],
            GetMetadataReferences(),
            new(OutputKind.DynamicallyLinkedLibrary, allowUnsafe: true));
        var runResult = driver.RunGenerators(compilation);
        var generatedSources = runResult.GetRunResult().Results
            .SelectMany(result => result.GeneratedSources)
            .Select(source => CSharpSyntaxTree.ParseText(source.SourceText, path: source.HintName))
            .ToList();
        var generatedCompilation = compilation.AddSyntaxTrees(generatedSources);
        var compilationErrors = generatedCompilation.GetDiagnostics()
            .Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ToArray();
        Assert.Empty(compilationErrors);
        // Update the compilation and rerun the generator
        compilation = compilation.AddSyntaxTrees(CSharpSyntaxTree.ParseText("// dummy"));

        var runResult2 = runResult.RunGenerators(compilation);

        // Assert the driver doesn't recompute the output
        var result = runResult2.GetRunResult().Results.Single();
        var allOutputs = result.TrackedOutputSteps.SelectMany(outputStep => outputStep.Value)
            .SelectMany(output => output.Outputs).ToList();
        if (allOutputs.Count != 0)
        {
            Assert.Collection(allOutputs, output => Assert.Equal(IncrementalStepRunReason.Cached, output.Reason));
        }

        return Verifier.Verify(runResult);
    }

    private static IEnumerable<MetadataReference> GetMetadataReferences()
    {
        var references = new List<MetadataReference>();
        var referencePaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var trustedAssemblies = (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES");

        if (trustedAssemblies is not null)
        {
            foreach (var assemblyPath in trustedAssemblies.Split(Path.PathSeparator,
                         StringSplitOptions.RemoveEmptyEntries))
            {
                AddReference(assemblyPath, references, referencePaths);
            }
        }
        else
        {
            AddReference(typeof(object).Assembly.Location, references, referencePaths);
            AddReference(typeof(Enumerable).Assembly.Location, references, referencePaths);
        }

        AddReference(typeof(GenerateAttribute).Assembly.Location, references, referencePaths);
        AddReference(typeof(FromKeyedServicesAttribute).Assembly.Location, references, referencePaths);
        AddReference(typeof(I3rdPartyInterface).Assembly.Location, references, referencePaths);
        AddReference(typeof(IPAddress).Assembly.Location, references, referencePaths);
        AddReference(typeof(StringValues).Assembly.Location, references, referencePaths);

        return references;
    }

    private static void AddReference(string assemblyPath, ICollection<MetadataReference> references,
        ISet<string> referencePaths)
    {
        if (!referencePaths.Add(assemblyPath))
        {
            return;
        }

        references.Add(MetadataReference.CreateFromFile(assemblyPath));
    }
}
