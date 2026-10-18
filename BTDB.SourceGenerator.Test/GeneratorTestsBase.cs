using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using DiffEngine;
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
    [ModuleInitializer]
    internal static void InitVerifyTests()
    {
        if (AgentTestEnvironment.IsAgentRun)
            DiffRunner.Disabled = true;
    }

    protected static Task VerifySourceGenerator(string sourceCode,
        string? sourceCodeForCompilationValidation = null)
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
        if (sourceCodeForCompilationValidation is not null)
        {
            generatedCompilation = generatedCompilation.AddSyntaxTrees(
                CSharpSyntaxTree.ParseText(sourceCodeForCompilationValidation, path: "ValidationOnly.cs"));
        }

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

    protected static CSharpParseOptions InterceptorParseOptions(bool optIn) =>
        CSharpParseOptions.Default.WithFeatures(optIn
            ? [new("InterceptorsNamespaces", RelationQueryInterceptorGenerator.InterceptorsNamespace)]
            : []);

    /// Runs the relation query interceptor generator with the project's opt-in, checks that the result compiles and
    /// that a rerun is cached, and verifies the generated interceptors.
    protected static Task VerifyInterceptorGenerator(string sourceCode)
    {
        var parseOptions = InterceptorParseOptions(true);
        var driver = CSharpGeneratorDriver.Create([new RelationQueryInterceptorGenerator().AsSourceGenerator()],
            parseOptions: parseOptions,
            driverOptions: new GeneratorDriverOptions(default, trackIncrementalGeneratorSteps: true));
        var compilation = CSharpCompilation.Create("test",
            [CSharpSyntaxTree.ParseText(sourceCode, parseOptions)],
            GetMetadataReferences(),
            new(OutputKind.DynamicallyLinkedLibrary, allowUnsafe: true));
        var runResult = driver.RunGeneratorsAndUpdateCompilation(compilation, out var generatedCompilation, out _);
        var compilationErrors = generatedCompilation.GetDiagnostics()
            .Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ToArray();
        Assert.Empty(compilationErrors);
        compilation = compilation.AddSyntaxTrees(CSharpSyntaxTree.ParseText("// dummy", parseOptions));
        var result = runResult.RunGenerators(compilation).GetRunResult().Results.Single();
        foreach (var output in result.TrackedOutputSteps.SelectMany(step => step.Value).SelectMany(o => o.Outputs))
            Assert.Equal(IncrementalStepRunReason.Cached, output.Reason);
        return Verifier.Verify(runResult);
    }

    protected static IReadOnlyList<GeneratedSourceResult> GetInterceptorSources(string sourceCode, bool optIn)
    {
        return GetInterceptorSources(sourceCode, InterceptorParseOptions(optIn));
    }

    protected static IReadOnlyList<GeneratedSourceResult> GetInterceptorSources(string sourceCode,
        CSharpParseOptions parseOptions)
    {
        var driver = CSharpGeneratorDriver.Create([new RelationQueryInterceptorGenerator().AsSourceGenerator()],
            parseOptions: parseOptions);
        var compilation = CSharpCompilation.Create("test",
            [CSharpSyntaxTree.ParseText(sourceCode, parseOptions)],
            GetMetadataReferences(),
            new(OutputKind.DynamicallyLinkedLibrary, allowUnsafe: true));
        return driver.RunGenerators(compilation).GetRunResult().Results.SelectMany(r => r.GeneratedSources)
            .ToArray();
    }

    protected static Diagnostic[] GetGeneratedCompilationDiagnostics(string sourceCode,
        DiagnosticSeverity minimumSeverity)
    {
        var generator = new SourceGenerator();
        var driver = CSharpGeneratorDriver.Create([generator.AsSourceGenerator()]);
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

        return generatedCompilation.GetDiagnostics()
            .Where(diagnostic => diagnostic.Severity >= minimumSeverity)
            .ToArray();
    }

    protected static IReadOnlyList<GeneratedSourceResult> GetGeneratedSources(string sourceCode)
    {
        var generator = new SourceGenerator();
        var driver = CSharpGeneratorDriver.Create([generator.AsSourceGenerator()]);
        var compilation = CSharpCompilation.Create("test",
            [CSharpSyntaxTree.ParseText(sourceCode)],
            GetMetadataReferences(),
            new(OutputKind.DynamicallyLinkedLibrary, allowUnsafe: true));

        return driver.RunGenerators(compilation)
            .GetRunResult()
            .Results
            .SelectMany(result => result.GeneratedSources)
            .ToArray();
    }

    protected static CSharpCompilation CreateCompilation(string sourceCode, bool includeBtdbReference = true)
    {
        var references = GetMetadataReferences();
        if (!includeBtdbReference)
        {
            references = references.Where(reference =>
                !Path.GetFileName(reference.Display ?? "").StartsWith("BTDB", StringComparison.Ordinal) &&
                !Path.GetFileName(reference.Display ?? "").StartsWith("Sample3rdPartyLib", StringComparison.Ordinal));
        }

        return CSharpCompilation.Create("test",
            [CSharpSyntaxTree.ParseText(sourceCode)],
            references,
            new(OutputKind.DynamicallyLinkedLibrary, allowUnsafe: true));
    }

    /// Returns diagnostics reported by the generator itself (not compilation diagnostics of the generated code).
    protected static Diagnostic[] GetGeneratorDiagnostics(string sourceCode, bool includeBtdbReference = true)
    {
        var driver = CSharpGeneratorDriver.Create([new SourceGenerator().AsSourceGenerator()]);
        return driver.RunGenerators(CreateCompilation(sourceCode, includeBtdbReference)).GetRunResult().Diagnostics
            .ToArray();
    }

    /// Asserts that the generator reports exactly one diagnostic with the given id and returns it.
    protected static Diagnostic AssertSingleGeneratorDiagnostic(string sourceCode, string expectedId)
    {
        var diagnostics = GetGeneratorDiagnostics(sourceCode);
        var diagnostic = Assert.Single(diagnostics);
        Assert.Equal(expectedId, diagnostic.Id);
        Assert.Equal(DiagnosticSeverity.Error, diagnostic.Severity);
        return diagnostic;
    }

    /// Runs the generator, asserts it reports no diagnostics, that the generated code compiles without errors and
    /// that rerunning the generator is fully cached. Returns generated sources by hint name.
    protected static IReadOnlyDictionary<string, string> GenerateAndCompile(string sourceCode)
    {
        var driver = CSharpGeneratorDriver.Create([new SourceGenerator().AsSourceGenerator()],
            driverOptions: new GeneratorDriverOptions(default, trackIncrementalGeneratorSteps: true));
        var compilation = CreateCompilation(sourceCode);
        var runResult = driver.RunGeneratorsAndUpdateCompilation(compilation, out var generatedCompilation,
            out var generatorDiagnostics);
        Assert.Empty(generatorDiagnostics);
        var compilationErrors = generatedCompilation.GetDiagnostics()
            .Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ToArray();
        Assert.Empty(compilationErrors);
        compilation = compilation.AddSyntaxTrees(CSharpSyntaxTree.ParseText("// dummy"));
        var result = runResult.RunGenerators(compilation).GetRunResult().Results.Single();
        foreach (var output in result.TrackedOutputSteps.SelectMany(step => step.Value).SelectMany(o => o.Outputs))
            Assert.Equal(IncrementalStepRunReason.Cached, output.Reason);
        return runResult.GetRunResult().Results.SelectMany(r => r.GeneratedSources)
            .ToDictionary(s => s.HintName, s => s.SourceText.ToString());
    }

    /// Returns generated source with the given hint name, failing with list of generated hint names when missing.
    protected static string GetSource(IReadOnlyDictionary<string, string> sources, string hintName)
    {
        Assert.True(sources.TryGetValue(hintName, out var source),
            $"Missing {hintName}, generated: {string.Join(", ", sources.Keys)}");
        return source!;
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
