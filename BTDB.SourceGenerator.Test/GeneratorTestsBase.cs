using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.Extensions.DependencyInjection;
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
            [
                MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(GenerateAttribute).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(FromKeyedServicesAttribute).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(I3rdPartyInterface).Assembly.Location),
            ], new(OutputKind.ConsoleApplication, allowUnsafe: true));
        var runResult = driver.RunGenerators(compilation);
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
}
