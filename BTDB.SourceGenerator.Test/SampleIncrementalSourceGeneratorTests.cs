using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using VerifyTests;
using VerifyXunit;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

[UsesVerify]
public class SampleIncrementalSourceGeneratorTests
{
    [ModuleInitializer]
    internal static void Init() => VerifySourceGenerators.Initialize();

    [Fact]
    public Task VerifyIocRegistrationForSimpleParametersLessClass()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.Generate]
            public interface ILogger
            {
            }

            public class Logger: ILogger
            {
            }
            """);
    }

    [Fact]
    public Task VerifyIocRegistrationForSingleParameterConstructor()
    {
        // language=cs
        return VerifySourceGenerator("""
            public interface ILogger
            {
            }

            public interface IErrorHandler
            {
                ILogger Logger { get; }
            }

            [BTDB.Generate]
            public class ErrorHandler : IErrorHandler
            {
                readonly ILogger _logger;

                public ErrorHandler(ILogger logger)
                {
                    _logger = logger;
                }

                public ILogger Logger => _logger;
            }
            """);
    }

    [Fact]
    public Task VerifyIocRegistrationForDependencyProperties()
    {
        // language=cs
        return VerifySourceGenerator("""
                                     public interface ILogger
                                     {
                                     }

                                     [BTDB.Generate]
                                     public class ErrorHandler
                                     {
                                         [BTDB.IOC.Dependency]
                                         public ILogger Logger { get; init; }
                                     }
                                     """);
    }

    [Fact]
    public Task VerifyDelegateGeneration()
    {
        // language=cs
        return VerifySourceGenerator(@"
            namespace TestNamespace;

            [BTDB.Generate]
            public delegate ILogger Factory(int a, string b);
            ");
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
