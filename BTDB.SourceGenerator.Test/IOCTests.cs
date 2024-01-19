using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using VerifyTests;
using VerifyXunit;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

[UsesVerify]
public class IOCTests
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
    public Task VerifyIocRegistrationForPartialClassWithMultipleDeclarations()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.Generate]
            public interface ILogger
            {
            }

            public partial class Logger: ILogger
            {
                [BTDB.IOC.Dependency]
                public int A { get; set; }
            }

            public partial class Logger: ILogger
            {
                [BTDB.IOC.Dependency]
                public int B { get; set; }
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
    public Task VerifyIocRegistrationForAbstractClassIsSkipped()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            [BTDB.Generate]
            public interface ILogger
            {
            }

            public interface IBetterLogger: ILogger
            {
            }

            public abstract class AbstractLogger: IBetterLogger
            {
                protected AbstractLogger(int a)
                {
                }

                public abstract void Log(string message);
            }

            public interface IErrorHandler
            {
                IBetterLogger Logger { get; }
            }

            public class Logger: AbstractLogger, IErrorHandler
            {
                public Logger(int a, int b): base(a)
                {
                }

                public override void Log(string message)
                {
                    Console.WriteLine(message);
                }

                public IBetterLogger Logger => this;
            }
            """);
    }

    [Fact]
    public Task VerifyIocRegistrationForSingleParameterConstructorWithDefaultValue()
    {
        // language=cs
        return VerifySourceGenerator("""
            [BTDB.Generate]
            public class ErrorHandler : IErrorHandler
            {
                const long DefaultBufferSize = 1024L * 1024 * 1024 * 1024;

                public ErrorHandler(long bufferSize = DefaultBufferSize)
                {
                }
            }
            """);
    }

    [Fact]
    public Task VerifyIocRegistrationForGenericClassDoesNotDoAnything()
    {
        // language=cs
        return VerifySourceGenerator("""
            [BTDB.Generate]
            public class ErrorHandler<T> : IErrorHandler
            {
                public ErrorHandler()
                {
                }
            }
            """);
    }

    [Fact]
    public Task VerifyIocRegistrationForSingleParameterPrivateConstructor()
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

                private ErrorHandler(ILogger logger)
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
                public ILogger Logger { get; set; }
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

    [Fact]
    public Task VerifyInitOnlyDependency()
    {
        // language=cs
        return VerifySourceGenerator(@"
            namespace TestNamespace;

            public interface ILogger
            {
            }

            [BTDB.Generate]
            public class ErrorHandler
            {
                [BTDB.IOC.Dependency]
                public ILogger Logger { get; init; }
            }
            ");
    }

    [Fact]
    public Task VerifyInitOnlyOptionalDependency()
    {
        // language=cs
        return VerifySourceGenerator(@"
            namespace TestNamespace;

            public interface ILogger
            {
            }

            [BTDB.Generate]
            public class ErrorHandler
            {
                [BTDB.IOC.Dependency]
                public ILogger? Logger { get; init; }
            }
            ");
    }

    [Fact]
    public Task VerifyCustomInitOnlyOptionalDependency()
    {
        // language=cs
        return VerifySourceGenerator(@"
            namespace TestNamespace;

            public interface ILogger
            {
            }

            [BTDB.Generate]
            public class ErrorHandler
            {
                [BTDB.IOC.Dependency]
                public ILogger? Logger {
                    get => null;
                    init => Console.WriteLine(value!.ToString());
                }
            }
            ");
    }

    [Fact]
    public Task VerifyCustomPrivateSetterOptionalDependency()
    {
        // language=cs
        return VerifySourceGenerator(@"
            namespace TestNamespace;

            public interface ILogger
            {
            }

            [BTDB.Generate]
            public class ErrorHandler
            {
                [BTDB.IOC.Dependency]
                public ILogger? Logger {
                    get => null;
                    private set => Console.WriteLine(value!.ToString());
                }
            }
            ");
    }

    [Fact]
    public Task VerifySimpleDispatcherGeneration()
    {
        // language=cs
        return VerifySourceGenerator(@"
            using BTDB.IOC;
            namespace TestNamespace;

            [BTDB.Generate]
            public partial interface IDispatcher
            {
                public static unsafe partial delegate*<IContainer, object, object?> CreateConsumeDispatcher(IContainer container);
            }

            public class Message
            {
                public string Text { get; set; }
            }

            public class MessageHandler : IDispatcher
            {
                public void Consume(Message message)
                {
                    Console.WriteLine(message.Text);
                }
            }
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
