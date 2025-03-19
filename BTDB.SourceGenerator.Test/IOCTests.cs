using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using VerifyTests;
using VerifyXunit;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class IOCTests : GeneratorTestsBase
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
    public Task VerifyIocRegistrationForSimpleParametersLessClassThatIsObsolete()
    {
        // language=cs
        return VerifySourceGenerator("""
             namespace TestNamespace;

             [BTDB.Generate]
             public interface ILogger
             {
             }

             [System.Obsolete]
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
    public Task VerifyIocRegistrationForProtectedClassDoesNotDoAnything()
    {
        // language=cs
        return VerifySourceGenerator("""
            [BTDB.Generate]
            public interface IHandler
            {
            }

            public class ErrorHandler
            {
                protected class NestedErrorHandler : IHandler
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
    public Task VerifyIocRegistrationForParameterlessPrivateConstructor()
    {
        // language=cs
        return VerifySourceGenerator("""
            [BTDB.Generate]
            public class Logger
            {
                private Logger()
                {
                }
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
    public Task VerifyClassWithRequiredProperty()
    {
        // language=cs
        return VerifySourceGenerator(@"
            namespace TestNamespace;

            [BTDB.Generate]
            public class ErrorHandler
            {
                public required int Prop { get; set; }
            }
            ");
    }

    [Fact]
    public Task VerifyClassWithRequiredPropertyInBaseClass()
    {
        // language=cs
        return VerifySourceGenerator(@"
            namespace TestNamespace;

            public class BaseClass
            {
                public required int Prop { get; set; }
            }

            [BTDB.Generate]
            public class ErrorHandler : BaseClass
            {
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
    public Task VerifyGeneratedRecordsWorks()
    {
        // language=cs
        return VerifySourceGenerator(@"
            namespace TestNamespace;

            [BTDB.Generate]
            internal sealed record RecordSealedGenerated
            {
                public string Name { get; init; } = ""name"";
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

    [Fact]
    public Task VerifyGenerateForOnAssembly()
    {
        // language=cs
        return VerifySourceGenerator(@"
            [assembly: BTDB.GenerateFor(typeof(TestNamespace.Logger))]
            namespace TestNamespace;

            public interface ILogger
            {
            }

            public class Logger: ILogger
            {
            }
            ");
    }

    [Fact]
    public Task VerifyGenerateForOnAssemblyFromDifferentAssembly()
    {
        // language=cs
        return VerifySourceGenerator(@"
            [assembly: BTDB.GenerateFor(typeof(BTDB.GenerateForAttribute))]
            ");
    }

    [Fact]
    public Task VerifyGenerateForDisablesGenerating()
    {
        // language=cs
        return VerifySourceGenerator(@"
            namespace TestNamespace;

            [BTDB.Generate]
            public interface ILogger
            {
            }

            [BTDB.GenerateFor(typeof(TestNamespace.Logger2))]
            public class Logger: ILogger
            {
            }

            public class Logger2: ILogger
            {
            }
            ");
    }

    [Fact]
    public Task VerifyGenerateForCouldBeUsedForGenericClasses()
    {
        // language=cs
        return VerifySourceGenerator(@"
            namespace TestNamespace;

            public interface ILogger
            {
            }

            [BTDB.GenerateFor(typeof(Logger<int>))]
            public class Logger<T>: ILogger
            {
                public Logger(T a)
                {
                }
            }
            ");
    }

    [Fact]
    public Task VerifyGenerateForCouldBeUsedForGenericClassesWithMultipleVariants()
    {
        // language=cs
        return VerifySourceGenerator(@"
            namespace TestNamespace;

            public interface ILogger
            {
            }

            [BTDB.GenerateFor(typeof(Logger<int>))]
            [BTDB.GenerateFor(typeof(Logger<string>))]
            public class Logger<T>: ILogger
            {
                public Logger(T a)
                {
                }
            }
            ");
    }

    [Fact]
    public Task VerifyGenerateForCouldBeUsedForChoosingDifferentConstructor()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;

            public interface ILogger
            {
            }

            [BTDB.GenerateFor(typeof(Logger), ConstructorParameters = [typeof(int)])]
            public class Logger: ILogger
            {
                public Logger(int a)
                {
                }

                public Logger(int a, int b)
                {
                }
            }
            """);
    }

    [Fact]
    public Task ShowErrorWhenNotUsingModernCsharpForConstructorParameters()
    {
        // language=cs
        return VerifySourceGenerator(@"
            namespace TestNamespace;

            public interface ILogger
            {
            }

            [BTDB.GenerateFor(typeof(Logger), ConstructorParameters = new [] {typeof(int)})]
            public class Logger: ILogger
            {
                public Logger(int a)
                {
                }

                public Logger(int a, int b)
                {
                }
            }
            ");
    }

    [Fact]
    public Task VerifyDynamicDoesNotBreakMe()
    {
        // language=cs
        return VerifySourceGenerator(@"
            [BTDB.Generate]
            public class DynamicEventWrapper
            {
                public dynamic DynamicEvent { get; set; }

                public DynamicEventWrapper(dynamic dynamicEvent)
                {
                    DynamicEvent = dynamicEvent;
                }
            }
            ");
    }

    [Fact]
    public Task VerifySupportFromKeyedServices()
    {
        // language=cs
        return VerifySourceGenerator("""
            namespace TestNamespace;
            using Microsoft.Extensions.DependencyInjection;

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

                public ErrorHandler([FromKeyedServices("key1")] ILogger logger)
                {
                    _logger = logger;
                }

                public ILogger Logger => _logger;
            }
            """);
    }

    [Fact]
    public Task VerifySupportFromKeyedServicesFromExternalDependency()
    {
        // language=cs
        return VerifySourceGenerator(
            "[assembly: BTDB.GenerateFor(typeof(Sample3rdPartyLib.Class3rdPartyWithKeyedDependency))]");
    }
}
