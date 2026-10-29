using System.Linq;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

/// Covers IOC registration and GenerateFor edge cases.
public class IOCCoverageTests : GeneratorTestsBase
{
    [Fact]
    public void GenerateForAttributeFromOtherNamespaceIsIgnored()
    {
        var sources = GenerateAndCompile("""
            [assembly: Other.GenerateFor(typeof(TestNamespace.Logger))]

            namespace Other
            {
                [System.AttributeUsage(System.AttributeTargets.Assembly)]
                public class GenerateForAttribute : System.Attribute
                {
                    public GenerateForAttribute(System.Type type)
                    {
                    }
                }
            }

            namespace TestNamespace
            {
                public class Logger
                {
                }
            }
            """);
        Assert.Empty(sources);
    }

    [Theory]
    [InlineData("typeof(int[])")]
    [InlineData("typeof(TestNamespace.Logger<>)")]
    [InlineData("TestNamespace.Types.LoggerType")]
    public void GenerateForWithUnsupportedTypeArgumentGeneratesNothing(string argument)
    {
        var sources = GetGeneratedSources($$"""
            [assembly: BTDB.GenerateFor({{argument}})]

            namespace TestNamespace
            {
                public class Logger<T>
                {
                }

                public static class Types
                {
                    public static readonly System.Type LoggerType = typeof(Logger<int>);
                }
            }
            """);
        Assert.Empty(sources);
        Assert.Empty(GetGeneratorDiagnostics($$"""
            [assembly: BTDB.GenerateFor({{argument}})]
            namespace TestNamespace
            {
                public class Logger<T>
                {
                }

                public static class Types
                {
                    public static readonly System.Type LoggerType = typeof(Logger<int>);
                }
            }
            """));
    }

    [Fact]
    public void GenerateForOnNonAttributeClassIsStillRecognized()
    {
        // When BTDB.GenerateForAttribute does not derive from Attribute (here declared in source, hiding the real one)
        // Roslyn reports the constructor only as candidate symbol, generator should still use it.
        var sources = GetGeneratedSources("""
            [assembly: BTDB.GenerateFor(typeof(TestNamespace.Logger))]

            namespace BTDB
            {
                public class GenerateForAttribute
                {
                    public GenerateForAttribute(System.Type type)
                    {
                    }
                }
            }

            namespace TestNamespace
            {
                public class Logger
                {
                }
            }
            """);
        Assert.Contains(sources, s => s.HintName == "TestNamespace.Logger.g.cs");
    }

    [Fact]
    public void GenerateForWithNotMatchingConstructorParametersGeneratesNothing()
    {
        var sources = GenerateAndCompile("""
            [assembly: BTDB.GenerateFor(typeof(TestNamespace.Logger), ConstructorParameters = [typeof(string)])]

            namespace TestNamespace
            {
                public class Logger
                {
                    public Logger(int value)
                    {
                    }

                    public Logger(int value, string name)
                    {
                    }
                }
            }
            """);
        Assert.Empty(sources);
    }

    [Fact]
    public void PrivateAndProtectedDeclarationsAreSkipped()
    {
        var sources = GenerateAndCompile("""
            namespace TestNamespace;

            public class Outer
            {
                [BTDB.Generate]
                delegate Logger PrivateFactory();

                [BTDB.Generate]
                class PrivateLogger
                {
                }

                [BTDB.Generate]
                protected record ProtectedRecord(int A);

                [BTDB.Generate]
                private partial interface IPrivateDispatcher
                {
                    public static unsafe partial delegate*<BTDB.IOC.IContainer, object, object?> CreateConsumeDispatcher(
                        BTDB.IOC.IContainer container);
                }

                private partial interface IPrivateDispatcher
                {
                    public static unsafe partial delegate*<BTDB.IOC.IContainer, object, object?> CreateConsumeDispatcher(
                        BTDB.IOC.IContainer container) => null;
                }
            }

            public class Logger
            {
            }
            """);
        Assert.Empty(sources);
    }

    [Fact]
    public void RecordWithGenerateForAttributeIsGeneratedOnlyThroughGenerateFor()
    {
        var sources = GenerateAndCompile("""
            namespace TestNamespace;

            [BTDB.Generate]
            [BTDB.GenerateFor(typeof(Data))]
            public record Data(int A);
            """);
        Assert.Equal(["TestNamespace.Data.g.cs"], sources.Keys);
    }

    [Fact]
    public void PartialClassNestedInNonPartialClassIsGeneratedAsRegistration()
    {
        var sources = GenerateAndCompile("""
            namespace TestNamespace;

            public class Outer
            {
                [BTDB.Generate]
                public partial class Logger
                {
                    public Logger(string name)
                    {
                    }
                }
            }
            """);
        var code = GetSource(sources, "TestNamespace.Outer.Logger.g.cs");
        Assert.Contains("static file class LoggerRegistration", code);
    }

    [Fact]
    public void DispatcherSkipsExplicitAndInaccessibleImplementations()
    {
        var sources = GenerateAndCompile("""
            using System;
            using BTDB.IOC;

            namespace TestNamespace;

            [BTDB.Generate]
            public partial interface IDispatcher
            {
                public static unsafe partial delegate*<IContainer, object, object?> CreateVerifyDispatcher(IContainer container);
            }

            public interface IHandler<in TMessage> : IDispatcher
            {
                internal void Verify(TMessage message)
                {
                }
            }

            public interface IExplicitHandler<in TMessage> : IDispatcher
            {
                void Verify(TMessage message);
            }

            public class Message
            {
            }

            public class InternalDefaultHandler : IHandler<Message>
            {
            }

            public class ExplicitHandler : IExplicitHandler<Message>
            {
                void IExplicitHandler<Message>.Verify(Message message)
                {
                }
            }
            """);
        var handler = GetSource(sources, "TestNamespace.InternalDefaultHandler.g.cs");
        Assert.Contains("Unsafe.As<global::TestNamespace.IHandler<global::TestNamespace.Message>>(res).Verify(", handler);
        Assert.DoesNotContain("VerifyHandlers", GetSource(sources, "TestNamespace.ExplicitHandler.g.cs"));
    }

    // Before the fix the first one-parameter method with the dispatcher name was used, so a static or private overload
    // made the generated code fail to compile and a class handling two message types was registered for one of them.
    [Fact]
    public void DispatcherRegistersEveryAccessibleOverload()
    {
        var sources = GenerateAndCompile("""
            using BTDB.IOC;

            namespace TestNamespace;

            [BTDB.Generate]
            public partial interface IDispatcher
            {
                public static unsafe partial delegate*<IContainer, object, object?> CreateConsumeDispatcher(IContainer container);
            }

            public interface IHandler<in TMessage> : IDispatcher
            {
                void Consume(TMessage message);
            }

            public class Message
            {
            }

            public class Other
            {
            }

            public class Handler : IHandler<Message>
            {
                static void Consume(string text)
                {
                }

                void Consume(int value)
                {
                }

                public void Consume(Message message)
                {
                }
            }

            public class TwoHandler : IHandler<Message>, IHandler<Other>
            {
                public void Consume(Message message)
                {
                }

                public void Consume(Other message)
                {
                }
            }
            """);
        var handler = GetSource(sources, "TestNamespace.Handler.g.cs");
        Assert.Contains("ConsumeHandlers.GetOrAddValueRef(typeof(global::TestNamespace.Message)", handler);
        Assert.DoesNotContain("typeof(string)", handler);
        var twoHandler = GetSource(sources, "TestNamespace.TwoHandler.g.cs");
        Assert.Contains("ConsumeHandlers.GetOrAddValueRef(typeof(global::TestNamespace.Message)", twoHandler);
        Assert.Contains("ConsumeHandlers.GetOrAddValueRef(typeof(global::TestNamespace.Other)", twoHandler);
    }

    [Fact]
    public void DefaultInterfaceDispatcherReimplementedByClassIsNotUsedFromInterface()
    {
        var sources = GenerateAndCompile("""
            using BTDB.IOC;

            namespace TestNamespace;

            [BTDB.Generate]
            public partial interface IDispatcher
            {
                public static unsafe partial delegate*<IContainer, object, object?> CreateVerifyDispatcher(IContainer container);
            }

            public interface IHandler<in TMessage> : IDispatcher
            {
                void Verify(TMessage message)
                {
                }
            }

            public interface IOverridingHandler<in TMessage> : IHandler<TMessage>
            {
                void IHandler<TMessage>.Verify(TMessage message)
                {
                }
            }

            public class Message
            {
            }

            public class OverriddenHandler : IOverridingHandler<Message>
            {
            }
            """);
        Assert.DoesNotContain("VerifyHandlers", GetSource(sources, "TestNamespace.OverriddenHandler.g.cs"));
    }

    [Fact]
    public void ClassWithOnlyCopyConstructorGeneratesMetadataOnly()
    {
        var sources = GenerateAndCompile("""
            namespace TestNamespace;

            [BTDB.Generate]
            public class Data
            {
                public Data(Data other)
                {
                    A = other.A;
                }

                public int A { get; set; }
            }
            """);
        var code = GetSource(sources, "TestNamespace.Data.g.cs");
        Assert.DoesNotContain("RegisterFactory", code);
        Assert.Contains("RuntimeHelpers.GetUninitializedObject(typeof(global::TestNamespace.Data))", code);
        Assert.Contains("ReflectionMetadata.Register(metadata);", code);
    }

    [Fact]
    public void DelegateWithOptionalParametersAndDefaults()
    {
        var sources = GenerateAndCompile("""
            namespace TestNamespace;

            public class Logger
            {
                public Logger(int level, string? name, double ratio = 0.5, char mark = 'x')
                {
                }
            }

            [BTDB.Generate]
            public delegate Logger LoggerFactory(int level, string? name);
            """);
        var code = GetSource(sources, "TestNamespace.LoggerFactory.g.cs");
        Assert.Contains("typeof(global::System.Func<int,string,global::TestNamespace.Logger>)", code);
        var generated = sources.Keys.ToArray();
        Assert.Equal(["TestNamespace.LoggerFactory.g.cs"], generated);
    }

    [Fact]
    public void ConstructorDefaultValuesAreFormattedAsLiterals()
    {
        var sources = GenerateAndCompile("""
            namespace TestNamespace;

            public enum Level : byte { Low, High }

            [BTDB.Generate]
            public class Settings
            {
                public Settings(double d = 1.5, double nan = double.NaN, double pinf = double.PositiveInfinity,
                    double ninf = double.NegativeInfinity, float f = 2.5f, float fnan = float.NaN,
                    float fpinf = float.PositiveInfinity, float fninf = float.NegativeInfinity, decimal m = 1.25m,
                    char c = '\n', bool b = true, bool b2 = false, string s = "a\"b", string? none = null,
                    long l = -5, Level level = Level.High, int? maybe = null)
                {
                }
            }
            """);
        var code = GetSource(sources, "TestNamespace.Settings.g.cs");
        Assert.Contains(": 1.5D", code);
        Assert.Contains(": double.NaN", code);
        Assert.Contains(": double.PositiveInfinity", code);
        Assert.Contains(": double.NegativeInfinity", code);
        Assert.Contains(": 2.5F", code);
        Assert.Contains(": float.NaN", code);
        Assert.Contains(": float.PositiveInfinity", code);
        Assert.Contains(": float.NegativeInfinity", code);
        Assert.Contains(": 1.25M", code);
        Assert.Contains(": '\\n'", code);
        Assert.Contains(": true", code);
        Assert.Contains(": false", code);
        Assert.Contains(": \"a\\\"b\"", code);
        Assert.Contains(": default(string)", code);
        Assert.Contains(": (long)(-5)", code);
        Assert.Contains(": (global::TestNamespace.Level)(1)", code);
        Assert.Contains(": default(int?)", code);
    }
    [Fact]
    public void DispatcherInterfaceNestedInNonPartialTypeIsReported()
    {
        var diagnostic = AssertSingleGeneratorDiagnostic("""
            namespace TestNamespace
            {
                public class Outer
                {
                    [BTDB.Generate]
                    public partial interface IDispatcher
                    {
                        public static unsafe partial delegate*<BTDB.IOC.IContainer, object, object?> CreateConsumeDispatcher(BTDB.IOC.IContainer container);
                    }
                }
            }
            """, "BTDB0052");
        Assert.Contains("IDispatcher", diagnostic.GetMessage());
    }
}
