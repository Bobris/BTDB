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
    public void GenerateForWithNotMatchingConstructorParametersIsReported()
    {
        // Generating nothing would only fail at runtime, when the container finds no factory for Logger.
        AssertSingleGeneratorDiagnostic("""
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
            """, "BTDB0055");
    }

    [Fact]
    public void GenerateForWithConstructorParameterOfNoNamedTypeIsReported()
    {
        AssertSingleGeneratorDiagnostic("""
            [assembly: BTDB.GenerateFor(typeof(TestNamespace.Logger), ConstructorParameters = [typeof(int[])])]

            namespace TestNamespace
            {
                public class Logger
                {
                    public Logger(int[] values)
                    {
                    }
                }
            }
            """, "BTDB0055");
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

    // Before the fix the generated code cast a struct message with Unsafe.As, which works only for reference types, and
    // did not compile.
    [Fact]
    public void DispatcherUnboxesValueTypeMessages()
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
                int Consume(TMessage message);
            }

            public struct Message
            {
            }

            public class Handler : IHandler<Message>, IHandler<int?>
            {
                public int Consume(Message message) => 1;

                public int Consume(int? message) => 2;
            }
            """);
        var handler = GetSource(sources, "TestNamespace.Handler.g.cs");
        Assert.Contains(".Consume((global::TestNamespace.Message)message)", handler);
        Assert.Contains("GetOrAddValueRef(typeof(int).TypeHandle.Value)", handler);
        Assert.Contains(".Consume((int?)message)", handler);
    }

    // Before the fix a public readonly [Dependency] field was assigned directly and the generated code did not compile.
    [Fact]
    public void DependencyMembersOfEveryShapeCompile()
    {
        var sources = GenerateAndCompile("""
            namespace TestNamespace;

            public interface ILogger
            {
            }

            [BTDB.Generate]
            public class Shapes
            {
                [BTDB.IOC.Dependency] public ILogger Init { get; init; } = null!;
                [BTDB.IOC.Dependency] public required ILogger Required { get; set; }
                [BTDB.IOC.Dependency] public readonly ILogger ReadonlyField = null!;
                [BTDB.IOC.Dependency] public ILogger? Optional { get; set; }
            }

            [BTDB.Generate]
            public record RecordShape(ILogger Logger)
            {
                [BTDB.IOC.Dependency] public ILogger Other { get; init; } = null!;
            }
            """);
        Assert.Contains("UnsafeAccessorKind.Field, Name = \"ReadonlyField\"", GetSource(sources, "TestNamespace.Shapes.g.cs"));
    }

    // Before the fix only string keys were read, as the text of their literal, so the key 1 became "1" and enum and type
    // keys were replaced by the parameter name, and the attribute declared first won, unlike in reflection, where the
    // name of [Dependency] comes first.
    [Fact]
    public void DependencyKeysMatchReflection()
    {
        var sources = GenerateAndCompile("""
            using Microsoft.Extensions.DependencyInjection;
            namespace TestNamespace;

            public interface ILogger { }
            public enum Mode { A = 1, B = 2 }

            [BTDB.Generate]
            public class Keys
            {
                public Keys([FromKeyedServices(1)] ILogger a, [FromKeyedServices(Mode.B)] ILogger b,
                    [FromKeyedServices(typeof(Mode))] ILogger c, [FromKeyedServices("k")][BTDB.IOC.Dependency("d")] ILogger d,
                    [BTDB.IOC.Dependency][FromKeyedServices("k")] ILogger e, [FromKeyedServices(Name)] ILogger f)
                {
                }

                const string Name = "constant";
            }
            """);
        var code = GetSource(sources, "TestNamespace.Keys.g.cs");
        Assert.Contains("typeof(global::TestNamespace.ILogger), (int)(1));", code);
        Assert.Contains("typeof(global::TestNamespace.ILogger), (global::TestNamespace.Mode)(2));", code);
        Assert.Contains("typeof(global::TestNamespace.ILogger), typeof(global::TestNamespace.Mode));", code);
        Assert.Contains("var f3 = container.CreateFactory(ctx, typeof(global::TestNamespace.ILogger), \"d\");", code);
        Assert.Contains("var f4 = container.CreateFactory(ctx, typeof(global::TestNamespace.ILogger), \"k\");", code);
        Assert.Contains("var f5 = container.CreateFactory(ctx, typeof(global::TestNamespace.ILogger), \"constant\");", code);
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

    // Before the fix the longest constructor was taken even when it needed a pointer or a ref struct, which nothing can
    // pass, so GenerateFor of string generated code calling string(sbyte*, int, int, Encoding) that did not compile.
    [Fact]
    public void ConstructorsTakingPointersOrRefStructsAreNotChosen()
    {
        GenerateAndCompile("""
            namespace TestNamespace;

            public class Target
            {
                public unsafe Target(byte* data, int length) { }
                public Target(System.ReadOnlySpan<byte> data, int a, int b) { }
                public Target(string name) { }
            }

            [BTDB.GenerateFor(typeof(string))]
            [BTDB.GenerateFor(typeof(Target))]
            public class Registrations { }
            """);
    }

    // Before the fix a struct or a delegate named by GenerateFor got a creator and metadata written for a class, which did
    // not compile.
    [Theory]
    [InlineData("public struct Target { public int X { get; set; } }")]
    [InlineData("public record struct Target(int A, string B);")]
    [InlineData("public delegate int Target(int x);")]
    public void GenerateForValueTypeIsReported(string declaration)
    {
        var diagnostic = AssertSingleGeneratorDiagnostic($$"""
            namespace TestNamespace;

            {{declaration}}

            [BTDB.GenerateFor(typeof(Target))]
            public class Registrations { }
            """, "BTDB0054");
        Assert.Contains("TestNamespace.Target", diagnostic.GetMessage());
    }

    // Before the fix a member of a type the generated code cannot access, like a private nested class of a class that is
    // not partial, made the generated metadata fail to compile.
    [Fact]
    public void MemberOfInaccessibleTypeIsReported()
    {
        var diagnostic = AssertSingleGeneratorDiagnostic("""
            namespace TestNamespace;

            public class Outer
            {
                [BTDB.Generate]
                public class UsesPrivateType
                {
                    private System.Collections.Generic.List<Hidden>? Items { get; set; }
                    private class Hidden { public int X { get; set; } }
                }
            }
            """, "BTDB0053");
        Assert.Contains("Items", diagnostic.GetMessage());
    }

    // Before the fix two members stored under one name, as PersistedName can make, were generated, and were saved and
    // loaded into each other at runtime.
    [Fact]
    public void PersistedNameClashIsReported()
    {
        var diagnostic = AssertSingleGeneratorDiagnostic("""
            namespace TestNamespace;

            [BTDB.Generate]
            public class Clashing
            {
                [BTDB.FieldHandler.PersistedName("X")] public int A { get; set; }
                public string? X { get; set; }
            }
            """, "BTDB0056");
        Assert.Contains("stored under the name X", diagnostic.GetMessage());
    }

    [Fact]
    public void MemberOfPrivateTypeOfPartialClassCompiles()
    {
        GenerateAndCompile("""
            namespace TestNamespace;

            public partial class Outer
            {
                [BTDB.Generate]
                public partial class UsesPrivateType
                {
                    UsesPrivateType(Hidden hidden) { }
                    [BTDB.IOC.Dependency] Hidden? Dep { get; set; }
                    private Hidden? Field { get; set; }
                    private Hidden[]? Array { get; set; }
                    private class Hidden { public int X { get; set; } }
                }
            }
            """);
    }

    [Theory]
    [InlineData("WithPrivateTypes(Hidden hidden) { }", "Parameter hidden")]
    [InlineData("[BTDB.IOC.Dependency] Hidden? Dep { get; set; }", "Member Dep")]
    [InlineData("[BTDB.IOC.Dependency] Hidden? _depField;", "Member _depField")]
    public void DependencyOfInaccessibleTypeIsReported(string member, string reported)
    {
        var diagnostic = AssertSingleGeneratorDiagnostic($$"""
            namespace TestNamespace;

            public class Outer
            {
                [BTDB.Generate]
                public class WithPrivateTypes
                {
                    {{member}}
                    class Hidden { }
                }
            }
            """, "BTDB0053");
        Assert.StartsWith(reported + " of", diagnostic.GetMessage());
    }

    // Projects treating warnings as errors with nullable enabled build generated code too.
    [Fact]
    public void GeneratedCodeHasNoWarningsWithNullableEnabled()
    {
        var source = """
            #nullable enable
            using System;
            using System.Collections.Generic;
            using BTDB.IOC;
            using BTDB.ODBLayer;
            namespace TestNamespace;

            public interface ILogger { }

            [BTDB.Generate]
            public class Service
            {
                public Service(ILogger logger, ILogger? optional, int count = 3, string? name = null) { }
                [Dependency] public ILogger? Maybe { get; set; }
                [Dependency] public ILogger Required { get; set; } = null!;
                public string? Text { get; set; }
                public List<int>? Numbers { get; set; }
                public Dictionary<string, int?>? Map { get; set; }
                public (int, string?) Pair { get; set; }
            }

            [BTDB.Generate]
            public partial interface IDispatcher
            {
                public static unsafe partial delegate*<IContainer, object, object?> CreateConsumeDispatcher(IContainer container);
            }

            public interface IHandler<in T> : IDispatcher { void Consume(T message); }

            public class Message { }

            [BTDB.Generate]
            public class Handler : IHandler<Message>, IHandler<int> { public void Consume(Message m) { } public void Consume(int m) { } }

            public class Item
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                [SecondaryKey("Name")] public string? Name { get; set; }
                public int? Age { get; set; }
            }

            [BTDB.Generate]
            public interface IItemTable : IRelation<Item>
            {
                void Insert(Item item);
                Item? FindByIdOrDefault(ulong id);
                IEnumerable<Item> FindByName(string? name);
                bool RemoveById(ulong id);
            }
            """;
        var generator = new SourceGenerator();
        var driver = Microsoft.CodeAnalysis.CSharp.CSharpGeneratorDriver.Create(generator);
        var options = new Microsoft.CodeAnalysis.CSharp.CSharpCompilationOptions(Microsoft.CodeAnalysis.OutputKind.DynamicallyLinkedLibrary,
            allowUnsafe: true, nullableContextOptions: Microsoft.CodeAnalysis.NullableContextOptions.Enable, warningLevel: 9999);
        var compilation = CreateCompilation(source).WithOptions(options);
        driver.RunGeneratorsAndUpdateCompilation(compilation, out var generated, out _);
        Assert.True(generated.SyntaxTrees.Count() > 5);
        var warnings = generated.GetDiagnostics()
            .Where(d => d.Severity >= Microsoft.CodeAnalysis.DiagnosticSeverity.Warning)
            .Select(d => $"{d.Location.SourceTree?.FilePath} {d.Id} {d.GetMessage()}")
            .ToList();
        Assert.Empty(warnings);
    }

    // Before the fix the generated registration class FooRegistration hid a type FooRegistration of the namespace in the
    // generated file, so code naming that type did not compile.
    [Fact]
    public void RegistrationClassDoesNotHideTypeOfSameName()
    {
        var sources = GenerateAndCompile("""
            namespace TestNamespace;

            [BTDB.Generate]
            public class Foo
            {
                public FooRegistration? Other { get; set; }
            }

            [BTDB.Generate]
            public class FooRegistration
            {
                public int Value { get; set; }
            }

            [BTDB.Generate]
            public class Outer
            {
                [BTDB.Generate]
                public class Foo
                {
                    public int Value { get; set; }
                }
            }

            [BTDB.Generate]
            public delegate FooRegistration MakeRegistration(int value);

            public class MakeRegistrationRegistration
            {
            }
            """);
        Assert.Contains("static file class FooRegistration4BTDB", GetSource(sources, "TestNamespace.Foo.g.cs"));
        Assert.Contains("static file class FooRegistration", GetSource(sources, "TestNamespace.Outer.Foo.g.cs"));
    }

    // Before the fix generated code of a partial class declared the members Creator and Constr in it, which clashed with
    // members of the class of those names, like a Creator property.
    [Fact]
    public void PartialClassCanHaveMembersNamedLikeGeneratedOnes()
    {
        GenerateAndCompile("""
            namespace TestNamespace;

            public interface ILogger { }

            [BTDB.Generate]
            public partial class WithPrivateConstructor
            {
                WithPrivateConstructor(ILogger logger) { }
                public string? Creator { get; set; }
                public string? Constr { get; set; }
            }

            [BTDB.Generate]
            public partial class WithDefaultConstructor
            {
                public string? Creator { get; set; }
                public int Constr;
            }
            """);
    }
}
