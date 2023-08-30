using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace BTDB.SourceGenerator;

/// <summary>
/// A sample source generator that creates a custom report based on class properties. The target class should be annotated with the 'Generators.ReportAttribute' attribute.
/// When using the source code as a baseline, an incremental source generator is preferable because it reduces the performance overhead.
/// </summary>
[Generator]
public class SourceGenerator : IIncrementalGenerator
{
    const string Namespace = "BTDB";
    const string AttributeName = "GenerateAttribute";

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var gen = context.SyntaxProvider.CreateSyntaxProvider(
            (node, _) => node is ClassDeclarationSyntax or InterfaceDeclarationSyntax,
            (syntaxContext, _) =>
            {
                var semanticModel = syntaxContext.SemanticModel;

                // Symbols allow us to get the compile-time information.
                if (semanticModel.GetDeclaredSymbol(syntaxContext.Node) is not INamedTypeSymbol symbol)
                    return null!;
                if (syntaxContext.Node is ClassDeclarationSyntax classDeclarationSyntax)
                {
                    var isPartial = classDeclarationSyntax.Modifiers
                        .Any(m => m.ValueText == "partial");
                    if (!symbol.GetAttributes().Any(a =>
                            a.AttributeClass is { Name: AttributeName } attr &&
                            attr.ContainingNamespace?.ToDisplayString() == Namespace)
                        && !symbol.AllInterfaces.Any(interfaceSymbol => interfaceSymbol.GetAttributes().Any(a =>
                            a.AttributeClass is { Name: AttributeName } attr &&
                            attr.ContainingNamespace?.ToDisplayString() == Namespace)))
                    {
                        return null!;
                    }
                    var containingNamespace = symbol.ContainingNamespace;
                    var namespaceName = containingNamespace.IsGlobalNamespace
                        ? null
                        : containingNamespace.ToDisplayString();
                    var className = symbol.Name;
                    var constructor = symbol.Constructors.FirstOrDefault();
                    foreach (var symbolConstructor in symbol.Constructors)
                    {
                        if (symbolConstructor.Parameters.Length > constructor?.Parameters.Length)
                        {
                            constructor = symbolConstructor;
                        }
                    }

                    var parameters = constructor?.Parameters.Select(p => new ParameterInfo(p.Name,
                                             p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                                             p.Type.IsReferenceType,
                                             p.IsOptional || p.NullableAnnotation == NullableAnnotation.Annotated))
                                         .ToImmutableArray() ??
                                     ImmutableArray<ParameterInfo>.Empty;

                    var propertyInfos = symbol.GetMembers()
                        .OfType<IPropertySymbol>()
                        .Where(p=> p.GetAttributes().Any(a => a.AttributeClass?.Name == "DependencyAttribute") && p.SetMethod is { DeclaredAccessibility: Accessibility.Public or Accessibility.Internal })
                        .Select(p => new PropertyInfo(p.Name,
                            p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                            p.GetAttributes().FirstOrDefault(a => a.AttributeClass?.Name == "DependencyAttribute")
                                ?.ConstructorArguments.FirstOrDefault().Value as string,
                            p.Type.IsReferenceType,
                            p.NullableAnnotation == NullableAnnotation.Annotated, p.SetMethod!.IsInitOnly))
                        .ToImmutableArray();
                    return new GenerationInfo(namespaceName, className, symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), isPartial, parameters, propertyInfos);
                }

                return null!;
            }).Where(i => i != null);

        context.RegisterSourceOutput(gen.Collect(), GenerateCode);
    }

    static void GenerateCode(SourceProductionContext context,
        ImmutableArray<GenerationInfo> generationInfos)
    {
        // Go through all filtered class declarations.
        foreach (var generationInfo in generationInfos)
        {
            // We need to get semantic model of the class to retrieve metadata.
            // Build up the source code
            var namespaceLine = "";
            if (generationInfo.Namespace != null)
            {
                // language=c#
                namespaceLine = $"\nnamespace {generationInfo.Namespace};\n";
            }

            var factoryCode = new StringBuilder();
            var parametersCode = new StringBuilder();
            var propertyCode = new StringBuilder();
            var propertyInitOnlyCode = new StringBuilder();
            var parameterIndex = 0;

            foreach (var (name, type, isReference, optional) in generationInfo.ConstructorParameters)
            {
                if (parameterIndex > 0) parametersCode.Append(", ");
                factoryCode.Append($"var f{parameterIndex} = container.CreateFactory(ctx, typeof({type}), \"{name}\");");
                factoryCode.Append("\n            ");
                if (!optional)
                {
                    factoryCode.Append(
                        $"if (f{parameterIndex} == null) throw new BTDB.KVDBLayer.BTDBException(\"Cannot resolve {type.Replace("global::","")} {name} parameter of {generationInfo.FullName.Replace("global::","")}\");");
                    factoryCode.Append("\n            ");
                    parametersCode.Append(isReference ? $"Unsafe.As<{type}>(" : $"({type})(");
                    parametersCode.Append($"f{parameterIndex}(container2, ctx2))");
                }
                else
                {
                    parametersCode.Append($"f{parameterIndex} != null ? ");
                    parametersCode.Append(isReference ? $"Unsafe.As<{type}>(" : $"(({type})");
                    parametersCode.Append($"f{parameterIndex}(container2, ctx2)) : default({type})");
                }
                parameterIndex++;
            }
            foreach (var propertyInfo in generationInfo.Properties)
            {
                var name = propertyInfo.Name;
                var type = propertyInfo.Type;
                var dependencyName = propertyInfo.DependencyName ?? name;
                var isReference = propertyInfo.IsReference;
                var optional = propertyInfo.Optional;
                factoryCode.Append($"var f{parameterIndex} = container.CreateFactory(ctx, typeof({type}), \"{dependencyName}\");");
                factoryCode.Append("\n            ");
                if (!optional)
                {
                    factoryCode.Append(
                        $"if (f{parameterIndex} == null) throw new BTDB.KVDBLayer.BTDBException(\"Cannot resolve {type.Replace("global::","")} {name} property of {generationInfo.FullName.Replace("global::","")}\");");
                    factoryCode.Append("\n            ");
                }
                if (propertyInfo.IsInitOnly)
                {
                    if (propertyInitOnlyCode.Length > 0) propertyInitOnlyCode.Append(", ");
                    if (!optional)
                    {
                        propertyInitOnlyCode.Append($"{name} = ");
                        propertyInitOnlyCode.Append(isReference ? $"Unsafe.As<{type}>(" : $"({type})(");
                        propertyInitOnlyCode.Append($"f{parameterIndex}(container2, ctx2))");
                    }
                    else
                    {
                        propertyInitOnlyCode.Append($"{name} = f{parameterIndex} != null ?");
                        propertyInitOnlyCode.Append(isReference ? $"Unsafe.As<{type}>(" : $"({type})(");
                        propertyInitOnlyCode.Append($"f{parameterIndex}(container2, ctx2)) : default");
                    }
                }
                else
                {
                    if (!optional)
                    {
                        propertyCode.Append($"res.{name} = ");
                        propertyCode.Append(isReference ? $"Unsafe.As<{type}>(" : $"({type})(");
                        propertyCode.Append($"f{parameterIndex}(container2, ctx2));");
                    }
                    else
                    {
                        propertyCode.Append($"if (f{parameterIndex}!=null) res.{name} = ");
                        propertyCode.Append(isReference ? $"Unsafe.As<{type}>(" : $"(({type})");
                        propertyCode.Append($"f{parameterIndex}(container2, ctx2));");
                    }
                    propertyCode.Append("\n                ");
                }
                parameterIndex++;
            }

            if (propertyInitOnlyCode.Length > 0)
            {
                propertyInitOnlyCode.Append(" }");
                propertyInitOnlyCode.Insert(0, " { ");
            }
            // language=c#
            var code = $$"""
                // <auto-generated/>
                using System;
                using System.Runtime.CompilerServices;
                {{namespaceLine}}
                static file class {{generationInfo.Name}}Registration
                {
                    [ModuleInitializer]
                    internal static void Init()
                    {
                        BTDB.IOC.IContainer.RegisterFactory(typeof({{generationInfo.FullName}}).TypeHandle.Value, (container, ctx) =>
                        {
                            {{factoryCode}}return (container2, ctx2) =>
                            {
                                var res = new {{generationInfo.FullName}}({{parametersCode}}){{propertyInitOnlyCode}};
                                {{propertyCode}}return res;
                            };
                        });
                    }
                }
                """;

            // Add the source code to the compilation.
            context.AddSource(
                $"{(generationInfo.Namespace == null ? "" : generationInfo.Namespace + ".") + generationInfo.Name}.g.cs",
                SourceText.From(code, Encoding.UTF8));
        }
    }
}

record GenerationInfo(string? Namespace, string Name, string FullName, bool IsPartial, ImmutableArray<ParameterInfo> ConstructorParameters, ImmutableArray<PropertyInfo> Properties);

record ParameterInfo(string Name, string Type, bool IsReference, bool Optional);

record PropertyInfo(string Name, string Type, string? DependencyName, bool IsReference, bool Optional, bool IsInitOnly);
