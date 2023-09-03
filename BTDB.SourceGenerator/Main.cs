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
            (node, _) => node is ClassDeclarationSyntax or InterfaceDeclarationSyntax or DelegateDeclarationSyntax,
            (syntaxContext, _) =>
            {
                var semanticModel = syntaxContext.SemanticModel;

                // Symbols allow us to get the compile-time information.
                if (semanticModel.GetDeclaredSymbol(syntaxContext.Node) is not INamedTypeSymbol symbol)
                    return null!;
                if (syntaxContext.Node is DelegateDeclarationSyntax delegateDeclarationSyntax)
                {
                    if (!symbol.GetAttributes().Any(a =>
                            a.AttributeClass is { Name: AttributeName } attr &&
                            attr.ContainingNamespace?.ToDisplayString() == Namespace))
                    {
                        return null!;
                    }
                    if (symbol.DeclaredAccessibility == Accessibility.Private)
                    {
                        return null!;
                    }
                    var containingNamespace = symbol.ContainingNamespace;
                    var namespaceName = containingNamespace.IsGlobalNamespace
                        ? null
                        : containingNamespace.ToDisplayString();
                    var delegateName = symbol.Name;
                    var returnType = symbol.DelegateInvokeMethod!.ReturnType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                    var parameters = symbol.DelegateInvokeMethod!.Parameters.Select(p => new ParameterInfo(p.Name,
                                             p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                                             p.Type.IsReferenceType,
                                             p.IsOptional || p.NullableAnnotation == NullableAnnotation.Annotated,
                                             p.HasExplicitDefaultValue ? p.ExplicitDefaultValue!=null ? ExtractDefaultValue(p.DeclaringSyntaxReferences[0], p.Type) : null : null))
                                         .ToImmutableArray();
                    return new GenerationInfo(GenerationType.Delegate, namespaceName, delegateName,
                        symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), false, parameters,
                        new[] { new PropertyInfo("", returnType, null, true, false, false) }.ToImmutableArray(),
                        ImmutableArray<string>.Empty);
                }
                if (syntaxContext.Node is ClassDeclarationSyntax classDeclarationSyntax)
                {
                    var isPartial = classDeclarationSyntax.Modifiers
                        .Any(m => m.ValueText == "partial");
                    if (!symbol.GetAttributes().Any(a =>
                            a.AttributeClass is { Name: AttributeName } attr &&
                            attr.ContainingNamespace?.ToDisplayString() == Namespace)
                        && !symbol.AllInterfaces.Any(interfaceSymbol => interfaceSymbol.GetAttributes().Any(a =>
                            a.AttributeClass is { Name: AttributeName } attr &&
                            attr.ContainingNamespace?.ToDisplayString() == Namespace))
                        && !(symbol.BaseType?.GetAttributes().Any(a =>
                            a.AttributeClass is { Name: AttributeName } attr &&
                            attr.ContainingNamespace?.ToDisplayString() == Namespace) ?? false))
                    {
                        return null!;
                    }

                    if (symbol.DeclaredAccessibility == Accessibility.Private)
                    {
                        return null!;
                    }

                    if (symbol.IsAbstract)
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
                                             p.IsOptional || p.NullableAnnotation == NullableAnnotation.Annotated,
                                             p.HasExplicitDefaultValue ? p.ExplicitDefaultValue!=null ? ExtractDefaultValue(p.DeclaringSyntaxReferences[0], p.Type) : null : null))
                                         .ToImmutableArray() ??
                                     ImmutableArray<ParameterInfo>.Empty;

                    var parentDeclarations = ImmutableArray<string>.Empty;
                    if (isPartial)
                    {
                        parentDeclarations = classDeclarationSyntax.AncestorsAndSelf().OfType<TypeDeclarationSyntax>().Select(c =>
                        {
                            if (c.Modifiers.All(m => m.ValueText != "partial") || c.Modifiers.Any(m=>m.ValueText=="file"))
                            {
                                isPartial = false;
                                return "";
                            }

                            return c.Modifiers + " " + c.Keyword.ValueText + " " + c.Identifier.ValueText;
                        }).ToImmutableArray();
                    }
                    var propertyInfos = symbol.GetMembers()
                        .OfType<IPropertySymbol>()
                        .Where(p=> p.GetAttributes().Any(a => a.AttributeClass?.Name == "DependencyAttribute") && (isPartial || p.SetMethod is { DeclaredAccessibility: Accessibility.Public or Accessibility.Internal }))
                        .Select(p => new PropertyInfo(p.Name,
                            p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                            p.GetAttributes().FirstOrDefault(a => a.AttributeClass?.Name == "DependencyAttribute")
                                ?.ConstructorArguments.FirstOrDefault().Value as string,
                            p.Type.IsReferenceType,
                            p.NullableAnnotation == NullableAnnotation.Annotated, p.SetMethod!.IsInitOnly))
                        .ToImmutableArray();
                    return new GenerationInfo(GenerationType.Class, namespaceName, className, symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), isPartial, parameters, propertyInfos, parentDeclarations);
                }

                return null!;
            }).Where(i => i != null);

        context.RegisterSourceOutput(gen.Collect(), GenerateCode);
    }

    static string? ExtractDefaultValue(SyntaxReference syntaxReference, ITypeSymbol typeSymbol)
    {
        var p = (ParameterSyntax)syntaxReference.GetSyntax();
        var s = p.Default?.Value.ToString();
        if (s == null) return null;
        if (s.StartsWith(typeSymbol.Name + "."))
        {
            return typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) + s.Substring(typeSymbol.Name.Length);
        }
        return $"({typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)}){s}";
    }

    static void GenerateCode(SourceProductionContext context,
        ImmutableArray<GenerationInfo> generationInfos)
    {
        foreach (var generationInfo in generationInfos)
        {
            if (generationInfo.GenType == GenerationType.Delegate)
            {
                GenerateDelegateFactory(context, generationInfo);
            }
            else
            {
                GenerateClassFactory(context, generationInfo);
            }
        }
    }

    static void GenerateDelegateFactory(SourceProductionContext context, GenerationInfo generationInfo)
    {
        var namespaceLine = "";
        if (generationInfo.Namespace != null)
        {
            // language=c#
            namespaceLine = $"\nnamespace {generationInfo.Namespace};\n";
        }

        var factoryCode1 = new StringBuilder();
        var factoryCode2 = new StringBuilder();
        var factoryCode3 = new StringBuilder();
        var factoryCode4 = new StringBuilder();
        var funcParams = new StringBuilder();
        var parametersCode = new StringBuilder();
        var parameterIndex = 0;

        foreach (var (name, type, isReference, optional, defaultValue) in generationInfo.ConstructorParameters)
        {
            if (parameterIndex > 0) parametersCode.Append(", ");
            parametersCode.Append($"{type} p{parameterIndex}");
            funcParams.Append($"{type},");
            factoryCode1.Append($"var p{parameterIndex}Idx = ctx.AddInstanceToCtx(typeof({type}), \"{name}\");\n            ");
            factoryCode2.Append($"var p{parameterIndex}Backup = r.Exchange(p{parameterIndex}Idx, p{parameterIndex});\n                    ");
            factoryCode3.Append($"    r.Set(p{parameterIndex}Idx, p{parameterIndex}Backup);\n                    ");
            factoryCode4.Append($"r.Set(p{parameterIndex}Idx, p{parameterIndex});\n                    ");
            parameterIndex++;
        }

        var resultingType = generationInfo.Properties[0].Type;
        // language=c#
        var code = $$"""
         // <auto-generated/>
         #nullable enable
         using System;
         using System.Runtime.CompilerServices;
         {{namespaceLine}}
         static file class {{generationInfo.Name}}Registration
         {
             [ModuleInitializer]
             internal static void Register4BTDB()
             {
                 BTDB.IOC.IContainer.RegisterFactory(typeof({{generationInfo.FullName}}).TypeHandle.Value, Factory);
                 BTDB.IOC.IContainer.RegisterFactory(typeof(Func<{{funcParams}}object>).TypeHandle.Value, Factory);
                 static Func<BTDB.IOC.IContainer,BTDB.IOC.IResolvingCtx?,object> Factory(BTDB.IOC.IContainer container, BTDB.IOC.ICreateFactoryCtx ctx)
                 {
                     var hasResolvingCtx = ctx.HasResolvingCtx();
                     {{factoryCode1}}var nestedFactory = container.CreateFactory(ctx, typeof({{resultingType}}), null);
                     if (nestedFactory == null) return null;
                     if (hasResolvingCtx)
                     {
                         return (c, r) => ({{parametersCode}}) =>
                         {
                             {{factoryCode2}}try
                             {
                                 return nestedFactory(c, r);
                             }
                             finally
                             {
                             {{factoryCode3}}}
                         };
                     }
                     else
                     {
                         var paramSize = ctx.GetParamSize();
                         return (c, _) => ({{parametersCode}}) =>
                         {
                             var r = new global::BTDB.IOC.ResolvingCtx(paramSize);
                             {{factoryCode4}}return nestedFactory(c, r);
                         };
                     }
                 }
             }
         }
         """;

        context.AddSource(
            $"{(generationInfo.Namespace == null ? "" : generationInfo.Namespace + ".") + generationInfo.Name}.g.cs",
            SourceText.From(code, Encoding.UTF8));
    }

    static void GenerateClassFactory(SourceProductionContext context, GenerationInfo generationInfo)
    {
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

        foreach (var (name, type, isReference, optional, defaultValue) in generationInfo.ConstructorParameters)
        {
            if (parameterIndex > 0) parametersCode.Append(", ");
            factoryCode.Append($"var f{parameterIndex} = container.CreateFactory(ctx, typeof({type}), \"{name}\");");
            factoryCode.Append("\n            ");
            if (!optional)
            {
                factoryCode.Append(
                    $"if (f{parameterIndex} == null) throw new global::System.ArgumentException(\"Cannot resolve {type.Replace("global::", "")} {name} parameter of {generationInfo.FullName.Replace("global::", "")}\");");
                factoryCode.Append("\n            ");
                parametersCode.Append(isReference ? $"Unsafe.As<{type}>(" : $"({type})(");
                parametersCode.Append($"f{parameterIndex}(container2, ctx2))");
            }
            else
            {
                parametersCode.Append($"f{parameterIndex} != null ? ");
                parametersCode.Append(isReference ? $"Unsafe.As<{type}>(" : $"(({type})");
                parametersCode.Append($"f{parameterIndex}(container2, ctx2)) : " +
                                      (defaultValue ?? $"default({type})"));
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
            factoryCode.Append(
                $"var f{parameterIndex} = container.CreateFactory(ctx, typeof({type}), \"{dependencyName}\");");
            factoryCode.Append("\n            ");
            if (!optional)
            {
                factoryCode.Append(
                    $"if (f{parameterIndex} == null) throw new global::System.ArgumentException(\"Cannot resolve {type.Replace("global::", "")} {name} property of {generationInfo.FullName.Replace("global::", "")}\");");
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

        var declarations = new StringBuilder();
        if (generationInfo.IsPartial)
        {
            foreach (var parentDeclaration in generationInfo.ParentDeclarations.Reverse())
            {
                declarations.Append(parentDeclaration);
                declarations.Append("\n{\n");
            }
        }
        else
        {
            declarations.Append($"static file class {generationInfo.Name}Registration\n{{\n");
        }

        // language=c#
        var code = $$"""
                     // <auto-generated/>
                     using System;
                     using System.Runtime.CompilerServices;
                     {{namespaceLine}}
                     {{declarations}}    [ModuleInitializer]
                         internal static void Register4BTDB()
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
                     {{(generationInfo.IsPartial ? new string('}', generationInfo.ParentDeclarations.Length) : "}")}}
                     """;

        context.AddSource(
            $"{(generationInfo.Namespace == null ? "" : generationInfo.Namespace + ".") + generationInfo.Name}.g.cs",
            SourceText.From(code, Encoding.UTF8));
    }
}

enum GenerationType
{
    Class,
    Delegate
}

record GenerationInfo(GenerationType GenType, string? Namespace, string Name, string FullName, bool IsPartial, ImmutableArray<ParameterInfo> ConstructorParameters, ImmutableArray<PropertyInfo> Properties, ImmutableArray<string> ParentDeclarations);

record ParameterInfo(string Name, string Type, bool IsReference, bool Optional, string? DefaultValue);

record PropertyInfo(string Name, string Type, string? DependencyName, bool IsReference, bool Optional, bool IsInitOnly);
