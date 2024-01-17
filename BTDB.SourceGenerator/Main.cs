using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace BTDB.SourceGenerator;

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
                if (syntaxContext.Node is DelegateDeclarationSyntax)
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
                    var returnType =
                        symbol.DelegateInvokeMethod!.ReturnType.ToDisplayString(
                            SymbolDisplayFormat.FullyQualifiedFormat);
                    var parameters = symbol.DelegateInvokeMethod!.Parameters.Select(p => new ParameterInfo(p.Name,
                            p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                            p.Type.IsReferenceType,
                            p.IsOptional || p.NullableAnnotation == NullableAnnotation.Annotated,
                            p.HasExplicitDefaultValue
                                ? CSharpSyntaxUtilities.FormatLiteral(p.ExplicitDefaultValue, new(p.Type))
                                : null))
                        .ToImmutableArray();
                    return new GenerationInfo(GenerationType.Delegate, namespaceName, delegateName,
                        symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), false, false, false,
                        parameters,
                        new[] { new PropertyInfo("", returnType, null, true, false, false, false, null) }
                            .ToImmutableArray(),
                        ImmutableArray<string>.Empty, ImmutableArray<DispatcherInfo>.Empty,
                        ImmutableArray<FieldsInfo>.Empty, ImmutableArray<string>.Empty);
                }

                if (syntaxContext.Node is InterfaceDeclarationSyntax)
                {
                    var dispatchers = DetectDispatcherInfo(symbol);
                    if (dispatchers.Length == 0) return null;
                    var containingNamespace = symbol.ContainingNamespace;
                    var namespaceName = containingNamespace.IsGlobalNamespace
                        ? null
                        : containingNamespace.ToDisplayString();
                    var interfaceName = symbol.Name;
                    return new(GenerationType.Interface, namespaceName, interfaceName,
                        symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), false, false, false,
                        ImmutableArray<ParameterInfo>.Empty,
                        ImmutableArray<PropertyInfo>.Empty, ImmutableArray<string>.Empty,
                        dispatchers.ToImmutableArray(), ImmutableArray<FieldsInfo>.Empty, ImmutableArray<string>.Empty);
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

                    var implements = symbol.AllInterfaces.Select(s => s.ToDisplayString()).ToImmutableArray();

                    var dispatchers = ImmutableArray.CreateBuilder<DispatcherInfo>();
                    foreach (var (name, type, resultType, ifaceName) in symbol.AllInterfaces.SelectMany(
                                 DetectDispatcherInfo))
                    {
                        var m = symbol.GetMembers().OfType<IMethodSymbol>().FirstOrDefault(m =>
                            m.Name == name && m.Parameters.Length == 1);
                        if (m == null) continue;
                        dispatchers.Add(new(name,
                            m.Parameters[0].Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                            m.ReturnType.SpecialType == SpecialType.System_Void ? null : m.ReturnType.ToDisplayString(),
                            ifaceName));
                    }

                    var containingNamespace = symbol.ContainingNamespace;
                    var namespaceName = containingNamespace.IsGlobalNamespace
                        ? null
                        : containingNamespace.ToDisplayString();
                    var className = symbol.Name;
                    var constructor = symbol.Constructors.FirstOrDefault();
                    var hasDefaultConstructor = false;
                    foreach (var symbolConstructor in symbol.Constructors)
                    {
                        if (symbolConstructor.Parameters.Length == 0)
                        {
                            hasDefaultConstructor = true;
                        }

                        if (symbolConstructor.Parameters.Length > constructor?.Parameters.Length)
                        {
                            constructor = symbolConstructor;
                        }
                    }

                    var parameters = constructor?.Parameters.Select(p => new ParameterInfo(p.Name,
                                             p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                                             p.Type.IsReferenceType,
                                             p.IsOptional || p.NullableAnnotation == NullableAnnotation.Annotated,
                                             p.HasExplicitDefaultValue
                                                 ? CSharpSyntaxUtilities.FormatLiteral(p.ExplicitDefaultValue,
                                                     new(p.Type))
                                                 : null))
                                         .ToImmutableArray() ??
                                     ImmutableArray<ParameterInfo>.Empty;

                    var parentDeclarations = ImmutableArray<string>.Empty;
                    if (isPartial)
                    {
                        parentDeclarations = classDeclarationSyntax.AncestorsAndSelf().OfType<TypeDeclarationSyntax>()
                            .Select(c =>
                            {
                                if (c.Modifiers.All(m => m.ValueText != "partial") ||
                                    c.Modifiers.Any(m => m.ValueText == "file"))
                                {
                                    isPartial = false;
                                    return "";
                                }

                                return c.Modifiers + " " + c.Keyword.ValueText + " " + c.Identifier.ValueText;
                            }).ToImmutableArray();
                    }

                    var propertyInfos = symbol.GetMembers()
                        .OfType<IPropertySymbol>()
                        .Where(p => p.GetAttributes().Any(a => a.AttributeClass?.Name == "DependencyAttribute") &&
                                    p.SetMethod is not null)
                        .Select(p =>
                        {
                            var isComplex = p.SetMethod!.DeclaredAccessibility == Accessibility.Private ||
                                            p.SetMethod.IsInitOnly;
                            // if Set/init method have default implementation => then it could directly set backing field
                            var isFieldBased = isComplex && IsDefaultMethodImpl(p.SetMethod.DeclaringSyntaxReferences);
                            return new PropertyInfo(p.Name,
                                p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                                p.GetAttributes().FirstOrDefault(a => a.AttributeClass?.Name == "DependencyAttribute")
                                    ?.ConstructorArguments.FirstOrDefault().Value as string,
                                p.Type.IsReferenceType,
                                p.NullableAnnotation == NullableAnnotation.Annotated, isComplex, isFieldBased,
                                isComplex ? isFieldBased ? $"<{p.Name}>k__BackingField" : p.SetMethod.Name : null);
                        })
                        .ToImmutableArray();
                    var fields = symbol.GetMembers()
                        .OfType<IFieldSymbol>()
                        .Where(f =>
                            f.DeclaredAccessibility == Accessibility.Public &&
                            f.GetAttributes().All(a => a.AttributeClass?.Name != "DependencyAttribute") &&
                            f.GetAttributes().All(a => a.AttributeClass?.Name != "NotStoredAttribute")
                            || f.GetAttributes().Any(a => a.AttributeClass?.Name == "PersistedNameAttribute"))
                        .Select(f =>
                        {
                            return new FieldsInfo(f.Name,
                                f.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                                f.GetAttributes()
                                    .FirstOrDefault(a => a.AttributeClass?.Name == "PersistedNameAttribute")
                                    ?.ConstructorArguments.FirstOrDefault().Value as string,
                                f.Type.IsReferenceType, f.Name, null, null,
                                ImmutableArray<IndexInfo>.Empty);
                        })
                        .Concat(symbol.GetMembers()
                            .OfType<IPropertySymbol>()
                            .Where(p =>
                                p.GetAttributes().All(a => a.AttributeClass?.Name != "DependencyAttribute") &&
                                p.GetAttributes().All(a => a.AttributeClass?.Name != "NotStoredAttribute") &&
                                p.SetMethod is not null && p.GetMethod is not null)
                            .Select(p =>
                            {
                                var getterName = !IsDefaultMethodImpl(p.GetMethod!.DeclaringSyntaxReferences)
                                    ? p.GetMethod.Name
                                    : null;
                                var setterName = !IsDefaultMethodImpl(p.SetMethod!.DeclaringSyntaxReferences)
                                    ? p.SetMethod.Name
                                    : null;
                                var backingName = getterName == null || setterName == null
                                    ? $"<{p.Name}>k__BackingField"
                                    : null;
                                if (getterName != null && backingName == null)
                                {
                                    backingName = ExtractPropertyFromGetter(p.GetMethod!.DeclaringSyntaxReferences);
                                    if (backingName != null) getterName = null;
                                }

                                return new FieldsInfo(p.Name,
                                    p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                                    p.GetAttributes().FirstOrDefault(a =>
                                            a.AttributeClass?.Name == "PersistedNameAttribute")
                                        ?.ConstructorArguments.FirstOrDefault().Value as string,
                                    p.Type.IsReferenceType,
                                    backingName, getterName, setterName, ImmutableArray<IndexInfo>.Empty);
                            })).ToImmutableArray();
                    var privateConstructor =
                        constructor?.DeclaredAccessibility is Accessibility.Private or Accessibility.Protected;
                    return new GenerationInfo(GenerationType.Class, namespaceName, className,
                        symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), isPartial, privateConstructor,
                        hasDefaultConstructor,
                        parameters,
                        propertyInfos, parentDeclarations, dispatchers.ToImmutable(), fields, implements);
                }

                return null!;
            }).Where(i => i != null);

        context.RegisterSourceOutput(gen.Collect(), GenerateCode!);
    }

    string? ExtractPropertyFromGetter(ImmutableArray<SyntaxReference> declaringSyntaxReferences)
    {
        if (declaringSyntaxReferences.IsEmpty) return null;
        if (declaringSyntaxReferences.Length > 1) return null;
        var syntax = declaringSyntaxReferences[0].GetSyntax();
        if (syntax is not AccessorDeclarationSyntax ads) return null;
        if (ads.ExpressionBody is ArrowExpressionClauseSyntax aecs)
        {
            if (aecs.Expression is IdentifierNameSyntax ins)
                return ins.Identifier.ValueText;
        }

        return null;
    }

    bool IsDefaultMethodImpl(ImmutableArray<SyntaxReference> setMethodDeclaringSyntaxReferences)
    {
        if (setMethodDeclaringSyntaxReferences.IsEmpty) return true;
        if (setMethodDeclaringSyntaxReferences.Length > 1) return false;
        var syntax = setMethodDeclaringSyntaxReferences[0].GetSyntax();
        if (syntax is AccessorDeclarationSyntax { Body: null, ExpressionBody: null })
        {
            return true;
        }

        return false;
    }

    static DispatcherInfo[] DetectDispatcherInfo(INamedTypeSymbol symbol)
    {
        if (!symbol.GetAttributes().Any(a =>
                a.AttributeClass is { Name: AttributeName } attr &&
                attr.ContainingNamespace?.ToDisplayString() == Namespace))
        {
            return Array.Empty<DispatcherInfo>();
        }

        if (symbol is not { Kind: SymbolKind.NamedType, TypeKind: TypeKind.Interface })
        {
            return Array.Empty<DispatcherInfo>();
        }

        if (symbol.DeclaredAccessibility == Accessibility.Private)
        {
            return Array.Empty<DispatcherInfo>();
        }

        var builder = new List<DispatcherInfo>();
        foreach (var member in symbol.GetMembers())
        {
            if (member is IMethodSymbol { IsPartialDefinition: true, Parameters.Length: 1 } methodSymbol &&
                methodSymbol.Name.StartsWith("Create") && methodSymbol.Name.EndsWith("Dispatcher")
                &&
                IsContainer(methodSymbol.Parameters[0].Type)
                &&
                methodSymbol.ReturnType is IFunctionPointerTypeSymbol
                {
                    Signature:
                    {
                        ReturnType: { } returnType,
                        Parameters: { Length: 2 } parameters
                    }
                } && IsNullableObject(returnType) && IsContainer(parameters[0].Type) && IsObject(parameters[1].Type))
            {
                builder.Add(new(methodSymbol.Name.Substring(6, methodSymbol.Name.Length - 6 - 10),
                    parameters[1].Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    returnType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)));
            }
        }

        return builder.Count == 0 ? Array.Empty<DispatcherInfo>() : builder.ToArray();
    }

    static bool IsObject(ITypeSymbol type)
    {
        return type.Name == "Object" && type.ContainingNamespace?.ToDisplayString() == "System" &&
               type.NullableAnnotation != NullableAnnotation.Annotated;
    }

    static bool IsNullableObject(ITypeSymbol type)
    {
        return type.Name == "Object" && type.ContainingNamespace?.ToDisplayString() == "System" &&
               type.NullableAnnotation == NullableAnnotation.Annotated;
    }

    static bool IsContainer(ITypeSymbol type)
    {
        return type.Name == "IContainer" && type.ContainingNamespace?.ToDisplayString() == "BTDB.IOC" &&
               type.NullableAnnotation != NullableAnnotation.Annotated;
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
            else if (generationInfo.GenType == GenerationType.Interface)
            {
                GenerateInterfaceFactory(context, generationInfo);
            }
            else
            {
                GenerateClassFactory(context, generationInfo);
            }
        }
    }

    static void GenerateInterfaceFactory(SourceProductionContext context, GenerationInfo generationInfo)
    {
        var namespaceLine = "";
        if (generationInfo.Namespace != null)
        {
            // language=c#
            namespaceLine = $"\nnamespace {generationInfo.Namespace};\n";
        }

        var factoryCode = new StringBuilder();
        // language=c#
        factoryCode.Append($$"""
            // <auto-generated/>
            #nullable enable
            using System;
            using System.Runtime.CompilerServices;
            {{namespaceLine}}
            public partial interface {{generationInfo.Name}}
            {
            """);

        foreach (var (name, type, resultType, _) in generationInfo.Dispatchers)
        {
            // language=c#
            factoryCode.Append($$"""

                    public static readonly global::BTDB.Collections.RefDictionary<nint, global::BTDB.IOC.DispatcherItem> {{name}}Handlers = new();

                    public static unsafe partial delegate*<global::BTDB.IOC.IContainer, {{type}}, {{resultType}}?> Create{{name}}Dispatcher(global::BTDB.IOC.IContainer container)
                    {
                        foreach(var idx in {{name}}Handlers.Index)
                        {
                            {{name}}Handlers.ValueRef(idx).Execute = {{name}}Handlers.ValueRef(idx).ExecuteFactory(container);
                        }
                        static {{resultType}}? Consume(global::BTDB.IOC.IContainer container, {{type}} message)
                        {
                            if ({{name}}Handlers.TryGetValue(message.GetType().TypeHandle.Value, out var handler))
                            {
                                return Unsafe.As<{{resultType}}>(handler.Execute!(container, message));
                            }
                            throw new InvalidOperationException($"No handler for message {message.GetType().FullName}");
                        }

                        return &Consume;
                    }

                """);
        }

        // language=c#
        factoryCode.Append("""
            }

            """);
        context.AddSource(
            $"{(generationInfo.Namespace == null ? "" : generationInfo.Namespace + ".") + generationInfo.Name}.g.cs",
            SourceText.From(factoryCode.ToString(), Encoding.UTF8));
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

        foreach (var (name, type, _, _, _) in generationInfo.ConstructorParameters)
        {
            if (parameterIndex > 0) parametersCode.Append(", ");
            parametersCode.Append($"{type} p{parameterIndex}");
            funcParams.Append($"{type},");
            factoryCode1.Append(
                $"var p{parameterIndex}Idx = ctx.AddInstanceToCtx(typeof({type}), \"{name}\");\n            ");
            factoryCode2.Append(
                $"var p{parameterIndex}Backup = r.Exchange(p{parameterIndex}Idx, p{parameterIndex});\n                    ");
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
                    global::BTDB.IOC.IContainer.RegisterFactory(typeof({{generationInfo.FullName}}), Factory);
                    global::BTDB.IOC.IContainer.RegisterFactory(typeof(Func<{{funcParams}}{{resultingType}}>), Factory);
                    static Func<global::BTDB.IOC.IContainer,global::BTDB.IOC.IResolvingCtx?,object> Factory(global::BTDB.IOC.IContainer container, global::BTDB.IOC.ICreateFactoryCtx ctx)
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
        var additionalDeclarations = new StringBuilder();
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

            if (optional)
            {
                propertyCode.Append($"if (f{parameterIndex} != null) ");
            }

            if (propertyInfo.IsComplex)
            {
                if (propertyInfo.IsFieldBased)
                {
                    // language=c#
                    additionalDeclarations.Append($"""
                            [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "{propertyInfo.BackingName}")]
                            extern static ref {propertyInfo.Type} FieldRef{propertyInfo.Name}({generationInfo.FullName} @this);

                        """);
                    propertyCode.Append($"FieldRef{name}(res) = ");
                    propertyCode.Append(isReference ? $"Unsafe.As<{type}>(" : $"({type})(");
                    propertyCode.Append($"f{parameterIndex}(container2, ctx2));");
                }
                else
                {
                    // language=c#
                    additionalDeclarations.Append($"""
                            [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{propertyInfo.BackingName}")]
                            extern static void MethodSetter{propertyInfo.Name}({generationInfo.FullName} @this, {propertyInfo.Type} value);

                        """);
                    propertyCode.Append($"MethodSetter{name}(res, ");
                    propertyCode.Append(isReference ? $"Unsafe.As<{type}>(" : $"({type})(");
                    propertyCode.Append($"f{parameterIndex}(container2, ctx2)));");
                }
            }
            else
            {
                propertyCode.Append($"res.{name} = ");
                propertyCode.Append(isReference ? $"Unsafe.As<{type}>(" : $"({type})(");
                propertyCode.Append($"f{parameterIndex}(container2, ctx2));");
            }

            propertyCode.Append("\n                ");

            parameterIndex++;
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

        if (generationInfo.PrivateConstructor)
        {
            var constructorParameters = new StringBuilder();
            foreach (var (name, type, _, _, _) in generationInfo.ConstructorParameters)
            {
                if (constructorParameters.Length > 0) constructorParameters.Append(", ");
                constructorParameters.Append($"{type} {name}");
            }

            // language=c#
            declarations.Append($"""
                    [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
                    extern static {generationInfo.FullName} Constr({constructorParameters});

                """);
        }

        declarations.Append(additionalDeclarations);

        var metadataCode = new StringBuilder();

        if (!generationInfo.Fields.IsEmpty)
        {
            if (generationInfo.HasDefaultConstructor)
            {
                // language=c#
                declarations.Append($"""
                        [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
                        extern static {generationInfo.FullName} Creator();

                    """);
            }
            else
            {
                // language=c#
                declarations.Append($$"""
                        static object Creator()
                        {
                            return RuntimeHelpers.GetUninitializedObject(typeof({{generationInfo.FullName}}));
                        }

                    """);
            }

            // language=c#
            metadataCode.Append($$"""

                        var metadata = new global::BTDB.Serialization.ClassMetadata();
                        metadata.Name = "{{generationInfo.Name}}";
                        metadata.Type = typeof({{generationInfo.FullName}});
                        metadata.Namespace = "{{generationInfo.Namespace ?? ""}}";
                        metadata.Implements = [{{string.Join(", ", generationInfo.Implements.Select(i => $"typeof({i})"))}}];
                        metadata.Creator = &Creator;
                        var dummy = Unsafe.As<{{generationInfo.FullName}}>(metadata);
                        metadata.Fields = new[]
                        {

                """);
            var fieldIndex = 0;
            foreach (var field in generationInfo.Fields)
            {
                fieldIndex++;
                // language=c#
                metadataCode.Append($$"""
                                new global::BTDB.Serialization.FieldMetadata
                                {
                                    Name = "{{field.Name}}",
                                    Type = typeof({{field.Type}}),

                    """);
                if (field.BackingName != null)
                {
                    // language=c#
                    declarations.Append($"""
                            [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "{field.BackingName}")]
                            extern static ref {field.Type} Field{fieldIndex}({generationInfo.FullName} @this);

                        """);
                    // language=c#
                    metadataCode.Append($"""
                                        ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref Field{fieldIndex}(dummy)),

                        """);
                }

                if (field is { GetterName: not null, IsReference: true })
                {
                    // language=c#
                    declarations.Append($$"""
                            [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{{field.GetterName}}")]
                            extern static {{field.Type}} Getter{{fieldIndex}}({{generationInfo.FullName}} @this);
                            static object GenGetter{{fieldIndex}}(object @this)
                            {
                                return Getter{{fieldIndex}}(Unsafe.As<{{generationInfo.FullName}}>(@this));
                            }

                        """);
                    // language=c#
                    metadataCode.Append($$"""
                                        PropObjGetter = &GenGetter{{fieldIndex}},

                        """);
                }

                if (field is { GetterName: not null, IsReference: false })
                {
                    // language=c#
                    declarations.Append($$"""
                            [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{{field.GetterName}}")]
                            extern static {{field.Type}} Getter{{fieldIndex}}({{generationInfo.FullName}} @this);
                            static void GenGetter{{fieldIndex}}(object @this, ref byte value)
                            {
                                Unsafe.As<byte, {{field.Type}}>(ref value) = Getter{{fieldIndex}}(Unsafe.As<{{generationInfo.FullName}}>(@this));
                            }

                        """);
                    // language=c#
                    metadataCode.Append($"""
                                        PropRefGetter = &GenGetter{fieldIndex},

                        """);
                }

                if (field is { SetterName: not null, IsReference: true })
                {
                    // language=c#
                    declarations.Append($$"""
                            [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{{field.SetterName}}")]
                            extern static void Setter{{fieldIndex}}({{generationInfo.FullName}} @this, {{field.Type}} value);
                            static void GenSetter{{fieldIndex}}(object @this, object value)
                            {
                                Setter{{fieldIndex}}(Unsafe.As<{{generationInfo.FullName}}>(@this), Unsafe.As<{{field.Type}}>(value));
                            }

                        """);
                    // language=c#
                    metadataCode.Append($"""
                                        PropObjSetter = &GenSetter{fieldIndex},

                        """);
                }

                if (field is { SetterName: not null, IsReference: false })
                {
                    // language=c#
                    declarations.Append($$"""
                            [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{{field.SetterName}}")]
                            extern static void Setter{{fieldIndex}}({{generationInfo.FullName}} @this, {{field.Type}} value);
                            static void GenSetter{{fieldIndex}}(object @this, ref byte value)
                            {
                                Setter{{fieldIndex}}(Unsafe.As<{{generationInfo.FullName}}>(@this), Unsafe.As<byte, {{field.Type}}>(ref value));
                            }

                        """);
                    // language=c#
                    metadataCode.Append($"""
                                        PropRefSetter = &GenSetter{fieldIndex},

                        """);
                }

                // language=c#
                metadataCode.Append("            },\n");
            }

            // language=c#
            metadataCode.Append($$"""
                        };
                        global::BTDB.Serialization.ReflectionMetadata.Register(metadata);
                """);
        }

        var dispatchers = new StringBuilder();
        foreach (var (name, type, resultType, ifaceName) in generationInfo.Dispatchers)
        {
            // language=c#
            dispatchers.Append($$"""

                        {{ifaceName}}.{{name}}Handlers.GetOrAddValueRef(typeof({{type}}).TypeHandle.Value).ExecuteFactory = (global::BTDB.IOC.IContainer c) => {
                           var nestedFactory = c.CreateFactory(typeof({{generationInfo.FullName}}));
                           return (container, message) =>
                           {
                               var res = nestedFactory(container, null);
                               {{(resultType != null ? "return " : "")}}Unsafe.As<{{generationInfo.FullName}}>(res).{{name}}(Unsafe.As<{{type}}>(message));
                               {{(resultType != null ? "" : "return null;")}}
                           };
                        };
                """);
        }

        // language=c#
        var code = $$"""
            // <auto-generated/>
            using System;
            using System.Runtime.CompilerServices;
            {{namespaceLine}}
            {{declarations}}    [ModuleInitializer]
                internal static unsafe void Register4BTDB()
                {
                    global::BTDB.IOC.IContainer.RegisterFactory(typeof({{generationInfo.FullName}}), (container, ctx) =>
                    {
                        {{factoryCode}}return (container2, ctx2) =>
                        {
                            var res = {{(generationInfo.PrivateConstructor ? "Constr" : "new " + generationInfo.FullName)}}({{parametersCode}});
                            {{propertyCode}}return res;
                        };
                    });{{metadataCode}}{{dispatchers}}
                }
            {{(generationInfo.IsPartial ? new('}', generationInfo.ParentDeclarations.Length) : "}")}}

            """;

        context.AddSource(
            $"{(generationInfo.Namespace == null ? "" : generationInfo.Namespace + ".") + generationInfo.Name}.g.cs",
            SourceText.From(code, Encoding.UTF8));
    }
}

enum GenerationType
{
    Class,
    Delegate,
    Interface
}

record GenerationInfo(
    GenerationType GenType,
    string? Namespace,
    string Name,
    string FullName,
    bool IsPartial,
    bool PrivateConstructor,
    bool HasDefaultConstructor,
    ImmutableArray<ParameterInfo> ConstructorParameters,
    ImmutableArray<PropertyInfo> Properties,
    ImmutableArray<string> ParentDeclarations,
    ImmutableArray<DispatcherInfo> Dispatchers,
    ImmutableArray<FieldsInfo> Fields,
    ImmutableArray<string> Implements
);

record ParameterInfo(string Name, string Type, bool IsReference, bool Optional, string? DefaultValue);

record PropertyInfo(
    string Name,
    string Type,
    string? DependencyName,
    bool IsReference,
    bool Optional,
    bool IsComplex,
    bool IsFieldBased,
    string? BackingName);

record FieldsInfo(
    string Name,
    string Type,
    string? StoredName,
    bool IsReference,
    string? BackingName,
    string? GetterName,
    string? SetterName,
    ImmutableArray<IndexInfo> Indexes);

record IndexInfo(string? Name, int Order);

record DispatcherInfo(string Name, string? Type, string? ResultType, string IfaceName);
