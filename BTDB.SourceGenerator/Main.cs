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
    const string GenerateForName = "GenerateForAttribute";

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var gen = context.SyntaxProvider.CreateSyntaxProvider(
            (node, _) => node is ClassDeclarationSyntax or InterfaceDeclarationSyntax or DelegateDeclarationSyntax
                or RecordDeclarationSyntax or AttributeSyntax,
            (syntaxContext, _) =>
            {
                var semanticModel = syntaxContext.SemanticModel;

                if (syntaxContext.Node is AttributeSyntax attributeSyntax)
                {
                    var symbolInfo = semanticModel.GetSymbolInfo(attributeSyntax);
                    IMethodSymbol? ms = null;
                    if (symbolInfo.CandidateReason == CandidateReason.NotAnAttributeType &&
                        symbolInfo.CandidateSymbols.FirstOrDefault() is IMethodSymbol ms1)
                    {
                        ms = ms1;
                    }
                    else if (symbolInfo.Symbol is IMethodSymbol ms2)
                    {
                        ms = ms2;
                    }

                    if (ms == null) return null!;
                    if (ms.ContainingType.Name != GenerateForName)
                        return null!;
                    if (ms.ContainingNamespace.Name != Namespace)
                        return null!;
                    var typeParam =
                        attributeSyntax.ArgumentList!.Arguments.FirstOrDefault()?.Expression as TypeOfExpressionSyntax;
                    if (typeParam == null)
                        return null!;
                    if (semanticModel.GetSymbolInfo(typeParam.Type).Symbol is not INamedTypeSymbol symb)
                        return null!;
                    var constructorParametersExpression = attributeSyntax.ArgumentList!.Arguments
                        .FirstOrDefault(a => a.NameEquals?.Name?.Identifier.ValueText == "ConstructorParameters")
                        ?.Expression;
                    INamedTypeSymbol[]? constructorParameters = null;
                    if (constructorParametersExpression is not null)
                    {
                        if (constructorParametersExpression is not CollectionExpressionSyntax aces)
                        {
                            return new GenerationInfo(GenerationType.Error, null, "BTDB0001",
                                "Must use CollectionExpression syntax for ConstructorParameters", null, false, false,
                                false, EquatableArray<ParameterInfo>.Empty, EquatableArray<PropertyInfo>.Empty,
                                EquatableArray<string>.Empty, EquatableArray<DispatcherInfo>.Empty,
                                EquatableArray<FieldsInfo>.Empty, EquatableArray<TypeRef>.Empty,
                                EquatableArray<CollectionInfo>.Empty, EquatableArray<GenerationInfo>.Empty,
                                constructorParametersExpression.GetLocation());
                        }

                        constructorParameters = aces.Elements
                            .Select(e =>
                            {
                                var expressionSyntax = (e as ExpressionElementSyntax)?.Expression;
                                var typeSyntax = (expressionSyntax as TypeOfExpressionSyntax)?.Type;
                                if (typeSyntax == null) return null;
                                var info = semanticModel.GetSymbolInfo(typeSyntax);
                                return info.Symbol;
                            })
                            .OfType<INamedTypeSymbol>()
                            .ToArray();
                    }

                    return GenerationInfoForClass(symb, null, false, constructorParameters, semanticModel,
                        new HashSet<CollectionInfo>(), new HashSet<GenerationInfo>());
                }

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
                        .ToArray();
                    return new GenerationInfo(GenerationType.Delegate, namespaceName, delegateName,
                        symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), null, false, false, false,
                        parameters, [new PropertyInfo("", returnType, null, true, false, false, false, null)],
                        EquatableArray<string>.Empty, EquatableArray<DispatcherInfo>.Empty,
                        EquatableArray<FieldsInfo>.Empty, EquatableArray<TypeRef>.Empty,
                        EquatableArray<CollectionInfo>.Empty, EquatableArray<GenerationInfo>.Empty, null);
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
                        symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), null, false, false, false,
                        EquatableArray<ParameterInfo>.Empty,
                        EquatableArray<PropertyInfo>.Empty, EquatableArray<string>.Empty,
                        dispatchers.ToArray(), EquatableArray<FieldsInfo>.Empty,
                        EquatableArray<TypeRef>.Empty, EquatableArray<CollectionInfo>.Empty,
                        EquatableArray<GenerationInfo>.Empty, null);
                }

                if (syntaxContext.Node is ClassDeclarationSyntax classDeclarationSyntax)
                {
                    if (symbol.IsGenericType)
                    {
                        return null;
                    }

                    if (symbol.DeclaredAccessibility == Accessibility.Protected)
                    {
                        return null;
                    }

                    // If it has GenerateForAttribute then it has priority over GenerateAttribute
                    if (symbol.GetAttributes().Any(a =>
                            a.AttributeClass is { Name: GenerateForName } attr &&
                            attr.ContainingNamespace?.ToDisplayString() == Namespace))
                        return null;
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
                        return null;
                    }

                    var isPartial = classDeclarationSyntax.Modifiers
                        .Any(m => m.ValueText == "partial");

                    return GenerationInfoForClass(symbol, classDeclarationSyntax, isPartial, null, semanticModel,
                        new HashSet<CollectionInfo>(), new HashSet<GenerationInfo>());
                }

                if (syntaxContext.Node is RecordDeclarationSyntax recordDeclarationSyntax)
                {
                    if (symbol.IsGenericType)
                    {
                        return null;
                    }

                    if (symbol.DeclaredAccessibility == Accessibility.Protected)
                    {
                        return null;
                    }

                    // If it has GenerateForAttribute then it has priority over GenerateAttribute
                    if (symbol.GetAttributes().Any(a =>
                            a.AttributeClass is { Name: GenerateForName } attr &&
                            attr.ContainingNamespace?.ToDisplayString() == Namespace))
                        return null;
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
                        return null;
                    }

                    var isPartial = recordDeclarationSyntax.Modifiers
                        .Any(m => m.ValueText == "partial");

                    return GenerationInfoForClass(symbol, recordDeclarationSyntax, isPartial, null, semanticModel,
                        new HashSet<CollectionInfo>(), new HashSet<GenerationInfo>());
                }

                return null!;
            }).Where(i => i != null);
        gen = gen.SelectMany((g, _) => g!.Nested.IsEmpty ? Enumerable.Repeat(g, 1) : [g, ..g.Nested])!;
        context.RegisterSourceOutput(gen.Collect(), GenerateCode!);
    }

    static GenerationInfo? GenerationInfoForClass(INamedTypeSymbol symbol,
        TypeDeclarationSyntax? classDeclarationSyntax,
        bool isPartial, INamedTypeSymbol[]? constructorParameters, SemanticModel model,
        HashSet<CollectionInfo> collections, HashSet<GenerationInfo> nested, HashSet<string>? processed = null)
    {
        if (symbol.DeclaredAccessibility == Accessibility.Private)
        {
            return null;
        }

        if (symbol.IsAbstract)
        {
            return null;
        }

        if (symbol.IsUnboundGenericType)
        {
            return null;
        }

        if (processed == null)
        {
            processed = new HashSet<string>();
            processed.Add(symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat));
        }
        else
        {
            if (!processed.Add(symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)))
            {
                return null;
            }
        }

        var persistedName = ExtractPersistedName(symbol);

        var implements = symbol.AllInterfaces.Select(s => new TypeRef(s)).ToArray();

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
        IMethodSymbol? constructor = null;
        var hasDefaultConstructor = false;
        foreach (var symbolConstructor in symbol.Constructors)
        {
            if (symbolConstructor.Parameters.Any(p => SymbolEqualityComparer.Default.Equals(p.Type, symbol)))
                continue;
            if (constructorParameters != null)
            {
                if (symbolConstructor.Parameters.Length == constructorParameters.Length)
                {
                    var allMatch = true;
                    for (var i = 0; i < symbolConstructor.Parameters.Length; i++)
                    {
                        if (SymbolEqualityComparer.Default.Equals(symbolConstructor.Parameters[i].Type,
                                constructorParameters[i])) continue;
                        allMatch = false;
                        break;
                    }

                    if (allMatch)
                    {
                        constructor = symbolConstructor;
                        break;
                    }
                }
            }
            else
            {
                if (symbolConstructor.Parameters.Length == 0)
                {
                    hasDefaultConstructor = true;
                    constructor ??= symbolConstructor;
                }

                if (symbolConstructor.Parameters.Length > (constructor?.Parameters.Length ?? 0))
                {
                    constructor = symbolConstructor;
                }
            }
        }

        if (constructorParameters != null && constructor == null) return null;

        var parameters = constructor?.Parameters.Select(p => new ParameterInfo(p.Name,
                                 p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                                 p.Type.IsReferenceType,
                                 p.IsOptional || p.NullableAnnotation == NullableAnnotation.Annotated,
                                 p.HasExplicitDefaultValue
                                     ? CSharpSyntaxUtilities.FormatLiteral(p.ExplicitDefaultValue,
                                         new(p.Type))
                                     : null))
                             .ToArray() ??
                         Array.Empty<ParameterInfo>();

        var parentDeclarations = EquatableArray<string>.Empty;
        if (isPartial)
        {
            parentDeclarations = classDeclarationSyntax!.AncestorsAndSelf().OfType<TypeDeclarationSyntax>()
                .Select(c =>
                {
                    if (c.Modifiers.All(m => m.ValueText != "partial") ||
                        c.Modifiers.Any(m => m.ValueText == "file"))
                    {
                        isPartial = false;
                        return "";
                    }

                    return c.Modifiers + " " + c.Keyword.ValueText + " " + c.Identifier.ValueText;
                }).ToArray();
        }

        var propertyInfos = symbol.GetMembers()
            .OfType<IPropertySymbol>()
            .Where(p => !p.IsStatic)
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
            .ToArray();
        var fields = symbol.GetMembers()
            .OfType<IFieldSymbol>()
            .Where(f => !f.IsStatic)
            .Where(f =>
                f.DeclaredAccessibility == Accessibility.Public &&
                f.GetAttributes().All(a => a.AttributeClass?.Name != "DependencyAttribute") &&
                f.GetAttributes().All(a => a.AttributeClass?.Name != "NotStoredAttribute")
                || f.GetAttributes().Any(a => a.AttributeClass?.Name == "PersistedNameAttribute"))
            .Select(f =>
            {
                return new FieldsInfo(f.Name,
                    f.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    ExtractPersistedName(f),
                    f.Type.IsReferenceType, f.Name, null, null,
                    EquatableArray<IndexInfo>.Empty);
            })
            .Concat(symbol.GetMembers()
                .OfType<IPropertySymbol>()
                .Where(p => !p.IsStatic)
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
                        backingName = ExtractPropertyFromGetter(p.GetMethod!.DeclaringSyntaxReferences, model);
                        if (backingName != null) getterName = null;
                    }

                    return new FieldsInfo(p.Name,
                        p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                        ExtractPersistedName(p),
                        p.Type.IsReferenceType,
                        backingName, getterName, setterName, EquatableArray<IndexInfo>.Empty);
                })).ToArray();

        var fieldTypes = symbol.GetMembers()
            .OfType<IFieldSymbol>()
            .Where(f => !f.IsStatic)
            .Where(f =>
                f.DeclaredAccessibility == Accessibility.Public &&
                f.GetAttributes().All(a => a.AttributeClass?.Name != "DependencyAttribute") &&
                f.GetAttributes().All(a => a.AttributeClass?.Name != "NotStoredAttribute")
                || f.GetAttributes().Any(a => a.AttributeClass?.Name == "PersistedNameAttribute"))
            .Select(f => f.Type)
            .Concat(symbol.GetMembers()
                .OfType<IPropertySymbol>()
                .Where(p => !p.IsStatic)
                .Where(p =>
                    p.GetAttributes().All(a => a.AttributeClass?.Name != "DependencyAttribute") &&
                    p.GetAttributes().All(a => a.AttributeClass?.Name != "NotStoredAttribute") &&
                    p.SetMethod is not null && p.GetMethod is not null)
                .Select(p => p.Type));

        GatherCollections(model, fieldTypes, collections, nested, processed);

        var privateConstructor =
            constructor?.DeclaredAccessibility is Accessibility.Private or Accessibility.Protected ||
            GetAllMembersIncludingBase(symbol)
                .OfType<IFieldSymbol>()
                .Where(f => !f.IsStatic)
                .Any(f =>
                    f.IsRequired)
            ||
            GetAllMembersIncludingBase(symbol)
                .OfType<IPropertySymbol>()
                .Where(p => !p.IsStatic)
                .Any(p =>
                    p.IsRequired);
        return new GenerationInfo(GenerationType.Class, namespaceName, className,
            symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), persistedName, isPartial,
            privateConstructor,
            hasDefaultConstructor,
            parameters,
            propertyInfos, parentDeclarations, dispatchers.ToArray(), fields, implements, collections.ToArray(),
            nested.ToArray(), null);
    }

    static string? ExtractPersistedName(ISymbol symbol)
    {
        var persistedNameAttribute = symbol.GetAttributes()
            .FirstOrDefault(a => a.AttributeClass?.Name == "PersistedNameAttribute");

        var persistedName = GetStringArgForAttributeData(persistedNameAttribute);
        return persistedName;
    }

    static string? GetStringArgForAttributeData(AttributeData? attributeData)
    {
        if (attributeData?.ApplicationSyntaxReference?.GetSyntax() is AttributeSyntax attributeSyntax)
        {
            var literalExpressionSyntax =
                attributeSyntax.ArgumentList!.Arguments.FirstOrDefault()?.Expression as LiteralExpressionSyntax;
            return literalExpressionSyntax?.Token.ValueText;
        }

        return null;
    }

    static void GatherCollections(SemanticModel model, IEnumerable<ITypeSymbol> types,
        HashSet<CollectionInfo> collections,
        HashSet<GenerationInfo> nested, HashSet<string> processed)
    {
        foreach (var type in types)
        {
            GatherCollection(model, type, collections, nested, processed);
        }
    }

    static void GatherCollection(SemanticModel model, ITypeSymbol type, HashSet<CollectionInfo> collections,
        HashSet<GenerationInfo> nested, HashSet<string> processed)
    {
        if (type.TypeKind != TypeKind.Class && type.TypeKind != TypeKind.Interface) return;
        if (type is INamedTypeSymbol namedTypeSymbol)
        {
            if (namedTypeSymbol.IsGenericType)
            {
                foreach (var typeArgument in namedTypeSymbol.TypeArguments)
                {
                    GatherCollection(model, typeArgument, collections, nested, processed);
                }
            }

            var fullName = namedTypeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            ReadOnlySpan<string> collectionToInstance =
            [
                "global::System.Collections.Generic.List<", "global::System.Collections.Generic.List<",
                "global::System.Collections.Generic.IList<", "global::System.Collections.Generic.List<",
                "global::System.Collections.Generic.IReadOnlyList<", "global::System.Collections.Generic.List<",
                "global::System.Collections.Generic.HashSet<", "global::System.Collections.Generic.HashSet<",
                "global::System.Collections.Generic.ISet<", "global::System.Collections.Generic.HashSet<",
                "global::System.Collections.Generic.IReadOnlySet<", "global::System.Collections.Generic.HashSet<",
                "global::System.Collections.Generic.IEnumerable<", "global::System.Collections.Generic.List<",
            ];
            string? instance = null;
            for (var i = 0; i < collectionToInstance.Length; i += 2)
            {
                if (fullName.StartsWith(collectionToInstance[i], StringComparison.Ordinal))
                {
                    instance = collectionToInstance[i + 1] + fullName.Substring(collectionToInstance[i].Length);
                    break;
                }
            }

            if (instance != null)
            {
                var elementType = namedTypeSymbol.TypeArguments[0];
                var collectionInfo = new CollectionInfo(fullName, instance,
                    elementType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    elementType.IsReferenceType, null, null);
                if (collections.Add(collectionInfo))
                    GatherType(elementType, nested, model, collections, processed);
            }
            else
            {
                ReadOnlySpan<string> collectionToInstance2 =
                [
                    "global::System.Collections.Generic.Dictionary<", "global::System.Collections.Generic.Dictionary<",
                    "global::System.Collections.Generic.IDictionary<", "global::System.Collections.Generic.Dictionary<",
                    "global::System.Collections.Generic.IReadOnlyDictionary<",
                    "global::System.Collections.Generic.Dictionary<",
                ];
                instance = null;
                for (var i = 0; i < collectionToInstance2.Length; i += 2)
                {
                    if (fullName.StartsWith(collectionToInstance2[i], StringComparison.Ordinal))
                    {
                        instance = collectionToInstance2[i + 1] + fullName.Substring(collectionToInstance2[i].Length);
                        break;
                    }
                }

                if (instance != null)
                {
                    var keyType = namedTypeSymbol.TypeArguments[0];
                    var valueType = namedTypeSymbol.TypeArguments[1];
                    var collectionInfo = new CollectionInfo(fullName, instance,
                        keyType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                        keyType.IsReferenceType, valueType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                        valueType.IsReferenceType);
                    if (collections.Add(collectionInfo))
                    {
                        GatherType(keyType, nested, model, collections, processed);
                        GatherType(valueType, nested, model, collections, processed);
                    }
                }
                else
                {
                    GatherType(namedTypeSymbol, nested, model, collections, processed);
                }
            }
        }
    }

    static void GatherType(ITypeSymbol typeSymbol, HashSet<GenerationInfo> nested, SemanticModel model,
        HashSet<CollectionInfo> collections, HashSet<string> processed)
    {
        if (typeSymbol.IsValueType)
        {
            var gi = new GenerationInfo(GenerationType.Struct, typeSymbol.ContainingNamespace.ToString(),
                typeSymbol.Name, typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), null, false,
                false,
                true, EquatableArray<ParameterInfo>.Empty, EquatableArray<PropertyInfo>.Empty,
                EquatableArray<string>.Empty, EquatableArray<DispatcherInfo>.Empty, EquatableArray<FieldsInfo>.Empty,
                EquatableArray<TypeRef>.Empty, EquatableArray<CollectionInfo>.Empty,
                EquatableArray<GenerationInfo>.Empty, null);
            nested.Add(gi);
        }
        else if (typeSymbol.IsReferenceType && typeSymbol is INamedTypeSymbol namedTypeSymbol)
        {
            GenerationInfoForClass(namedTypeSymbol, null, false, null, model,
                collections, nested, processed);
        }
    }

    static IEnumerable<ISymbol> GetAllMembersIncludingBase(INamedTypeSymbol symbol)
    {
        var members = new List<ISymbol>();
        var currentSymbol = symbol;

        while (currentSymbol != null)
        {
            members.AddRange(currentSymbol.GetMembers());
            currentSymbol = currentSymbol.BaseType;
        }

        return members;
    }

    static string? ExtractPropertyFromGetter(ImmutableArray<SyntaxReference> declaringSyntaxReferences,
        SemanticModel model)
    {
        if (declaringSyntaxReferences.IsEmpty) return null;
        if (declaringSyntaxReferences.Length > 1) return null;
        var syntax = declaringSyntaxReferences[0].GetSyntax();
        if (syntax is not AccessorDeclarationSyntax ads) return null;
        if (ads.ExpressionBody is { } aecs)
        {
            if (aecs.Expression is IdentifierNameSyntax ins && model.GetSymbolInfo(ins) is { Symbol: IFieldSymbol })
            {
                return ins.Identifier.ValueText;
            }
        }

        return null;
    }

    static bool IsDefaultMethodImpl(ImmutableArray<SyntaxReference> setMethodDeclaringSyntaxReferences)
    {
        if (setMethodDeclaringSyntaxReferences.IsEmpty) return false;
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
        var dedupe = new HashSet<string>();
        var collections = new HashSet<CollectionInfo>();
        var structs = new HashSet<GenerationInfo>();
        foreach (var generationInfo in generationInfos)
        {
            if (generationInfo.GenType == GenerationType.Error)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    new DiagnosticDescriptor(generationInfo.Name, generationInfo.FullName, generationInfo.FullName,
                        "BTDB",
                        DiagnosticSeverity.Error, true), generationInfo.Location));
                continue;
            }

            if (!dedupe.Add(generationInfo.FullName)) continue;
            if (generationInfo.GenType == GenerationType.Delegate)
            {
                GenerateDelegateFactory(context, generationInfo);
            }
            else if (generationInfo.GenType == GenerationType.Interface)
            {
                GenerateInterfaceFactory(context, generationInfo);
            }
            else if (generationInfo.GenType == GenerationType.Class)
            {
                foreach (var collectionInfo in generationInfo.CollectionInfos.AsSpan())
                {
                    collections.Add(collectionInfo);
                }

                GenerateClassFactory(context, generationInfo);
            }
            else if (generationInfo.GenType == GenerationType.Struct)
            {
                structs.Add(generationInfo);
            }
        }

        if (collections.Count > 0) GenerateCollectionRegistrations(context, collections);
        if (structs.Count > 0) GenerateStructStackAllocations(context, structs);
    }

    static void GenerateStructStackAllocations(SourceProductionContext context, HashSet<GenerationInfo> structs)
    {
        var factoryCode = new StringBuilder();
        // language=c#
        factoryCode.Append($$"""
            // <auto-generated/>
            #nullable enable
            #pragma warning disable CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
            using System;
            using System.Runtime.CompilerServices;
            [CompilerGenerated]
            static file class StackAllocationRegistrations
            {
                [ModuleInitializer]
                internal static unsafe void Register4BTDB()
                {
            """);

        var idx = 0;
        foreach (var @struct in structs)
        {
            idx++;
            factoryCode.Append($$"""

                        BTDB.Serialization.ReflectionMetadata.RegisterStackAllocator(typeof({{@struct.FullName}}), &Allocate{{idx}});
                        static void Allocate{{idx}}(ref byte ctx, ref nint ptr, delegate*<ref byte, void> chain)
                            {
                                {{@struct.FullName}} value;
                                ptr = (nint)(&value);
                                chain(ref ctx);
                                ptr = 0;
                            }

                """);
        }

        // language=c#
        factoryCode.Append("""
                }
            }

            """);
        context.AddSource(
            $"StackAllocationRegistrations.g.cs",
            SourceText.From(factoryCode.ToString(), Encoding.UTF8));
    }

    static void GenerateCollectionRegistrations(SourceProductionContext context, HashSet<CollectionInfo> collections)
    {
        var factoryCode = new StringBuilder();
        // language=c#
        factoryCode.Append($$"""
            // <auto-generated/>
            #nullable enable
            #pragma warning disable 612,618
            using System;
            using System.Runtime.CompilerServices;
            [CompilerGenerated]
            static file class CollectionRegistrations
            {
                [ModuleInitializer]
                internal static void Register4BTDB()
                {
            """);

        var idx = 0;
        foreach (var collection in collections)
        {
            idx++;
            if (collection.ValueType != null)
            {
                factoryCode.Append($$"""

                            BTDB.Serialization.ReflectionMetadata.RegisterCollection(new()
                            {
                                Type = typeof({{collection.FullName}}),
                                ElementKeyType = typeof({{collection.KeyType}}),
                                ElementValueType = typeof({{collection.ValueType}}),
                                Creator = &Create{{idx}},
                                AdderKeyValue = &Add{{idx}}
                            });

                            static object Create{{idx}}(uint capacity)
                            {
                                return new {{collection.InstantiableFullName}}((int)capacity);
                            }

                            static void Add{{idx}}(object c, ref byte key, ref byte value)
                            {
                                Unsafe.As<{{collection.InstantiableFullName}}>(c).Add(Unsafe.As<byte, {{collection.KeyType}}>(ref key), Unsafe.As<byte, {{collection.ValueType}}>(ref value));
                            }

                    """);
            }
            else
            {
                // language=c#
                factoryCode.Append($$"""

                            BTDB.Serialization.ReflectionMetadata.RegisterCollection(new()
                            {
                                Type = typeof({{collection.FullName}}),
                                ElementKeyType = typeof({{collection.KeyType}}),
                                Creator = &Create{{idx}},
                                Adder = &Add{{idx}}
                            });

                            static object Create{{idx}}(uint capacity)
                            {
                                return new {{collection.InstantiableFullName}}((int)capacity);
                            }

                            static void Add{{idx}}(object c, ref byte value)
                            {
                                Unsafe.As<{{collection.InstantiableFullName}}>(c).Add(Unsafe.As<byte, {{collection.KeyType}}>(ref value));
                            }

                    """);
            }
        }

        // language=c#
        factoryCode.Append("""
                }
            }

            """);
        context.AddSource(
            $"CollectionRegistrations.g.cs",
            SourceText.From(factoryCode.ToString(), Encoding.UTF8));
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
            var normalizeType = NormalizeType(type);
            if (parameterIndex > 0) parametersCode.Append(", ");
            parametersCode.Append($"{normalizeType} p{parameterIndex}");
            funcParams.Append($"{normalizeType},");
            factoryCode1.Append(
                $"var p{parameterIndex}Idx = ctx.AddInstanceToCtx(typeof({normalizeType}), \"{name}\");\n            ");
            factoryCode2.Append(
                $"var p{parameterIndex}Backup = r!.Exchange(p{parameterIndex}Idx, p{parameterIndex});\n                    ");
            factoryCode3.Append($"    r!.Set(p{parameterIndex}Idx, p{parameterIndex}Backup);\n                    ");
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
            [CompilerGenerated]
            static file class {{generationInfo.Name}}Registration
            {
                [ModuleInitializer]
                internal static void Register4BTDB()
                {
                    global::BTDB.IOC.IContainer.RegisterFactory(typeof({{generationInfo.FullName}}), Factory);
                    global::BTDB.IOC.IContainer.RegisterFactory(typeof(Func<{{funcParams}}{{resultingType}}>), Factory);
                    static Func<global::BTDB.IOC.IContainer,global::BTDB.IOC.IResolvingCtx?,object>? Factory(global::BTDB.IOC.IContainer container, global::BTDB.IOC.ICreateFactoryCtx ctx)
                    {
                        using var resolvingCtxRestorer = ctx.ResolvingCtxRestorer();
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
            var normalizeType = NormalizeType(type);
            if (parameterIndex > 0) parametersCode.Append(", ");
            factoryCode.Append(
                $"var f{parameterIndex} = container.CreateFactory(ctx, typeof({normalizeType}), \"{name}\");");
            factoryCode.Append("\n            ");
            if (!optional)
            {
                factoryCode.Append(
                    $"if (f{parameterIndex} == null) throw new global::System.ArgumentException(\"Cannot resolve {normalizeType.Replace("global::", "")} {name} parameter of {generationInfo.FullName.Replace("global::", "")}\");");
                factoryCode.Append("\n            ");
                parametersCode.Append(isReference ? $"Unsafe.As<{normalizeType}>(" : $"({normalizeType})(");
                parametersCode.Append($"f{parameterIndex}(container2, ctx2))");
            }
            else
            {
                parametersCode.Append($"f{parameterIndex} != null ? ");
                parametersCode.Append(isReference ? $"Unsafe.As<{normalizeType}>(" : $"(({normalizeType})");
                parametersCode.Append($"f{parameterIndex}(container2, ctx2)) : " +
                                      (defaultValue ?? $"default({normalizeType})"));
            }

            parameterIndex++;
        }

        foreach (var propertyInfo in generationInfo.Properties)
        {
            var name = propertyInfo.Name;
            var normalizedType = NormalizeType(propertyInfo.Type);
            var dependencyName = propertyInfo.DependencyName ?? name;
            var isReference = propertyInfo.IsReference;
            var optional = propertyInfo.Optional;
            factoryCode.Append(
                $"var f{parameterIndex} = container.CreateFactory(ctx, typeof({normalizedType}), \"{dependencyName}\");");
            factoryCode.Append("\n            ");
            if (!optional)
            {
                factoryCode.Append(
                    $"if (f{parameterIndex} == null) throw new global::System.ArgumentException(\"Cannot resolve {normalizedType.Replace("global::", "")} {name} property of {generationInfo.FullName.Replace("global::", "")}\");");
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
                    propertyCode.Append(isReference ? $"Unsafe.As<{normalizedType}>(" : $"({normalizedType})(");
                    propertyCode.Append($"f{parameterIndex}(container2, ctx2));");
                }
                else
                {
                    // language=c#
                    additionalDeclarations.Append($"""
                            [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{propertyInfo.BackingName}")]
                            extern static void MethodSetter{name}({generationInfo.FullName} @this, {normalizedType} value);

                        """);
                    propertyCode.Append($"MethodSetter{name}(res, ");
                    propertyCode.Append(isReference ? $"Unsafe.As<{normalizedType}>(" : $"({normalizedType})(");
                    propertyCode.Append($"f{parameterIndex}(container2, ctx2)));");
                }
            }
            else
            {
                propertyCode.Append($"res.{name} = ");
                propertyCode.Append(isReference ? $"Unsafe.As<{normalizedType}>(" : $"({normalizedType})(");
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
            declarations.Append("[CompilerGenerated]\n");
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

        if (generationInfo.Fields.Count != 0 || generationInfo.PersistedName != null)
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
            metadataCode.Append($"""

                        var metadata = new global::BTDB.Serialization.ClassMetadata();
                        metadata.Name = "{generationInfo.Name}";
                        metadata.Type = typeof({generationInfo.FullName});
                        metadata.Namespace = "{generationInfo.Namespace ?? ""}";
                """);
            if (generationInfo.PersistedName != null)
            {
                metadataCode.Append($"""

                            metadata.PersistedName = "{generationInfo.PersistedName}";
                    """);
            }

            metadataCode.Append($$"""

                        metadata.Implements = [{{string.Join(", ", generationInfo.Implements.Where(i => i.FullyQualifiedName.StartsWith("global::", StringComparison.Ordinal)).Select(i => $"typeof({i.FullyQualifiedName})"))}}];
                        metadata.Creator = &Creator;
                        var dummy = Unsafe.As<{{generationInfo.FullName}}>(metadata);
                        metadata.Fields = new[]
                        {

                """);
            var fieldIndex = 0;
            foreach (var field in generationInfo.Fields)
            {
                var normalizedType = NormalizeType(field.Type);
                fieldIndex++;
                // language=c#
                metadataCode.Append($$"""
                                new global::BTDB.Serialization.FieldMetadata
                                {
                                    Name = "{{field.StoredName ?? field.Name}}",
                                    Type = typeof({{normalizedType}}),

                    """);
                if (field.BackingName != null)
                {
                    // language=c#
                    declarations.Append($"""
                            [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "{field.BackingName}")]
                            extern static ref {normalizedType} Field{fieldIndex}({generationInfo.FullName} @this);

                        """);
                    // language=c#
                    metadataCode.Append($"""
                                        ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref Field{fieldIndex}(dummy)),

                        """);
                }

                if (field is { GetterName: not null })
                {
                    // language=c#
                    declarations.Append($$"""
                            [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{{field.GetterName}}")]
                            extern static {{normalizedType}} Getter{{fieldIndex}}({{generationInfo.FullName}} @this);
                            static void GenGetter{{fieldIndex}}(object @this, ref byte value)
                            {
                                Unsafe.As<byte, {{normalizedType}}>(ref value) = Getter{{fieldIndex}}(Unsafe.As<{{generationInfo.FullName}}>(@this));
                            }

                        """);
                    // language=c#
                    metadataCode.Append($"""
                                        PropRefGetter = &GenGetter{fieldIndex},

                        """);
                }

                if (field is { SetterName: not null })
                {
                    // language=c#
                    declarations.Append($$"""
                            [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{{field.SetterName}}")]
                            extern static void Setter{{fieldIndex}}({{generationInfo.FullName}} @this, {{normalizedType}} value);
                            static void GenSetter{{fieldIndex}}(object @this, ref byte value)
                            {
                                Setter{{fieldIndex}}(Unsafe.As<{{generationInfo.FullName}}>(@this), Unsafe.As<byte, {{normalizedType}}>(ref value));
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
            #pragma warning disable 612,618
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
            {{(generationInfo.IsPartial ? new('}', generationInfo.ParentDeclarations.Count) : "}")}}

            """;

        context.AddSource(
            $"{generationInfo.FullName.Replace("global::", "").Replace("<", "[").Replace(">", "]")}.g.cs",
            SourceText.From(code, Encoding.UTF8));
    }

    static string NormalizeType(string type)
    {
        if (type == "dynamic") return "object";
        return type;
    }
}

enum GenerationType
{
    Class,
    Delegate,
    Interface,
    Struct,
    Error
}

record GenerationInfo(
    GenerationType GenType,
    string? Namespace,
    string Name,
    string FullName,
    string? PersistedName,
    bool IsPartial,
    bool PrivateConstructor, // or has required fields
    bool HasDefaultConstructor,
    EquatableArray<ParameterInfo> ConstructorParameters,
    EquatableArray<PropertyInfo> Properties,
    EquatableArray<string> ParentDeclarations,
    EquatableArray<DispatcherInfo> Dispatchers,
    EquatableArray<FieldsInfo> Fields,
    EquatableArray<TypeRef> Implements,
    EquatableArray<CollectionInfo> CollectionInfos,
    EquatableArray<GenerationInfo> Nested,
    Location? Location
)
{
    public virtual bool Equals(GenerationInfo? other)
    {
        if (ReferenceEquals(this, other)) return true;
        if (other is null) return false;
        return GenType == other.GenType &&
               Namespace == other.Namespace &&
               Name == other.Name &&
               FullName == other.FullName;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(GenType, Namespace, Name, FullName);
    }
}

record CollectionInfo(
    string FullName,
    string InstantiableFullName,
    string KeyType,
    bool KeyIsReference,
    string? ValueType,
    bool? ValueIsReference)
{
    public virtual bool Equals(CollectionInfo? other)
    {
        if (ReferenceEquals(this, other)) return true;
        if (other is null) return false;
        return FullName == other.FullName;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(FullName);
    }
}

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
    EquatableArray<IndexInfo> Indexes);

record IndexInfo(string? Name, int Order);

record DispatcherInfo(string Name, string? Type, string? ResultType, string IfaceName);
