using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.SymbolStore;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace BTDB.SourceGenerator;

[Generator]
public class SourceGenerator : IIncrementalGenerator
{
    const string AttributeName = "GenerateAttribute";
    const string GenerateForName = "GenerateForAttribute";
    const string OnSerializeAttributeName = "OnSerializeAttribute";
    const string OnBeforeRemoveAttributeName = "OnBeforeRemoveAttribute";
    const string CovariantRelationInterfaceName = "ICovariantRelation";

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var gen = context.SyntaxProvider.CreateSyntaxProvider(
            (node, _) => node is ClassDeclarationSyntax or InterfaceDeclarationSyntax or DelegateDeclarationSyntax
                or RecordDeclarationSyntax or AttributeSyntax,
            (syntaxContext, _) =>
            {
                try
                {
                    var semanticModel = syntaxContext.SemanticModel;

                    if (syntaxContext.Node is AttributeSyntax attributeSyntax)
                    {
                        var symbolInfo = ModelExtensions.GetSymbolInfo(semanticModel, attributeSyntax);
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
                        if (!ms.InBTDBNamespace())
                            return null!;
                        var typeParam =
                            attributeSyntax.ArgumentList!.Arguments.FirstOrDefault()?.Expression as
                                TypeOfExpressionSyntax;
                        if (typeParam == null)
                            return null!;
                        if (ModelExtensions.GetSymbolInfo(semanticModel, typeParam.Type)
                                .Symbol is not INamedTypeSymbol symb)
                            return null!;
                        var constructorParametersExpression = attributeSyntax.ArgumentList!.Arguments
                            .FirstOrDefault(a => a.NameEquals?.Name.Identifier.ValueText == "ConstructorParameters")
                            ?.Expression;
                        INamedTypeSymbol[]? constructorParameters = null;
                        if (constructorParametersExpression is not null)
                        {
                            if (constructorParametersExpression is not CollectionExpressionSyntax aces)
                            {
                                return GenerationError("BTDB0001",
                                    "Must use CollectionExpression syntax for ConstructorParameters",
                                    constructorParametersExpression.GetLocation());
                            }

                            constructorParameters = aces.Elements
                                .Select(e =>
                                {
                                    var expressionSyntax = (e as ExpressionElementSyntax)?.Expression;
                                    var typeSyntax = (expressionSyntax as TypeOfExpressionSyntax)?.Type;
                                    if (typeSyntax == null) return null;
                                    var info = ModelExtensions.GetSymbolInfo(semanticModel, typeSyntax);
                                    return info.Symbol;
                                })
                                .OfType<INamedTypeSymbol>()
                                .ToArray();
                        }

                        return GenerationInfoForClass(symb, null, false, constructorParameters, semanticModel,
                            [], [], false, false);
                    }

                    // Symbols allow us to get the compile-time information.
                    if (semanticModel.GetDeclaredSymbol(syntaxContext.Node) is not INamedTypeSymbol symbol)
                        return null!;
                    if (syntaxContext.Node is DelegateDeclarationSyntax)
                    {
                        if (!symbol.GetAttributes().Any(a =>
                                a.AttributeClass is { Name: AttributeName } attr &&
                                attr.InBTDBNamespace()))
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
                                DetectDependencyName(p),
                                p.Type.IsReferenceType,
                                p.IsOptional || p.NullableAnnotation == NullableAnnotation.Annotated,
                                p.HasExplicitDefaultValue
                                    ? CSharpSyntaxUtilities.FormatLiteral(p.ExplicitDefaultValue, new(p.Type))
                                    : null,
                                GetEnumUnderlyingType(p.Type)))
                            .ToArray();
                        return new(GenerationType.Delegate, namespaceName, delegateName,
                            symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), null, false, false, false,
                            false, false,
                            parameters, [new("", returnType, null, true, false, false, false, null)], [],
                            [], [], [],
                            [], [], [], [], null);
                    }

                    if (syntaxContext.Node is InterfaceDeclarationSyntax)
                    {
                        if (symbol.AllInterfaces.FirstOrDefault(IsICovariantRelation) is { } relation)
                        {
                            var relationType = relation.TypeArguments[0].OriginalDefinition;
                            if (relationType is not INamedTypeSymbol)
                                return null;
                            var containingNamespace = symbol.ContainingNamespace;
                            var namespaceName = containingNamespace.IsGlobalNamespace
                                ? null
                                : containingNamespace.ToDisplayString();
                            var interfaceName = symbol.Name;
                            var persistedName = ExtractPersistedName(symbol);
                            var generationInfo = GenerationInfoForClass((INamedTypeSymbol)relationType, null, false,
                                null,
                                semanticModel,
                                [], [], false, true);
                            if (generationInfo is not { GenType: GenerationType.Class }) return null;
                            var detectedError = DetectErrors(generationInfo.Fields.AsSpan(),
                                ((InterfaceDeclarationSyntax)syntaxContext.Node).Identifier.GetLocation());
                            if (detectedError != null)
                                return detectedError;
                            var methods = GetAllMethodsIncludingInheritance(symbol)
                                .Where(m => m.MethodKind == MethodKind.Ordinary &&
                                            !IsInterfaceMethodWithImplementation(m))
                                .ToList();
                            detectedError = DetectErrorsInMethods(generationInfo, methods, relationType, semanticModel);
                            if (detectedError != null)
                                return detectedError;
                            var methodsList = new List<MethodInfo>(methods.Count);
                            var variantsGenerationInfos = new List<GenerationInfo>();
                            var loadTypes = new HashSet<TypeRef> { new(relationType) };
                            foreach (var method in methods)
                            {
                                methodsList.Add(new(method.Name, IfVoidNull(method.ReturnType),
                                    method.Parameters.Select(p =>
                                        new ParameterInfo(p.Name, p.Type.ToDisplayString(), null,
                                            p.Type.IsReferenceType, p.IsOptional, null,
                                            GetEnumUnderlyingType(p.Type))).ToArray(), true,
                                    method.ContainingType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)));
                                var methodReturnType = method.ReturnType as INamedTypeSymbol;
                                if (methodReturnType is { TypeKind: TypeKind.Interface })
                                {
                                    // Check is return type is IEnumerator<>
                                    if (methodReturnType.OriginalDefinition.SpecialType ==
                                        SpecialType.System_Collections_Generic_IEnumerator_T)
                                    {
                                        return GenerationError("BTDB0009",
                                            "Cannot use IEnumerator<> as return type in " + method.Name,
                                            method.Locations[0]);
                                    }

                                    if (methodReturnType.OriginalDefinition.SpecialType ==
                                        SpecialType.System_Collections_Generic_IEnumerable_T)
                                    {
                                        // extract type argument
                                        var typeArgument = methodReturnType.TypeArguments
                                            .FirstOrDefault();
                                        if (typeArgument != null &&
                                            !SymbolEqualityComparer.Default.Equals(typeArgument, relationType) &&
                                            SerializableType(typeArgument))
                                        {
                                            var variantInfo = GenerationInfoForClass(
                                                (INamedTypeSymbol)typeArgument, null, false,
                                                null, semanticModel, [], [], false, false);
                                            if (variantInfo != null)
                                            {
                                                loadTypes.Add(new(typeArgument));
                                                variantsGenerationInfos.Add(variantInfo);
                                            }
                                        }
                                    }
                                    // If return type is BTDB.ODBLayer.IOrderedDictionaryEnumerator<,>
                                    else if (methodReturnType.OriginalDefinition.Name ==
                                             "IOrderedDictionaryEnumerator" &&
                                             methodReturnType.OriginalDefinition.InODBLayerNamespace() &&
                                             methodReturnType.TypeArguments.Length == 2)
                                    {
                                        var valueType = methodReturnType.TypeArguments[1];
                                        if (SerializableType(valueType))
                                        {
                                            var variantInfo = GenerationInfoForClass(
                                                (INamedTypeSymbol)valueType, null, false,
                                                null, semanticModel, [], [], false, false);
                                            if (variantInfo != null)
                                            {
                                                loadTypes.Add(new(valueType));
                                                variantsGenerationInfos.Add(variantInfo);
                                            }
                                        }
                                    }
                                }
                                else if (method.Name.StartsWith("FindBy") ||
                                         method.Name.StartsWith("FirstBy"))
                                {
                                    if (methodReturnType != null && SerializableType(methodReturnType))
                                    {
                                        var variantInfo = GenerationInfoForClass(
                                            methodReturnType, null, false,
                                            null, semanticModel, [], [], false, false);
                                        if (variantInfo != null)
                                        {
                                            loadTypes.Add(new(methodReturnType));
                                            variantsGenerationInfos.Add(variantInfo);
                                        }
                                    }
                                }
                                else if (method.Name.StartsWith("GatherBy"))
                                {
                                    // Extract type argument from the first parameter which must implement ICollection<>
                                    if (method.Parameters.Length > 0 &&
                                        method.Parameters[0].Type is INamedTypeSymbol
                                        {
                                            TypeKind: TypeKind.Interface,
                                            OriginalDefinition.SpecialType: SpecialType
                                                .System_Collections_Generic_ICollection_T
                                        } collectionType)
                                    {
                                        var typeArgument = collectionType.TypeArguments.FirstOrDefault();
                                        if (typeArgument != null && SerializableType(typeArgument))
                                        {
                                            var variantInfo = GenerationInfoForClass(
                                                (INamedTypeSymbol)typeArgument, null, false,
                                                null, semanticModel, [], [], false, false);
                                            if (variantInfo != null)
                                            {
                                                loadTypes.Add(new(typeArgument));
                                                variantsGenerationInfos.Add(variantInfo);
                                            }
                                        }
                                    }
                                }
                            }

                            return new(GenerationType.RelationIface, namespaceName, interfaceName,
                                symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), persistedName, false,
                                false, false, false, false,
                                [], [], [], [], generationInfo.Fields, methodsList.ToArray(),
                                loadTypes.ToArray(), [], [],
                                [
                                    generationInfo, ..generationInfo.Nested, ..variantsGenerationInfos,
                                    ..variantsGenerationInfos.SelectMany(g => g.Nested)
                                ], null);
                        }
                        else
                        {
                            var dispatchers = DetectDispatcherInfo(symbol);
                            if (dispatchers.Length == 0) return null;
                            var containingNamespace = symbol.ContainingNamespace;
                            var namespaceName = containingNamespace.IsGlobalNamespace
                                ? null
                                : containingNamespace.ToDisplayString();
                            var interfaceName = symbol.Name;
                            return new(GenerationType.Interface, namespaceName, interfaceName,
                                symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), null, false, false,
                                false, false, false,
                                [],
                                [], [],
                                dispatchers, [], [],
                                [], [], [],
                                [], null);
                        }
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
                                attr.InBTDBNamespace()))
                            return null;
                        if (!symbol.GetAttributes().Any(a =>
                                a.AttributeClass is { Name: AttributeName } attr &&
                                attr.InBTDBNamespace())
                            && !symbol.AllInterfaces.Any(interfaceSymbol => interfaceSymbol.GetAttributes().Any(a =>
                                a.AttributeClass is { Name: AttributeName } attr &&
                                attr.InBTDBNamespace()))
                            && !(symbol.BaseType?.GetAttributes().Any(a =>
                                a.AttributeClass is { Name: AttributeName } attr &&
                                attr.InBTDBNamespace()) ?? false))
                        {
                            return null;
                        }

                        var isPartial = classDeclarationSyntax.Modifiers
                            .Any(m => m.ValueText == "partial");

                        return GenerationInfoForClass(symbol, classDeclarationSyntax, isPartial, null, semanticModel,
                            [], [], false, false);
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
                                attr.InBTDBNamespace()))
                            return null;
                        if (!symbol.GetAttributes().Any(a =>
                                a.AttributeClass is { Name: AttributeName } attr &&
                                attr.InBTDBNamespace())
                            && !symbol.AllInterfaces.Any(interfaceSymbol => interfaceSymbol.GetAttributes().Any(a =>
                                a.AttributeClass is { Name: AttributeName } attr &&
                                attr.InBTDBNamespace()))
                            && !(symbol.BaseType?.GetAttributes().Any(a =>
                                a.AttributeClass is { Name: AttributeName } attr &&
                                attr.InBTDBNamespace()) ?? false))
                        {
                            return null;
                        }

                        var isPartial = recordDeclarationSyntax.Modifiers
                            .Any(m => m.ValueText == "partial");

                        return GenerationInfoForClass(symbol, recordDeclarationSyntax, isPartial, null, semanticModel,
                            [], [], false, false);
                    }

                    return null!;
                }
                catch (Exception e)
                {
                    return GenerationError("BTDB0000", e.StackTrace, syntaxContext.Node.GetLocation());
                }
            }).Where(i => i != null);
        gen = gen.SelectMany((g, _) => g!.Nested.IsEmpty ? Enumerable.Repeat(g, 1) : [g, ..g.Nested])!;
        context.RegisterSourceOutput(gen.Collect(), GenerateCode!);
    }

    GenerationInfo? DetectErrorsInMethods(GenerationInfo itemGenInfo, List<IMethodSymbol> methods,
        ITypeSymbol relationType,
        SemanticModel semanticModel)
    {
        // Virtually create type from BTDB package namespace BTDB.ODBLayer type RelationDBManipulator<relationType>
        var compilation = semanticModel.Compilation;
        var (indexOfInKeyValue, primaryKeyFields, secondaryKeys) = BuildIndexInfo(itemGenInfo);

        // Get the RelationDBManipulator<> type from BTDB.ODBLayer namespace
        var relationDbManipulatorType = compilation.GetTypeByMetadataName("BTDB.ODBLayer.RelationDBManipulator`1");
        if (relationDbManipulatorType == null)
        {
            throw new Exception("BTDB.ODBLayer.RelationDBManipulator`1 type not found");
        }

        // Construct RelationDBManipulator<relationType>
        var constructedType = relationDbManipulatorType.Construct(relationType);

        // Check each method in the interface
        foreach (var method in methods)
        {
            if (method.Name.StartsWith("FindBy", StringComparison.Ordinal))
            {
                var (indexName, hasOrDefault) = StripVariant(secondaryKeys, method.Name, true);
                if (method.ReturnType is INamedTypeSymbol
                    {
                        TypeKind: TypeKind.Interface,
                        OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_IEnumerable_T
                    } enumerableReturnType)
                {
                    var itemType = enumerableReturnType.TypeArguments.FirstOrDefault();
                    if (itemType is not INamedTypeSymbol { TypeKind: TypeKind.Class })
                    {
                        return GenerationError("BTDB0042",
                            $"Return type of '{method.Name}' must be class or IEnumerable<T> where T is class",
                            method.Locations[0]);
                    }

                    if (!SerializableType(itemType))
                    {
                        return GenerationError("BTDB0043",
                            $"Return type of '{method.Name}' must use serializable class type",
                            method.Locations[0]);
                    }
                }
                else
                {
                    if (method.ReturnType.TypeKind != TypeKind.Class)
                    {
                        return GenerationError("BTDB0042",
                            $"Return type of '{method.Name}' must be class or IEnumerable<T> where T is class",
                            method.Locations[0]);
                    }

                    if (!SerializableType(method.ReturnType))
                    {
                        return GenerationError("BTDB0043",
                            $"Return type of '{method.Name}' must use serializable class type",
                            method.Locations[0]);
                    }
                }

                return CheckParamsNamesAndTypes(method, indexName, itemGenInfo.Fields, primaryKeyFields,
                    indexOfInKeyValue, secondaryKeys);
            }

            if (method.Name.StartsWith("FirstBy"))
            {
                // Validate return type is class
                if (method.ReturnType.TypeKind != TypeKind.Class)
                {
                    return GenerationError("BTDB0027",
                        $"Method '{method.Name}' must have class return type",
                        method.Locations[0]);
                }

                var (indexName, hasOrDefault) = StripVariant(secondaryKeys, method.Name, true);

                // Check if last parameter is IOrderer[]
                var lastParameterIsIOrdererArray = CheckIfLastParameterIsIOrdererArray(method);

                // Validate constraint parameters
                var constraintParamCount = method.Parameters.Length - (lastParameterIsIOrdererArray ? 1 : 0);

                if (constraintParamCount > 0)
                {
                    var validationError = ValidateFirstByConstraintParameters(method, indexName,
                        itemGenInfo.Fields, primaryKeyFields, secondaryKeys, 0, constraintParamCount);
                    if (validationError != null)
                        return validationError;
                }

                continue;
            }

            if (method.Name.StartsWith("ScanBy"))
            {
                var (indexName, hasOrDefault) = StripVariant(secondaryKeys, method.Name, false);
                // Check if return type is IEnumerable<T>
                if (!IsIEnumerableOfTWhereTIsClass(method.ReturnType))
                    return GenerationError("BTDB0019",
                        $"Return type of '{method.Name}' must be IEnumerable<T>",
                        method.Locations[0]);

                return CheckParamsNamesAndTypesScanBy(method, indexName, itemGenInfo.Fields, primaryKeyFields,
                    uint.MaxValue, secondaryKeys, false);
            }

            if (method.Name.StartsWith("GatherBy"))
            {
                // Validate return type is ulong
                if (method.ReturnType.SpecialType != SpecialType.System_UInt64)
                {
                    return GenerationError("BTDB0021",
                        $"Method '{method.Name}' must return ulong",
                        method.Locations[0]);
                }

                // Validate minimum 3 parameters
                if (method.Parameters.Length < 3)
                {
                    return GenerationError("BTDB0022",
                        $"Method '{method.Name}' expects at least 3 parameters",
                        method.Locations[0]);
                }

                // Validate first parameter is ICollection<>
                var firstParam = method.Parameters[0];
                if (firstParam.Type is not INamedTypeSymbol
                    {
                        TypeKind: TypeKind.Interface,
                        OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_ICollection_T
                    })
                {
                    return GenerationError("BTDB0023",
                        $"Method '{method.Name}' first parameter must implement ICollection<>",
                        firstParam.Locations[0]);
                }

                // Validate second parameter is named "skip" and is long
                var secondParam = method.Parameters[1];
                if (secondParam.Name != "skip" || secondParam.Type.SpecialType != SpecialType.System_Int64)
                {
                    return GenerationError("BTDB0024",
                        $"Method '{method.Name}' second parameter must be long type and named skip",
                        secondParam.Locations[0]);
                }

                // Validate third parameter is named "take" and is long
                var thirdParam = method.Parameters[2];
                if (thirdParam.Name != "take" || thirdParam.Type.SpecialType != SpecialType.System_Int64)
                {
                    return GenerationError("BTDB0025",
                        $"Method '{method.Name}' third parameter must be long type and named take",
                        thirdParam.Locations[0]);
                }

                // Extract index name and validate remaining constraint parameters
                var (indexName, _) = StripVariant(secondaryKeys, method.Name, false);

                // Check if last parameter is IOrderer[]
                var lastParameterIsIOrdererArray = CheckIfLastParameterIsIOrdererArray(method);

                // Create a slice of parameters from index 3 onwards (skip collection, skip, take)
                var constraintParamCount = method.Parameters.Length - 3 - (lastParameterIsIOrdererArray ? 1 : 0);

                // Validate the constraint parameters
                if (constraintParamCount > 0)
                {
                    var validationError = ValidateGatherByConstraintParameters(method, indexName,
                        itemGenInfo.Fields, primaryKeyFields, secondaryKeys, 3, constraintParamCount,
                        lastParameterIsIOrdererArray);
                    if (validationError != null)
                        return validationError;
                }

                continue;
            }

            if (method.Name.StartsWith("ListBy"))
            {
                var (indexName, _) = StripVariant(secondaryKeys, method.Name, false);

                if (method.ReturnType is INamedTypeSymbol
                    {
                        TypeKind: TypeKind.Interface,
                        OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_IEnumerator_T
                    })
                {
                    return GenerationError("BTDB0009",
                        $"Cannot use IEnumerator<> as return type in {method.Name}",
                        method.Locations[0]);
                }

                var hasAdvancedEnumerator =
                    CheckIfLastParameterIsAdvancedEnumeratorParam(method, out var aepGenericType);
                if (hasAdvancedEnumerator)
                {
                    if (!IsIEnumerableOfTWhereTIsClass(method.ReturnType) &&
                        (aepGenericType == null || !IsIOrderedDictionaryEnumerator(method.ReturnType, aepGenericType)))
                    {
                        return GenerationError("BTDB0033",
                            $"Return type of '{method.Name}' must be IEnumerable<T> or IOrderedDictionaryEnumerator<,>",
                            method.Locations[0]);
                    }

                    return CheckParamsNamesAndTypes(method, indexName, itemGenInfo.Fields, primaryKeyFields,
                        indexOfInKeyValue, secondaryKeys, true, true);
                }

                if (!IsIEnumerableOfTWhereTIsClass(method.ReturnType))
                {
                    return GenerationError("BTDB0033",
                        $"Return type of '{method.Name}' must be IEnumerable<T>",
                        method.Locations[0]);
                }

                return CheckParamsNamesAndTypes(method, indexName, itemGenInfo.Fields, primaryKeyFields,
                    indexOfInKeyValue, secondaryKeys, true);
            }

            if (method.Name.StartsWith("AnyBy"))
            {
                // Validate return type is bool
                if (method.ReturnType.SpecialType != SpecialType.System_Boolean)
                {
                    return GenerationError("BTDB0029",
                        $"Method '{method.Name}' must return bool",
                        method.Locations[0]);
                }

                var (indexName, _) = StripVariant(secondaryKeys, method.Name, false);

                return CheckParamsNamesAndTypes(method, indexName, itemGenInfo.Fields, primaryKeyFields,
                    indexOfInKeyValue, secondaryKeys, true, true);
            }

            if (method.Name.StartsWith("CountBy"))
            {
                // Validate return type is long-like (int, uint, long, ulong)
                if (method.ReturnType.SpecialType != SpecialType.System_Int32 &&
                    method.ReturnType.SpecialType != SpecialType.System_UInt32 &&
                    method.ReturnType.SpecialType != SpecialType.System_Int64 &&
                    method.ReturnType.SpecialType != SpecialType.System_UInt64)
                {
                    return GenerationError("BTDB0030",
                        $"Method '{method.Name}' must return int, uint, long, or ulong",
                        method.Locations[0]);
                }

                var (indexName, _) = StripVariant(secondaryKeys, method.Name, false);

                return CheckParamsNamesAndTypes(method, indexName, itemGenInfo.Fields, primaryKeyFields,
                    indexOfInKeyValue, secondaryKeys, true, true);
            }

            if (method.Name.StartsWith("UpdateById", StringComparison.Ordinal))
            {
                if (method.ReturnType.SpecialType != SpecialType.System_Void &&
                    method.ReturnType.SpecialType != SpecialType.System_Boolean)
                {
                    return GenerationError("BTDB0039",
                        $"Method '{method.Name}' must return void or bool",
                        method.Locations[0]);
                }

                if (method.Parameters.Length < primaryKeyFields.Length)
                {
                    return GenerationError("BTDB0040",
                        $"Not enough parameters in {method.Name} (expected at least {primaryKeyFields.Length}).",
                        method.Locations[0]);
                }

                var duplicateParamError = ValidateUpdateByIdValueParameters(method, itemGenInfo.Fields,
                    primaryKeyFields);
                if (duplicateParamError != null)
                    return duplicateParamError;

                return CheckParamsNamesAndTypes(method, "Id", itemGenInfo.Fields, primaryKeyFields,
                    indexOfInKeyValue, secondaryKeys, true);
            }

            if (method.Name.StartsWith("RemoveBy", StringComparison.Ordinal))
            {
                var (indexName, hasVariant) = StripVariant(secondaryKeys, method.Name, false);

                // Check if method has special suffix like "Partial"
                var methodSuffix = method.Name.Substring(("RemoveBy" + indexName).Length);
                var hasSpecialSuffix = methodSuffix.StartsWith("Partial", StringComparison.Ordinal) ||
                                       methodSuffix.StartsWith("OrDefault", StringComparison.Ordinal);

                // Skip validation for special variants like "Partial", "OrDefault"
                if (hasVariant || hasSpecialSuffix)
                    continue;

                if (indexName != "Id")
                {
                    return GenerationError("BTDB0038",
                        $"Remove by secondary key in {itemGenInfo.Name}.{method.Name} is unsupported. Instead use ListBy and remove enumerated.",
                        method.Locations[0]);
                }

                // Validate return type (void, bool, int, uint, long, ulong)
                if (method.ReturnType.SpecialType != SpecialType.System_Void &&
                    method.ReturnType.SpecialType != SpecialType.System_Boolean &&
                    method.ReturnType.SpecialType != SpecialType.System_Int32 &&
                    method.ReturnType.SpecialType != SpecialType.System_UInt32 &&
                    method.ReturnType.SpecialType != SpecialType.System_Int64 &&
                    method.ReturnType.SpecialType != SpecialType.System_UInt64)
                {
                    return GenerationError("BTDB0032",
                        $"Method '{method.Name}' must return void, bool, int, uint, long, or ulong",
                        method.Locations[0]);
                }

                // Validate parameters normally if no AdvancedEnumeratorParam (skip parameter count check to allow prefix matching)
                return CheckParamsNamesAndTypes(method, indexName, itemGenInfo.Fields, primaryKeyFields,
                    indexOfInKeyValue, secondaryKeys, true, true);
            }

            if (method.Name == "RemoveWithSizesById")
            {
                if (method.ReturnType is not INamedTypeSymbol
                    {
                        IsTupleType: true,
                        TupleElements.Length: 3
                    } tupleType ||
                    tupleType.TupleElements.Any(e => e.Type.SpecialType != SpecialType.System_UInt64))
                {
                    return GenerationError("BTDB0036",
                        $"Method '{method.Name}' must return (ulong Count, ulong KeySizes, ulong ValueSizes)",
                        method.Locations[0]);
                }

                return ValidateFirstByConstraintParameters(method, "Id", itemGenInfo.Fields, primaryKeyFields,
                    secondaryKeys, 0, method.Parameters.Length);
            }

            if (method.Name == "ShallowUpsertWithSizes")
            {
                if (secondaryKeys.Length > 0)
                {
                    return GenerationError("BTDB0037",
                        $"Method '{method.Name}' cannot be used with relation with secondary indexes",
                        method.Locations[0]);
                }
            }

            if (method.Name == "Contains")
            {
                if (method.ReturnType.SpecialType != SpecialType.System_Boolean)
                {
                    return GenerationError("BTDB0034",
                        $"Method '{method.Name}' must return bool",
                        method.Locations[0]);
                }

                return CheckParamsNamesAndTypes(method, "Id", itemGenInfo.Fields, primaryKeyFields,
                    indexOfInKeyValue, secondaryKeys);
            }

            if (method.Name.StartsWith("ShallowRemoveBy", StringComparison.Ordinal) ||
                method.Name.StartsWith("Contains", StringComparison.Ordinal))
                continue;
            // Find a matching method in RelationDBManipulator<relationType>
            var baseMethod = constructedType.GetMembers(method.Name)
                .OfType<IMethodSymbol>()
                .FirstOrDefault(m => m.MethodKind == MethodKind.Ordinary);

            if (baseMethod == null)
            {
                return GenerationError("BTDB0041", $"Method {method.Name} is not supported.", method.Locations[0]);
            }

            // Check if parameter count matches
            if (method.Parameters.Length != baseMethod.Parameters.Length)
            {
                return GenerationError("BTDB0010",
                    $"Method '{method.Name}' has {method.Parameters.Length} parameter(s) but the base method in RelationDBManipulator<{relationType.Name}> requires {baseMethod.Parameters.Length} parameter(s)",
                    method.Locations[0]);
            }

            // Check if the return type matches or is void
            if (!SymbolEqualityComparer.Default.Equals(method.ReturnType, baseMethod.ReturnType) &&
                (method.Name != "Insert" ||
                 method.ReturnType.SpecialType != SpecialType.System_Void))
            {
                return GenerationError("BTDB0012",
                    $"Method '{method.Name}' has return type '{method.ReturnType.ToDisplayString()}' but method requires '{baseMethod.ReturnType.ToDisplayString()}'",
                    method.Locations[0]);
            }

            // Check if parameter types match
            for (var i = 0; i < method.Parameters.Length; i++)
            {
                var interfaceParam = method.Parameters[i];
                var baseParam = baseMethod.Parameters[i];

                if (!SymbolEqualityComparer.Default.Equals(interfaceParam.Type, baseParam.Type))
                {
                    return GenerationError("BTDB0011",
                        $"Method '{method.Name}' parameter {i + 1} has type '{interfaceParam.Type.ToDisplayString()}' but the base method requires '{baseParam.Type.ToDisplayString()}'",
                        method.Locations[0]);
                }
            }
        }

        return null;
    }

    static bool IsIEnumerableOfTWhereTIsClass(ITypeSymbol methodReturnType)
    {
        if (methodReturnType is INamedTypeSymbol
            {
                TypeKind: TypeKind.Interface,
                OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_IEnumerable_T
            } methodReturnTypeNamedTypeSymbol)
        {
            return methodReturnTypeNamedTypeSymbol.TypeArguments.FirstOrDefault()?.TypeKind == TypeKind.Class;
        }

        return false;
    }

    static bool IsIOrderedDictionaryEnumerator(ITypeSymbol methodReturnType, ITypeSymbol keyType)
    {
        if (methodReturnType is INamedTypeSymbol
            {
                TypeKind: TypeKind.Interface,
                OriginalDefinition.Name: "IOrderedDictionaryEnumerator"
            } named &&
            named.OriginalDefinition.InODBLayerNamespace() &&
            named.TypeArguments.Length == 2)
        {
            return SymbolEqualityComparer.Default.Equals(named.TypeArguments[0], keyType);
        }

        return false;
    }

    GenerationInfo? CheckParamsNamesAndTypesScanBy(IMethodSymbol method, string indexName,
        EquatableArray<FieldsInfo> fields, uint[] primaryKeyFields, uint inKeyValueIndex,
        (string Name, uint[] SecondaryKeyFields, uint ExplicitPrefixLength)[] secondaryKeys,
        bool lastParameterIsIOrdererArray)
    {
        if (ValidateIndexName(method, indexName, primaryKeyFields, inKeyValueIndex, secondaryKeys, out var fi,
                out var generationError))
            return generationError;
        var paramCount = method.Parameters.Length - (lastParameterIsIOrdererArray ? 1 : 0);
        if (paramCount > fi.Length)
        {
            return GenerationError("BTDB0016",
                $"Too many parameters for index '{indexName}'",
                method.Locations[0]);
        }

        return ValidateConstraintParameters(method, indexName, fields, fi, 0, paramCount);
    }

    static bool AreTypesCompatible(string pType, string fType)
    {
        if (pType == fType) return true;

        var normalizedParamType = RemoveGlobalPrefix(pType);
        var normalizedFieldType = RemoveGlobalPrefix(fType);
        if (normalizedParamType == normalizedFieldType) return true;
        if (IsStringEncryptedStringPair(normalizedParamType, normalizedFieldType)) return true;

        if (IsUnsignedIntegralType(normalizedParamType) && IsUnsignedIntegralType(normalizedFieldType)) return true;
        if (IsSignedIntegralType(normalizedParamType) && IsSignedIntegralType(normalizedFieldType)) return true;

        return false;
    }

    static string RemoveGlobalPrefix(string type)
    {
        if (type.StartsWith("global::", StringComparison.Ordinal))
        {
            type = type.Substring("global::".Length);
        }

        return type;
    }

    static bool IsStringEncryptedStringPair(string paramType, string fieldType)
    {
        const string stringType = "System.String";
        const string encryptedStringType = "BTDB.Encrypted.EncryptedString";
        return (paramType == stringType && fieldType == encryptedStringType) ||
               (paramType == encryptedStringType && fieldType == stringType);
    }

    static bool IsUnsignedIntegralType(string type)
    {
        return NormalizeIntegralType(type) is IntegralType.Byte or IntegralType.UInt16 or IntegralType.UInt32
            or IntegralType.UInt64;
    }

    static bool IsSignedIntegralType(string type)
    {
        return NormalizeIntegralType(type) is IntegralType.SByte or IntegralType.Int16 or IntegralType.Int32
            or IntegralType.Int64;
    }

    static IntegralType NormalizeIntegralType(string type)
    {
        if (type.StartsWith("global::System.", StringComparison.Ordinal))
        {
            type = type.Substring("global::System.".Length);
        }
        else if (type.StartsWith("System.", StringComparison.Ordinal))
        {
            type = type.Substring("System.".Length);
        }

        return type switch
        {
            "byte" or "Byte" => IntegralType.Byte,
            "ushort" or "UInt16" => IntegralType.UInt16,
            "uint" or "UInt32" => IntegralType.UInt32,
            "ulong" or "UInt64" => IntegralType.UInt64,
            "sbyte" or "SByte" => IntegralType.SByte,
            "short" or "Int16" => IntegralType.Int16,
            "int" or "Int32" => IntegralType.Int32,
            "long" or "Int64" => IntegralType.Int64,
            _ => IntegralType.None
        };
    }

    enum IntegralType
    {
        None = 0,
        Byte,
        UInt16,
        UInt32,
        UInt64,
        SByte,
        Int16,
        Int32,
        Int64
    }

    static bool CheckIfLastParameterIsIOrdererArray(IMethodSymbol method)
    {
        return method.Parameters.Length > 0 &&
               method.Parameters[method.Parameters.Length - 1].Type is IArrayTypeSymbol arrayType &&
               arrayType.ElementType.InODBLayerNamespace() && arrayType.ElementType.Name == "IOrderer";
    }

    static bool CheckIfLastParameterIsAdvancedEnumeratorParam(IMethodSymbol method, out ITypeSymbol? genericTypeArg)
    {
        genericTypeArg = null;
        if (method.Parameters.Length == 0) return false;

        var lastParam = method.Parameters[method.Parameters.Length - 1];
        if (lastParam.Type is INamedTypeSymbol namedType &&
            namedType.InODBLayerNamespace() &&
            namedType.Name == "AdvancedEnumeratorParam" &&
            namedType is { IsGenericType: true, TypeArguments.Length: 1 })
        {
            genericTypeArg = namedType.TypeArguments[0];
            return true;
        }

        return false;
    }

    static GenerationInfo? ValidateConstraintParameters(IMethodSymbol method, string indexName,
        EquatableArray<FieldsInfo> fields, ReadOnlySpan<uint> fieldIndexes, int startParamIndex, int paramCount)
    {
        for (var i = 0; i < fieldIndexes.Length; i++)
        {
            if (i >= paramCount) break;
            var paramIndex = startParamIndex + i;
            var param = method.Parameters[paramIndex];
            var f = fields[(int)fieldIndexes[i]];

            // Check parameter name matches field name
            if (!param.Name.Equals(f.Name, StringComparison.OrdinalIgnoreCase))
            {
                return GenerationError("BTDB0014",
                    $"Parameter '{param.Name}' does not match field '{f.Name}' from index '{indexName}' in method '{method.Name}'",
                    param.Locations[0]);
            }

            // Validate parameter is Constraint<T> type
            if (param.Type is not INamedTypeSymbol paramTypeSymbol ||
                !paramTypeSymbol.InODBLayerNamespace() ||
                paramTypeSymbol.Name != "Constraint" ||
                !paramTypeSymbol.IsGenericType ||
                paramTypeSymbol.TypeArguments.Length != 1)
            {
                return GenerationError("BTDB0017",
                    $"Parameter '{param.Name}' in method '{method.Name}' must be of type Constraint<T>",
                    param.Locations[0]);
            }

            var constraintGenericType = paramTypeSymbol.TypeArguments[0];
            var constraintGenericTypeStr =
                constraintGenericType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

            // Check if the constraint generic type matches the field type
            if (!AreTypesCompatible(constraintGenericTypeStr, f.Type))
            {
                return GenerationError("BTDB0018",
                    $"Parameter constraint type mismatch in method '{method.Name}' for parameter '{param.Name}' (expected '{f.Type}' but '{constraintGenericTypeStr}' found)",
                    param.Locations[0]);
            }
        }

        return null;
    }

    static GenerationInfo? ValidateGatherByConstraintParameters(IMethodSymbol method, string indexName,
        EquatableArray<FieldsInfo> fields, uint[] primaryKeyFields,
        (string Name, uint[] SecondaryKeyFields, uint ExplicitPrefixLength)[] secondaryKeys, int startParamIndex,
        int constraintParamCount, bool lastParameterIsIOrdererArray)
    {
        // Validate index name
        if (ValidateIndexName(method, indexName, primaryKeyFields, uint.MaxValue, secondaryKeys, out var fi,
                out var generationError))
            return generationError;

        // Validate constraint parameter count doesn't exceed index field count
        if (constraintParamCount > fi.Length)
        {
            return GenerationError("BTDB0026",
                $"Too many constraint parameters for index '{indexName}' in method '{method.Name}'",
                method.Locations[0]);
        }

        return ValidateConstraintParameters(method, indexName, fields, fi, startParamIndex, constraintParamCount);
    }

    static GenerationInfo? ValidateFirstByConstraintParameters(IMethodSymbol method, string indexName,
        EquatableArray<FieldsInfo> fields, uint[] primaryKeyFields,
        (string Name, uint[] SecondaryKeyFields, uint ExplicitPrefixLength)[] secondaryKeys, int startParamIndex,
        int constraintParamCount)
    {
        // Validate index name
        if (ValidateIndexName(method, indexName, primaryKeyFields, uint.MaxValue, secondaryKeys, out var fi,
                out var generationError))
            return generationError;

        // Validate constraint parameter count doesn't exceed index field count
        if (constraintParamCount > fi.Length)
        {
            return GenerationError("BTDB0028",
                $"Too many constraint parameters for index '{indexName}' in method '{method.Name}'",
                method.Locations[0]);
        }

        return ValidateConstraintParameters(method, indexName, fields, fi, startParamIndex, constraintParamCount);
    }

    static GenerationInfo? ValidateUpdateByIdValueParameters(IMethodSymbol method, EquatableArray<FieldsInfo> fields,
        uint[] primaryKeyFields)
    {
        var pkParamCount = primaryKeyFields.Length;
        if (method.Parameters.Length <= pkParamCount) return null;

        var pkFieldIndexes = new HashSet<uint>(primaryKeyFields);
        var nonPkFields = new List<FieldsInfo>();
        for (var fieldIndex = 0; fieldIndex < fields.Count; fieldIndex++)
        {
            if (pkFieldIndexes.Contains((uint)fieldIndex)) continue;
            var field = fields[fieldIndex];
            if (field.ReadOnly) continue;
            nonPkFields.Add(field);
        }

        for (var paramIndex = pkParamCount; paramIndex < method.Parameters.Length; paramIndex++)
        {
            var param = method.Parameters[paramIndex];
            var matches = 0;
            FieldsInfo? matchedField = null;
            for (var fieldIndex = 0; fieldIndex < nonPkFields.Count; fieldIndex++)
            {
                var field = nonPkFields[fieldIndex];
                if (string.Equals(field.Name, param.Name, StringComparison.OrdinalIgnoreCase))
                {
                    matches++;
                    matchedField = field;
                    if (matches > 1)
                    {
                        return GenerationError("BTDB0044",
                            $"Method {method.Name} matched parameter {param.Name} more than once.",
                            param.Locations[0]);
                    }
                }
            }

            if (matches == 0)
            {
                return GenerationError("BTDB0045",
                    $"Method {method.Name} parameter {param.Name} does not match any relation fields.",
                    param.Locations[0]);
            }

            var paramType = param.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            if (IsForbiddenUpdateByIdValueParameterType(param.Type))
            {
                return GenerationError("BTDB0047",
                    $"Method {method.Name} parameter {param.Name} type '{paramType}' is not allowed for UpdateById values.",
                    param.Locations[0]);
            }

            if (!AreTypesCompatible(paramType, matchedField!.Type))
            {
                return GenerationError("BTDB0046",
                    $"Method {method.Name} parameter {param.Name} type '{paramType}' does not match field '{matchedField.Name}' type '{matchedField.Type}'.",
                    param.Locations[0]);
            }
        }

        return null;
    }

    static bool IsForbiddenUpdateByIdValueParameterType(ITypeSymbol typeSymbol)
    {
        if (IsListFieldHandlerCompatible(typeSymbol) || IsDictionaryFieldHandlerCompatible(typeSymbol))
        {
            return true;
        }

        if (typeSymbol is not INamedTypeSymbol namedTypeSymbol)
        {
            return typeSymbol.TypeKind == TypeKind.Class;
        }

        if (namedTypeSymbol.SpecialType == SpecialType.System_String) return false;
        if (namedTypeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) ==
            "global::BTDB.Encrypted.EncryptedString")
        {
            return false;
        }

        return typeSymbol.TypeKind == TypeKind.Class;
    }

    static bool IsListFieldHandlerCompatible(ITypeSymbol typeSymbol)
    {
        if (typeSymbol is not INamedTypeSymbol namedTypeSymbol || !namedTypeSymbol.IsGenericType)
        {
            return false;
        }

        if (IsListOrSetType(namedTypeSymbol.ConstructedFrom)) return true;

        return namedTypeSymbol.AllInterfaces.Any(static iface =>
            iface.IsGenericType && IsListOrSetType(iface.ConstructedFrom));
    }

    static bool IsListOrSetType(INamedTypeSymbol typeSymbol)
    {
        var typeName = typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        return typeName is "global::System.Collections.Generic.IList<T>"
            or "global::System.Collections.Generic.ISet<T>";
    }

    static bool IsDictionaryFieldHandlerCompatible(ITypeSymbol typeSymbol)
    {
        if (typeSymbol is not INamedTypeSymbol { IsGenericType: true } namedTypeSymbol)
        {
            return false;
        }

        var constructedType = namedTypeSymbol.ConstructedFrom.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        return constructedType is "global::System.Collections.Generic.IDictionary<TKey, TValue>"
            or "global::System.Collections.Generic.Dictionary<TKey, TValue>";
    }

    static GenerationInfo? CheckParamsNamesAndTypes(IMethodSymbol method, string indexName,
        EquatableArray<FieldsInfo> fields,
        uint[] primaryKeyFields, uint inKeyValueIndex,
        (string Name, uint[] SecondaryKeyFields, uint ExplicitPrefixLength)[] secondaryKeys,
        bool skipNumberOfParametersCheck = false,
        bool allowAdvancedEnumeratorParam = false)
    {
        if (ValidateIndexName(method, indexName, primaryKeyFields, inKeyValueIndex, secondaryKeys, out var fi,
                out var generationError))
            return generationError;

        ITypeSymbol? aepGenericType = null;
        var hasAdvancedEnumeratorParam = allowAdvancedEnumeratorParam &&
                                         CheckIfLastParameterIsAdvancedEnumeratorParam(method, out aepGenericType);
        var paramCount = method.Parameters.Length - (hasAdvancedEnumeratorParam ? 1 : 0);

        // For non-prefix-based queries (returns single item, not IEnumerable), validate parameter count matches key count
        var isPrefixBased = IsIEnumerableOfTWhereTIsClass(method.ReturnType);
        if (!skipNumberOfParametersCheck && indexName == "Id" && !isPrefixBased &&
            paramCount != fi.Length)
        {
            return GenerationError("BTDB0020",
                $"Number of parameters in '{method.Name}' does not match {indexName} key count {fi.Length}.",
                method.Locations[0]);
        }

        if (hasAdvancedEnumeratorParam)
        {
            if (isPrefixBased)
            {
                if (paramCount >= fi.Length)
                {
                    return GenerationError("BTDB0016",
                        $"Too many parameters for index '{indexName}' in method '{method.Name}'",
                        method.Locations[0]);
                }
            }

            var nextFieldIndex = fi[paramCount];
            var nextField = fields[(int)nextFieldIndex];
            var aepTypeStr = aepGenericType!.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            if (!AreTypesCompatible(aepTypeStr, nextField.Type))
            {
                return GenerationError("BTDB0031",
                    $"AdvancedEnumeratorParam generic type '{aepTypeStr}' does not match field '{nextField.Name}' type '{nextField.Type}' in method '{method.Name}'",
                    method.Parameters[method.Parameters.Length - 1].Locations[0]);
            }
        }

        for (var i = 0; i < fi.Length; i++)
        {
            if (i >= paramCount) break;
            var param = method.Parameters[i];
            var f = fields[(int)fi[i]];
            if (!param.Name.Equals(f.Name, StringComparison.OrdinalIgnoreCase))
            {
                return GenerationError("BTDB0014",
                    $"Parameter '{param.Name}' does not match field '{f.Name}' from index '{indexName}'",
                    param.Locations[0]);
            }

            if (param.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) != f.Type)
            {
                return GenerationError("BTDB0015",
                    $"Parameter '{param.Name}' type '{param.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)}' does not match field '{f.Name}' type '{f.Type}' from index '{indexName}'",
                    param.Locations[0]);
            }
        }

        return null;
    }

    static bool ValidateIndexName(IMethodSymbol method, string indexName, uint[] primaryKeyFields, uint inKeyValueIndex,
        (string Name, uint[] SecondaryKeyFields, uint ExplicitPrefixLength)[] secondaryKeys, out ReadOnlySpan<uint> fi,
        out GenerationInfo? generationError)
    {
        uint[]? fii = null;
        uint explicitPrefixLength = 0;
        if (indexName == "Id")
        {
            fii = primaryKeyFields;
        }
        else
        {
            var skInfo = secondaryKeys.FirstOrDefault(sk => sk.Name == indexName);
            if (!string.IsNullOrEmpty(skInfo.Name))
            {
                fii = skInfo.SecondaryKeyFields;
                explicitPrefixLength = skInfo.ExplicitPrefixLength;
            }
        }

        if (fii == null)
        {
            generationError = GenerationError("BTDB0013",
                $"Cannot find index '{indexName}' defined sks: {string.Join(", ", secondaryKeys.Select(sk => sk.Name))}",
                method.Locations[0]);
            fi = default;
            return true;
        }

        generationError = null;
        fi = fii;
        if (inKeyValueIndex != uint.MaxValue) // Constraint-based methods are allowed to use all fields from indexes
        {
            if (fii == primaryKeyFields)
            {
                fi = fi.Slice(0, (int)inKeyValueIndex);
            }
            else if (indexName != "Id" && explicitPrefixLength > 0)
            {
                fi = fi.Slice(0, (int)explicitPrefixLength);
            }
        }

        return false;
    }

    static (string IndexName, bool HasOrDefault) StripVariant(
        (string Name, uint[] SecondaryKeyFields, uint ExplicitPrefixLength)[] skIndexes,
        string name, bool withOrDefault)
    {
        (string IndexName, bool HasOrDefault) result = ("", false);

        name = name.Substring(name.IndexOf("By", StringComparison.Ordinal) + 2);

        void Check(string id)
        {
            if (!name.StartsWith(id)) return;
            if (withOrDefault)
            {
                if (name.AsSpan(id.Length).StartsWith("OrDefault"))
                {
                    if (result.IndexName.Length < id.Length)
                    {
                        result = (id, true);
                        return;
                    }
                }
            }

            if (result.IndexName.Length < id.Length)
            {
                result = (id, false);
            }
        }

        Check("Id");
        foreach (var secondaryKeyName in skIndexes.Select(s =>
                     s.Name))
        {
            Check(secondaryKeyName);
        }

        return result.IndexName.Length == 0 ? (name, false) : result;
    }

    static bool IsInterfaceMethodWithImplementation(IMethodSymbol method)
    {
        if (method.ContainingType.TypeKind != TypeKind.Interface)
        {
            return false;
        }

        if (method.IsAbstract)
        {
            return false;
        }

        foreach (var syntaxRef in method.DeclaringSyntaxReferences)
        {
            if (syntaxRef.GetSyntax() is MethodDeclarationSyntax methodSyntax &&
                (methodSyntax.Body != null || methodSyntax.ExpressionBody != null))
            {
                return true;
            }
        }

        return !method.IsAbstract;
    }

    /// <summary>
    /// Returns null if the return type is void, otherwise returns the fully qualified type name.
    /// </summary>
    static string? IfVoidNull(ITypeSymbol methodReturnType)
    {
        if (methodReturnType.SpecialType == SpecialType.System_Void) return null;
        return methodReturnType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
    }

    static string? DetectDependencyName(ISymbol parameterSymbol)
    {
        var attr = parameterSymbol.GetAttributes()
                .FirstOrDefault(a =>
                    (a.AttributeClass?.Name == "FromKeyedServicesAttribute" &&
                     (a.AttributeClass?.InNamespace("Microsoft", "Extensions", "DependencyInjection") ?? false)) ||
                    (a.AttributeClass?.Name == "DependencyAttribute" &&
                     (a.AttributeClass?.InBTDBIOCNamespace() ?? false)))
            ;
        if (attr != null)
        {
            var val = GetStringArgForAttributeData(attr);
            if (val is not null) return SymbolDisplay.FormatLiteral(val, quote: true);
        }

        return null;
    }

    GenerationInfo? DetectErrors(ReadOnlySpan<FieldsInfo> fields, Location location)
    {
        uint? orderOfLastPrimaryKey = null;
        uint? orderOfFirstInKeyValue = null;
        var pkOrder2Index = new Dictionary<uint, int>();
        for (var i = 0; i < fields.Length; i++)
        {
            var f = fields[i];
            var indexes = f.Indexes.GetArray() ?? [];
            if (indexes.Length > 0 && IsFloatOrDoubleType(f.Type))
            {
                return GenerationError("BTDB0035",
                    $"Field '{f.Name}' type '{f.Type}' cannot be used in indexes", location);
            }

            if (indexes.Any(ii => ii.Name == null && ii.IncludePrimaryKeyOrder == 1))
            {
                return GenerationError("BTDB0002", "Cannot use PrimaryKey together with InKeyValue in " + f.Name,
                    location);
            }

            if (indexes.Any(ii => ii.Name == "Id"))
            {
                return GenerationError("BTDB0003", "Cannot use Id as name of secondary key in " + f.Name, location);
            }

            if (indexes.Any(ii => ii.InKeyValue) && indexes.Any(ii => ii.Name != null))
            {
                return GenerationError("BTDB0004",
                    "Cannot use InKeyValue cannot be part of any SecondaryKey in " + f.Name, location);
            }

            if (indexes.FirstOrDefault(ii => ii.Name == null) is { } pk)
            {
                if (pkOrder2Index.ContainsKey(pk.Order))
                {
                    return GenerationError("BTDB0005",
                        "Cannot have multiple PrimaryKey with same order in " + f.Name + " as in " +
                        fields[pkOrder2Index[pk.Order]].Name, location);
                }

                pkOrder2Index[pk.Order] = i;
                if (pk.InKeyValue)
                {
                    if (orderOfFirstInKeyValue == null)
                    {
                        orderOfFirstInKeyValue = pk.Order;
                    }
                    else if (orderOfFirstInKeyValue > pk.Order)
                    {
                        orderOfFirstInKeyValue = pk.Order;
                    }
                }
                else
                {
                    if (orderOfLastPrimaryKey == null)
                    {
                        orderOfLastPrimaryKey = pk.Order;
                    }
                    else if (orderOfLastPrimaryKey < pk.Order)
                    {
                        orderOfLastPrimaryKey = pk.Order;
                    }
                }
            }
        }

        if (orderOfLastPrimaryKey > orderOfFirstInKeyValue)
        {
            return GenerationError("BTDB0006",
                "InKeyValue " + fields[pkOrder2Index[orderOfFirstInKeyValue.Value]].Name +
                " must be in order after PrimaryKey " + fields[pkOrder2Index[orderOfLastPrimaryKey.Value]].Name,
                location);
        }

        var skName2Order2Index = new Dictionary<string, Dictionary<uint, int>>();

        for (var i = 0; i < fields.Length; i++)
        {
            var f = fields[i];
            var indexes = f.Indexes.GetArray() ?? [];
            foreach (var index in indexes)
            {
                if (index.Name == null) continue;
                if (!skName2Order2Index.TryGetValue(index.Name, out var order2Index))
                {
                    order2Index = new();
                    skName2Order2Index[index.Name] = order2Index;
                }

                for (var j = 1u; j <= index.IncludePrimaryKeyOrder; j++)
                {
                    if (pkOrder2Index.TryGetValue(j, out var pkIndex))
                    {
                        if (order2Index.TryGetValue(j, out var oldIndex))
                        {
                            return GenerationError("BTDB0007",
                                "Cannot have multiple SecondaryKey with same order in " + f.Name + " as in " +
                                fields[oldIndex].Name, location);
                        }

                        order2Index[j] = pkIndex;
                    }
                }

                if (order2Index.ContainsKey(index.Order))
                {
                    return GenerationError("BTDB0008",
                        "Cannot have multiple SecondaryKey with same order in " + f.Name + " as in " +
                        fields[order2Index[index.Order]].Name, location);
                }

                order2Index[index.Order] = i;
            }
        }

        return null;
    }

    static GenerationInfo GenerationError(string code, string message, Location location)
    {
        return new(GenerationType.Error, null, code, message, null, false, false, false, false, false, [],
            [], [], [], [], [], [], [], [], [], location);
    }

    static bool IsICovariantRelation(INamedTypeSymbol typeSymbol)
    {
        // Check if the type is BTDB.ODBLayer.ICovariantRelation<?>
        return typeSymbol is
                   { TypeKind: TypeKind.Interface, TypeArguments.Length: 1, Name: CovariantRelationInterfaceName } &&
               typeSymbol.InODBLayerNamespace();
    }

    static GenerationInfo? GenerationInfoForClass(INamedTypeSymbol symbol,
        TypeDeclarationSyntax? classDeclarationSyntax,
        bool isPartial, INamedTypeSymbol[]? constructorParameters, SemanticModel model,
        HashSet<CollectionInfo> collections, HashSet<GenerationInfo> nested, bool forceMetadata, bool isRelationItem,
        HashSet<string>? processed = null)
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
            processed =
            [
                symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)
            ];
        }
        else
        {
            if (!processed.Add(symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)))
            {
                return null;
            }
        }

        var persistedName = ExtractPersistedName(symbol);

        var implements = symbol.AllInterfaces.Where(s => s.DeclaredAccessibility == Accessibility.Public)
            .Select(s => new TypeRef(s)).ToArray();

        var dispatchers = ImmutableArray.CreateBuilder<DispatcherInfo>();
        foreach (var (name, _, _, ifaceName) in symbol.AllInterfaces.SelectMany(
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
                DetectDependencyName(p),
                p.Type.IsReferenceType,
                p.IsOptional || p.NullableAnnotation == NullableAnnotation.Annotated,
                p.HasExplicitDefaultValue
                    ? CSharpSyntaxUtilities.FormatLiteral(p.ExplicitDefaultValue,
                        new(p.Type))
                    : null,
                GetEnumUnderlyingType(p.Type)))
            .ToArray();

        if (hasDefaultConstructor && parameters == null)
        {
            parameters = [];
        }

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
            .Where(p => HasDependencyAttribute(p) &&
                        p.SetMethod is not null)
            .Select(p =>
            {
                var isComplex = p.SetMethod!.DeclaredAccessibility == Accessibility.Private ||
                                p.SetMethod.IsInitOnly;
                // if Set/init method have default implementation => then it could directly set backing field
                var isFieldBased = isComplex && IsDefaultMethodImpl(p.SetMethod.DeclaringSyntaxReferences);
                return new PropertyInfo(p.Name,
                    p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    DetectDependencyName(p),
                    p.Type.IsReferenceType,
                    p.NullableAnnotation == NullableAnnotation.Annotated, isComplex, isFieldBased,
                    isComplex ? isFieldBased ? $"<{p.Name}>k__BackingField" : p.SetMethod.Name : null);
            }).Concat(
                symbol.GetMembers()
                    .OfType<IFieldSymbol>()
                    .Where(p => !p.IsStatic)
                    .Where(HasDependencyAttribute)
                    .Select(p =>
                    {
                        return new PropertyInfo(p.Name,
                            p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                            DetectDependencyName(p),
                            p.Type.IsReferenceType,
                            p.NullableAnnotation == NullableAnnotation.Annotated,
                            p.DeclaredAccessibility != Accessibility.Public, true, p.Name);
                    }))
            .ToArray();
        var fields = GetAllMembersIncludingBase(symbol)
            .OfType<IFieldSymbol>()
            .Where(f => !f.IsStatic && SerializableType(f.Type))
            .Where(f =>
                f.DeclaredAccessibility == Accessibility.Public &&
                !HasDependencyAttribute(f) &&
                f.GetAttributes().All(a => a.AttributeClass?.Name != "NotStoredAttribute")
                || f.GetAttributes().Any(a => a.AttributeClass?.Name == "PersistedNameAttribute"))
            .Select(f => new FieldsInfo(f.Name,
                f.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), GenericTypeFrom(f.Type),
                ExtractPersistedName(f),
                f.Type.IsReferenceType, f.Name, null, null, false,
                f.ContainingType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                ExtractIndexInfo(f.GetAllAttributes())))
            .Concat(GetAllMembersIncludingBase(symbol)
                .OfType<IPropertySymbol>()
                .Where(p => !p.IsStatic && SerializableType(p.Type))
                .Where(p =>
                    !HasDependencyAttribute(p) &&
                    p.GetAttributes().All(a => a.AttributeClass?.Name != "NotStoredAttribute") &&
                    p.GetMethod is not null && (p.SetMethod is not null ||
                                                p.GetAttributes().Any(a =>
                                                    a.AttributeClass?.Name == "SecondaryKeyAttribute")))
                .Select(p =>
                {
                    var isReadOnly = p.SetMethod is null;
                    var getterName = !IsDefaultMethodImpl(p.GetMethod!.DeclaringSyntaxReferences)
                        ? p.GetMethod.Name
                        : null;
                    var setterName = isReadOnly
                        ? ""
                        : !IsDefaultMethodImpl(p.SetMethod!.DeclaringSyntaxReferences)
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

                    if (isReadOnly) setterName = null;

                    return new FieldsInfo(p.Name,
                        p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                        GenericTypeFrom(p.OriginalDefinition.Type),
                        ExtractPersistedName(p),
                        p.Type.IsReferenceType,
                        backingName, getterName, setterName, isReadOnly,
                        p.ContainingType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                        ExtractIndexInfo(p.GetAllAttributes()));
                })).ToArray();

        var fieldTypes = symbol.GetMembers()
            .OfType<IFieldSymbol>()
            .Where(f => !f.IsStatic)
            .Where(f =>
                f.DeclaredAccessibility == Accessibility.Public &&
                !HasDependencyAttribute(f) &&
                f.GetAttributes().All(a => a.AttributeClass?.Name != "NotStoredAttribute")
                || f.GetAttributes().Any(a => a.AttributeClass?.Name == "PersistedNameAttribute"))
            .Select(f => f.Type)
            .Concat(symbol.GetMembers()
                .OfType<IPropertySymbol>()
                .Where(p => !p.IsStatic)
                .Where(p =>
                    !HasDependencyAttribute(p) &&
                    p.GetAttributes().All(a => a.AttributeClass?.Name != "NotStoredAttribute") &&
                    p.SetMethod is not null && p.GetMethod is not null)
                .Select(p => p.Type)).ToList();

        if (fieldTypes.Any(t => !SerializableType(t)))
        {
            fields = [];
        }
        else
        {
            GatherCollections(model, fieldTypes, collections, nested, processed);
        }

        var genericParameters = symbol.TypeParameters.Zip(symbol.TypeArguments, (p, a) => (p, a)).Select(p =>
            new GenericParameter(p.p.Name, new(p.a),
                p.p.HasReferenceTypeConstraint, p.p.HasValueTypeConstraint, p.p.HasConstructorConstraint,
                p.p.ConstraintTypes.Select(pp => new TypeRef(pp)).ToArray())).ToArray();

        if (namespaceName == "System" && className == "Tuple")
        {
            genericParameters = [];
        }

        var methods = new List<MethodInfo>();

        foreach (var methodSymbol in GetAllMembersIncludingBase(symbol).OfType<IMethodSymbol>().Where(m => m
                     .GetAttributes().Any(a =>
                         a.AttributeClass?.Name == OnSerializeAttributeName && a.AttributeClass.InODBLayerNamespace())))
        {
            if (methodSymbol.IsStatic)
            {
                nested.Add(GenerationError("BTDB0010",
                    "Method " + methodSymbol.Name + " with OnSerializeAttribute cannot be static in " +
                    symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    methodSymbol.Locations[0]));
                continue;
            }

            if (!methodSymbol.ReturnsVoid)
            {
                nested.Add(GenerationError("BTDB0011",
                    "Method " + methodSymbol.Name + " with OnSerializeAttribute must return void in " +
                    symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    methodSymbol.Locations[0]));
                continue;
            }

            if (!methodSymbol.Parameters.IsEmpty)
            {
                nested.Add(GenerationError("BTDB0012",
                    "Method " + methodSymbol.Name + " with OnSerializeAttribute must not have parameters in " +
                    symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    methodSymbol.Locations[0]));
                continue;
            }

            methods.Add(new(methodSymbol.Name,
                methodSymbol.ReturnType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                [], methodSymbol.DeclaredAccessibility is Accessibility.Public, null, Purpose.OnSerialize));
        }

        foreach (var methodSymbol in GetAllMembersIncludingBase(symbol).OfType<IMethodSymbol>().Where(m => m
                     .GetAttributes().Any(a =>
                         a.AttributeClass?.Name == OnBeforeRemoveAttributeName &&
                         a.AttributeClass.InODBLayerNamespace())))
        {
            if (methodSymbol.IsStatic)
            {
                nested.Add(GenerationError("BTDB0013",
                    "Method " + methodSymbol.Name + " with OnBeforeRemoveAttribute cannot be static in " +
                    symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    methodSymbol.Locations[0]));
                continue;
            }

            if (!methodSymbol.ReturnsVoid && methodSymbol.ReturnType.SpecialType != SpecialType.System_Boolean)
            {
                nested.Add(GenerationError("BTDB0014",
                    "Method " + methodSymbol.Name + " with OnBeforeRemoveAttribute must return bool or void in " +
                    symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    methodSymbol.Locations[0]));
                continue;
            }

            methods.Add(new(methodSymbol.Name,
                methodSymbol.ReturnType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                methodSymbol.Parameters.Select(p => new ParameterInfo(p.Name,
                    p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    DetectDependencyName(p),
                    p.Type.IsReferenceType,
                    p.NullableAnnotation == NullableAnnotation.Annotated, p.HasExplicitDefaultValue
                        ? CSharpSyntaxUtilities.FormatLiteral(p.ExplicitDefaultValue, new(p.Type))
                        : null,
                    GetEnumUnderlyingType(p.Type))).ToArray(),
                methodSymbol.DeclaredAccessibility is Accessibility.Public, null, Purpose.OnBeforeRemove));
        }

        // No IOC and no metadata => no generation
        if (fields.Length == 0 && parameters == null) return null;

        if (!isRelationItem && fields.Any(f => !f.Indexes.IsEmpty))
        {
            isRelationItem = true;
        }

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
            forceMetadata, isRelationItem,
            parameters,
            propertyInfos, parentDeclarations, dispatchers.ToArray(), fields, methods.ToArray(), implements,
            collections.ToArray(), genericParameters,
            nested.ToArray(), null);
    }

    static string GenericTypeFrom(ITypeSymbol argType)
    {
        if (argType.TypeKind == TypeKind.TypeParameter)
        {
            return argType.Name;
        }

        if (argType.TypeKind == TypeKind.Array)
        {
            return GenericTypeFrom(((IArrayTypeSymbol)argType).ElementType) + "[]";
        }

        if (argType is INamedTypeSymbol { IsGenericType: true } namedTypeSymbol)
        {
            var genericArgs = string.Join(", ",
                namedTypeSymbol.TypeArguments.Select(GenericTypeFrom));
            if (namedTypeSymbol is { IsTupleType: true, IsValueType: true })
            {
                return "(" + genericArgs + ")";
            }

            var genericName = namedTypeSymbol.ConstructedFrom.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            genericName = genericName.Substring(0, genericName.IndexOf('<'));
            return
                $"{genericName}<{genericArgs}>";
        }

        return argType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
    }

    static bool SerializableType(ITypeSymbol typeSymbol)
    {
        if (typeSymbol.TypeKind is TypeKind.Delegate or TypeKind.Error or TypeKind.Pointer or TypeKind.FunctionPointer)
            return false;
        if (typeSymbol.Name == "Task" && typeSymbol.InNamespace("System", "Threading", "Tasks"))
            return false;
        if (typeSymbol.Name == "ValueTask" && typeSymbol.InNamespace("System", "Threading", "Tasks"))
            return false;
        return true;
    }

    static bool HasDependencyAttribute(ISymbol p)
    {
        return p.GetAttributes().Any(a =>
            a.AttributeClass?.Name == "DependencyAttribute" && a.AttributeClass.InBTDBIOCNamespace());
    }

    static EquatableArray<IndexInfo> ExtractIndexInfo(IEnumerable<AttributeData> attributeDatas)
    {
        var indexInfos = new List<IndexInfo>();
        var hasBothPrimaryAndInKeyValue = 0;
        foreach (var attributeData in attributeDatas)
        {
            if (attributeData.AttributeClass?.Name == "PrimaryKeyAttribute")
            {
                var order = GetUintArgForAttributeData(attributeData, 0) ?? 0;
                var inKeyValue = GetBoolArgForAttributeData(attributeData, 1) ?? false;
                var primaryKeyAttribute = new IndexInfo(null, order, inKeyValue, 0);
                indexInfos.Add(primaryKeyAttribute);
                hasBothPrimaryAndInKeyValue |= 1;
            }

            if (attributeData.AttributeClass?.Name == "InKeyValueAttribute")
            {
                var order = GetUintArgForAttributeData(attributeData, 0) ?? 0;
                var primaryKeyAttribute = new IndexInfo(null, order, true, 0);
                indexInfos.Add(primaryKeyAttribute);
                hasBothPrimaryAndInKeyValue |= 2;
            }

            if (attributeData.AttributeClass?.Name == "SecondaryKeyAttribute")
            {
                var name = GetStringArgForAttributeData(attributeData);
                var order = GetUintArgForAttributeData(attributeData, "Order") ?? 0;
                var includePrimaryKeyOrder = GetUintArgForAttributeData(attributeData, "IncludePrimaryKeyOrder") ?? 0;
                var secondaryKeyAttribute = new IndexInfo(name, order, false, includePrimaryKeyOrder);
                indexInfos.Add(secondaryKeyAttribute);
            }
        }

        if (hasBothPrimaryAndInKeyValue == 3)
        {
            indexInfos.Add(new(null, 0, false, 1));
        }

        return new(indexInfos.ToArray());
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
                attributeSyntax.ArgumentList?.Arguments.FirstOrDefault()?.Expression as LiteralExpressionSyntax;
            if (literalExpressionSyntax != null)
            {
                return literalExpressionSyntax.Token.ValueText;
            }

            var nameOfExpressionSyntax =
                attributeSyntax.ArgumentList?.Arguments.FirstOrDefault()?.Expression as InvocationExpressionSyntax;
            if (nameOfExpressionSyntax != null)
            {
                var nameOfArgument = nameOfExpressionSyntax.ArgumentList.Arguments.FirstOrDefault()?.Expression;
                if (nameOfArgument is IdentifierNameSyntax identifierNameSyntax)
                {
                    return identifierNameSyntax.Identifier.ValueText;
                }
            }
        }

        if (attributeData?.ConstructorArguments.FirstOrDefault().Value is string str)
            return str;
        return null;
    }

    static uint? GetUintArgForAttributeData(AttributeData? attributeData, int index)
    {
        if (attributeData?.ApplicationSyntaxReference?.GetSyntax() is AttributeSyntax attributeSyntax)
        {
            var literalExpressionSyntax =
                attributeSyntax.ArgumentList?.Arguments.Skip(index)
                    .FirstOrDefault()
                    ?.Expression;
            if (literalExpressionSyntax?.Kind() == SyntaxKind.NumericLiteralExpression)
            {
                var val = literalExpressionSyntax.GetFirstToken().Value;
                if (val is int i)
                {
                    return (uint)i;
                }

                if (val is uint u)
                {
                    return u;
                }

                return uint.Parse(literalExpressionSyntax.ToString());
            }
        }

        return null;
    }

    static uint? GetUintArgForAttributeData(AttributeData? attributeData, string name)
    {
        if (attributeData?.ApplicationSyntaxReference?.GetSyntax() is AttributeSyntax attributeSyntax)
        {
            var literalExpressionSyntax =
                attributeSyntax.ArgumentList?.Arguments
                    .FirstOrDefault(a => a.NameEquals?.Name.Identifier.ValueText == name)
                    ?.Expression;
            if (literalExpressionSyntax?.Kind() == SyntaxKind.NumericLiteralExpression)
            {
                var val = literalExpressionSyntax.GetFirstToken().Value;
                if (val is int i)
                {
                    return (uint)i;
                }

                if (val is uint u)
                {
                    return u;
                }

                return uint.Parse(literalExpressionSyntax.ToString());
            }
        }

        return null;
    }

    static bool? GetBoolArgForAttributeData(AttributeData? attributeData, int index)
    {
        if (attributeData?.ApplicationSyntaxReference?.GetSyntax() is AttributeSyntax attributeSyntax)
        {
            var literalExpressionSyntax =
                attributeSyntax.ArgumentList?.Arguments.Skip(index)
                    .FirstOrDefault()
                    ?.Expression;
            if (literalExpressionSyntax?.Kind() == SyntaxKind.TrueLiteralExpression)
            {
                return true;
            }

            if (literalExpressionSyntax?.Kind() == SyntaxKind.FalseLiteralExpression)
            {
                return false;
            }
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
        if (type is INamedTypeSymbol namedTypeSymbol)
        {
            // ignore types like object or string
            if (!namedTypeSymbol.IsGenericType && type.ContainingNamespace.Name == "System") return;
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
                "global::BTDB.ODBLayer.IOrderedSet<", "global::System.Collections.Generic.HashSet<",
                "global::BTDB.ODBLayer.OrderedSet<", "global::System.Collections.Generic.HashSet<"
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
                    GatherCollection(model, elementType, collections, nested, processed);
            }
            else
            {
                ReadOnlySpan<string> collectionToInstance2 =
                [
                    "global::System.Collections.Generic.Dictionary<", "global::System.Collections.Generic.Dictionary<",
                    "global::System.Collections.Generic.IDictionary<", "global::System.Collections.Generic.Dictionary<",
                    "global::System.Collections.Generic.IReadOnlyDictionary<",
                    "global::System.Collections.Generic.Dictionary<",
                    "global::BTDB.ODBLayer.IOrderedDictionary<",
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
                        GatherCollection(model, keyType, collections, nested, processed);
                        GatherCollection(model, valueType, collections, nested, processed);
                    }
                }
                else
                {
                    if (namedTypeSymbol.IsGenericType)
                    {
                        foreach (var typeArgument in namedTypeSymbol.TypeArguments)
                        {
                            GatherCollection(model, typeArgument, collections, nested, processed);
                        }
                    }

                    if (namedTypeSymbol.IsValueType)
                    {
                        if (!namedTypeSymbol.IsGenericType && namedTypeSymbol.InNamespace("System")) return;
                        var gi = new GenerationInfo(GenerationType.Struct,
                            namedTypeSymbol.ContainingNamespace.ToString(),
                            namedTypeSymbol.Name,
                            namedTypeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), null, false,
                            false,
                            true, true, false, [], [],
                            [], [],
                            DetectValueTupleFields(namedTypeSymbol), [],
                            [], [], [],
                            [], null);
                        nested.Add(gi);
                    }
                    else if (namedTypeSymbol is { IsReferenceType: true, TypeKind: TypeKind.Class } &&
                             (ITypeSymbol)namedTypeSymbol is INamedTypeSymbol namedTypeSymbol1)
                    {
                        var gi = GenerationInfoForClass(namedTypeSymbol1, null, false, null, model,
                            collections, nested, true, false, processed);
                        if (gi != null)
                            nested.Add(gi);
                    }
                }
            }
        }
    }

    static EquatableArray<FieldsInfo> DetectValueTupleFields(INamedTypeSymbol namedTypeSymbol)
    {
        if (namedTypeSymbol.Name != "ValueTuple")
            return EquatableArray<FieldsInfo>.Empty;
        if (namedTypeSymbol.TypeArguments.Length == 0)
            return EquatableArray<FieldsInfo>.Empty;
        var fields = new List<FieldsInfo>();
        for (var i = 0; i < namedTypeSymbol.TypeArguments.Length; i++)
        {
            var type = namedTypeSymbol.TypeArguments[i];
            var name = "Item" + (i + 1);
            fields.Add(new(name, type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), GenericTypeFrom(type),
                null, type.IsReferenceType, name, null, null, false,
                "", EquatableArray<IndexInfo>.Empty));
        }

        return new(fields.ToArray());
    }

    static IEnumerable<IMethodSymbol> GetAllMethodsIncludingInheritance(INamedTypeSymbol symbol)
    {
        if (symbol.InODBLayerNamespace()) yield break;
        foreach (var member in symbol.GetMembers().OfType<IMethodSymbol>())
        {
            yield return member;
        }

        foreach (var symbolInterface in symbol.Interfaces)
        {
            foreach (var methodSymbol in GetAllMethodsIncludingInheritance(symbolInterface))
            {
                yield return methodSymbol;
            }
        }
    }

    static IEnumerable<ISymbol> GetAllMembersIncludingBase(INamedTypeSymbol symbol)
    {
        var members = new List<ISymbol>();
        var currentSymbol = symbol;
        var uniqueNames = new HashSet<string>();

        while (currentSymbol != null)
        {
            foreach (var iSymbol in currentSymbol.GetMembers())
            {
                if (iSymbol is IFieldSymbol or IPropertySymbol)
                {
                    if (!uniqueNames.Add(iSymbol.Name)) continue;
                    members.Add(iSymbol);
                }
                else
                {
                    members.Add(iSymbol);
                }
            }

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
            try
            {
                if (aecs.Expression is IdentifierNameSyntax ins && ModelExtensions.GetSymbolInfo(model, ins) is
                        { Symbol: IFieldSymbol })
                {
                    return ins.Identifier.ValueText;
                }
            }
            catch
            {
                // ignored
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
                attr.InBTDBNamespace()))
        {
            return [];
        }

        if (symbol is not { Kind: SymbolKind.NamedType, TypeKind: TypeKind.Interface })
        {
            return [];
        }

        if (symbol.DeclaredAccessibility == Accessibility.Private)
        {
            return [];
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

        return builder.Count == 0 ? [] : builder.ToArray();
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
                    new(generationInfo.Name, generationInfo.FullName, generationInfo.FullName,
                        "BTDB", DiagnosticSeverity.Error, true), generationInfo.Location));
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
            else if (generationInfo.GenType == GenerationType.RelationIface)
            {
                GenerateRelationInterfaceFactory(context, generationInfo);
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
            #pragma warning disable 612, 618, CS0649, CS8500
            using System;
            using System.Runtime.CompilerServices;
            [CompilerGenerated]
            static file class StackAllocationRegistrations
            {
            """);

        var idx = 0;
        foreach (var @struct in structs)
        {
            idx++;
            if (@struct.Fields.Count > 0)
            {
                factoryCode.Append($$"""

                        struct ValueTuple{{idx}}
                        {
                    {{string.Join("\n", @struct.Fields.Select(f => $"       public {f.Type} {f.Name};"))}}
                        }

                    """);
            }
        }

        factoryCode.Append($$"""

                [ModuleInitializer]
                internal static unsafe void Register4BTDB()
                {
            """);

        idx = 0;
        foreach (var @struct in structs)
        {
            idx++;
            factoryCode.Append($$"""

                        BTDB.Serialization.ReflectionMetadata.RegisterStackAllocator(typeof({{@struct.FullName}}), &Allocate{{idx}});
                        static void Allocate{{idx}}(ref byte ctx, ref nint ptr, delegate*<ref byte, void> chain)
                        {
                            {{@struct.FullName}} value = default;
                            ptr = (nint)Unsafe.AsPointer(ref value);
                            chain(ref ctx);
                            ptr = 0;
                        }

                """);
            if (@struct.Fields.Count > 0)
            {
                factoryCode.Append($$"""

                            ValueTuple{{idx}} valueTuple{{idx}} = new();
                            BTDB.Serialization.ReflectionMetadata.Register(new()
                            {
                                Type = typeof({{@struct.FullName}}),
                                Name = "{{@struct.Name}}",
                                Fields =
                                [
                    """);
                foreach (var field in @struct.Fields)
                {
                    factoryCode.Append($$"""

                                        new()
                                        {
                                            Name = "{{field.Name}}",
                                            Type = typeof({{field.Type}}),
                                            ByteOffset = (uint)Unsafe.ByteOffset(ref Unsafe.As<ValueTuple{{idx}}, byte>(ref valueTuple{{idx}}),
                                                ref Unsafe.As<{{field.Type}}, byte>(ref valueTuple{{idx}}.{{field.Name}})),
                                        },
                        """);
                }

                factoryCode.Append($$"""

                                ]
                            });
                    """);
            }
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
            #pragma warning disable 612, 618, CS0649
            using System;
            using System.Runtime.CompilerServices;
            [CompilerGenerated]
            static file class CollectionRegistrations
            {
                struct DictEntry<TKey, TValue>
                {
                    public uint HashCode;
                    public int Next;
                    public TKey Key;
                    public TValue Value;
                }

                struct HashSetEntry<T>
                {
                    public uint HashCode;
                    public int Next;
                    public T Value;
                }

                [ModuleInitializer]
                internal static unsafe void Register4BTDB()
                {
            """);

        var idx = 0;
        foreach (var collection in collections)
        {
            idx++;
            if (collection.ValueType != null)
            {
                factoryCode.Append($$"""

                            DictEntry<{{collection.KeyType}},{{collection.ValueType}}> e{{idx}} = new();
                            BTDB.Serialization.ReflectionMetadata.RegisterCollection(new()
                            {
                                Type = typeof({{collection.FullName}}),
                                ElementKeyType = typeof({{collection.KeyType}}),
                                ElementValueType = typeof({{collection.ValueType}}),
                                OffsetNext = (uint)Unsafe.ByteOffset(ref Unsafe.As<DictEntry<{{collection.KeyType}},{{collection.ValueType}}>, byte>(ref e{{idx}}),
                                    ref Unsafe.As<int, byte>(ref e{{idx}}.Next)),
                                OffsetKey = (uint)Unsafe.ByteOffset(ref Unsafe.As<DictEntry<{{collection.KeyType}},{{collection.ValueType}}>, byte>(ref e{{idx}}),
                                    ref Unsafe.As<{{collection.KeyType}}, byte>(ref e{{idx}}.Key)),
                                OffsetValue = (uint)Unsafe.ByteOffset(ref Unsafe.As<DictEntry<{{collection.KeyType}},{{collection.ValueType}}>, byte>(ref e{{idx}}),
                                    ref Unsafe.As<{{collection.ValueType}}, byte>(ref e{{idx}}.Value)),
                                SizeOfEntry = (uint)Unsafe.SizeOf<DictEntry<{{collection.KeyType}},{{collection.ValueType}}>>(),
                                Creator = &Create{{idx}},
                                AdderKeyValue = &Add{{idx}},
                                ODBCreator = &ODBCreate{{idx}}
                            });

                            static object Create{{idx}}(uint capacity)
                            {
                                return new {{collection.InstantiableFullName}}((int)capacity);
                            }

                            static void Add{{idx}}(object c, ref byte key, ref byte value)
                            {
                                Unsafe.As<{{collection.InstantiableFullName}}>(c).Add(Unsafe.As<byte, {{collection.KeyType}}>(ref key), Unsafe.As<byte, {{collection.ValueType}}>(ref value));
                            }

                            static object ODBCreate{{idx}}(BTDB.ODBLayer.IInternalObjectDBTransaction tr, BTDB.ODBLayer.ODBDictionaryConfiguration config, ulong id)
                            {
                                return new BTDB.ODBLayer.ODBDictionary<{{collection.KeyType}}, {{collection.ValueType}}>(tr, config, id);
                            }

                    """);
            }
            else
            {
                if (collection.KeyType.EndsWith("?"))
                {
                    // language=c#
                    factoryCode.Append($$"""

                                HashSetEntry<{{collection.KeyType}}> e{{idx}} = new();
                                BTDB.Serialization.ReflectionMetadata.RegisterCollection(new()
                                {
                                    Type = typeof({{collection.FullName}}),
                                    ElementKeyType = typeof({{collection.KeyType}}),
                                    Creator = &Create{{idx}},
                                    Adder = &Add{{idx}},
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
                else
                {
                    // language=c#
                    factoryCode.Append($$"""

                                HashSetEntry<{{collection.KeyType}}> e{{idx}} = new();
                                BTDB.Serialization.ReflectionMetadata.RegisterCollection(new()
                                {
                                    Type = typeof({{collection.FullName}}),
                                    ElementKeyType = typeof({{collection.KeyType}}),
                                    OffsetNext = (uint)Unsafe.ByteOffset(ref Unsafe.As<HashSetEntry<{{collection.KeyType}}>, byte>(ref e{{idx}}),
                                        ref Unsafe.As<int, byte>(ref e{{idx}}.Next)),
                                    OffsetKey = (uint)Unsafe.ByteOffset(ref Unsafe.As<HashSetEntry<{{collection.KeyType}}>, byte>(ref e{{idx}}),
                                        ref Unsafe.As<{{collection.KeyType}}, byte>(ref e{{idx}}.Value)),
                                    SizeOfEntry = (uint)Unsafe.SizeOf<HashSetEntry<{{collection.KeyType}}>>(),
                                    Creator = &Create{{idx}},
                                    Adder = &Add{{idx}},
                                    ODBCreator = &ODBCreate{{idx}}
                                });

                                static object Create{{idx}}(uint capacity)
                                {
                                    return new {{collection.InstantiableFullName}}((int)capacity);
                                }

                                static void Add{{idx}}(object c, ref byte value)
                                {
                                    Unsafe.As<{{collection.InstantiableFullName}}>(c).Add(Unsafe.As<byte, {{collection.KeyType}}>(ref value));
                                }

                                static object ODBCreate{{idx}}(BTDB.ODBLayer.IInternalObjectDBTransaction tr, BTDB.ODBLayer.ODBDictionaryConfiguration config, ulong id)
                                {
                                    return new BTDB.ODBLayer.ODBSet<{{collection.KeyType}}>(tr, config, id);
                                }

                        """);
                }
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

        var constructorParameters = generationInfo.ConstructorParameters ?? EquatableArray<ParameterInfo>.Empty;
        foreach (var (name, type, _, _, _, _, _) in constructorParameters)
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
        var isTuple = generationInfo is { Namespace: "System", Name: "Tuple" };
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

        uint[]? primaryKeyFields = null;
        uint indexOfInKeyValue = 0; // If it is PrimaryKeyFields.Length then there is no in key values
        (string Name, uint[] SecondaryKeyFields, uint ExplicitPrefixLength)[]? secondaryKeys = null;

        if (generationInfo.IsRelationItem)
        {
            (indexOfInKeyValue, primaryKeyFields, secondaryKeys) = BuildIndexInfo(generationInfo);
        }

        if (generationInfo.ConstructorParameters != null)
        {
            foreach (var (name, type, keyCode, isReference, optional, defaultValue, _) in generationInfo
                         .ConstructorParameters)
            {
                var normalizeType = NormalizeType(type);
                if (parameterIndex > 0) parametersCode.Append(", ");
                factoryCode.Append(
                    $"var f{parameterIndex} = container.CreateFactory(ctx, typeof({normalizeType}), ");
                if (keyCode != null)
                {
                    factoryCode.Append(keyCode);
                }
                else
                {
                    factoryCode.Append("\"");
                    factoryCode.Append(name);
                    factoryCode.Append("\"");
                }

                factoryCode.Append(");");
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
                var dependencyName = propertyInfo.DependencyName ?? "\"" + name + "\"";
                var isReference = propertyInfo.IsReference;
                var optional = propertyInfo.Optional;
                factoryCode.Append(
                    $"var f{parameterIndex} = container.CreateFactory(ctx, typeof({normalizedType}), {dependencyName});");
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

        var constructorParameters = generationInfo.ConstructorParameters ?? EquatableArray<ParameterInfo>.Empty;
        if (isTuple)
        {
            declarations.Append($$"""
                    class TupleStunt
                    {
                {{string.Join("\n", constructorParameters.Select((f, i) => $"        public {f.Type} Item{i + 1};"))}}
                    }


                """);
        }

        if (generationInfo.PrivateConstructor)
        {
            var constructorParametersText = new StringBuilder();
            foreach (var (name, type, _, _, _, _, _) in constructorParameters)
            {
                if (constructorParametersText.Length > 0) constructorParametersText.Append(", ");
                constructorParametersText.Append($"{type} {name}");
            }

            // language=c#
            declarations.Append($"""
                    [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
                    extern static {generationInfo.FullName} Constr({constructorParametersText});

                """);
        }

        declarations.Append(additionalDeclarations);

        var metadataCode = new StringBuilder();
        var nameWithGeneric = "";
        if (generationInfo.Fields.Count != 0 || generationInfo.PersistedName != null || generationInfo.ForceMetadata)
        {
            if (!generationInfo.GenericParameters.IsEmpty)
            {
                nameWithGeneric =
                    $"{generationInfo.FullName.Substring(0, generationInfo.FullName.IndexOf('<'))}<{string.Join(", ", generationInfo.GenericParameters.Select(p => p.Name))}>";
                // language=c#
                declarations.Append($$"""
                        public class Activator<{{string.Join(", ", generationInfo.GenericParameters.Select(p => p.Name))}}>{{GenericConstrains(generationInfo)}}
                        {

                    """);
                if (generationInfo.HasDefaultConstructor)
                {
                    // language=c#
                    declarations.Append($"""
                                [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
                                extern public static {nameWithGeneric} Creator();

                        """);
                }
                else
                {
                    // language=c#
                    declarations.Append($$"""
                                public static object Creator()
                                {
                                    return RuntimeHelpers.GetUninitializedObject(typeof({{generationInfo.FullName}}));
                                }

                        """);
                }
            }
            else
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
            }

            // language=c#
            metadataCode.Append($"""

                        var metadata = new global::BTDB.Serialization.ClassMetadata();
                        metadata.Name = "{generationInfo.Name}{GenericParams(generationInfo)}";
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
                        metadata.Creator = &{{ActivatorName(generationInfo)}}Creator;
                        var dummy = Unsafe.As<{{(isTuple ? "TupleStunt" : generationInfo.FullName)}}>(metadata);
                        metadata.Fields = [

                """);
            var fieldIndex = 0;
            if (isTuple)
            {
                foreach (var field in constructorParameters)
                {
                    var normalizedType = NormalizeType(field.Type);
                    fieldIndex++;
                    // language=c#
                    metadataCode.Append($$"""
                                    new global::BTDB.Serialization.FieldMetadata
                                    {
                                        Name = "Item{{fieldIndex}}",
                                        Type = typeof({{normalizedType}}),
                                        ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref dummy.Item{{fieldIndex}}),
                                    },

                        """);
                }
            }
            else
            {
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
                        if (generationInfo.GenericParameters.IsEmpty)
                        {
                            // language=c#
                            declarations.Append($"""
                                    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "{field.BackingName}")]
                                    extern static ref {normalizedType} Field{fieldIndex}({field.OwnerFullName} @this);

                                """);
                        }
                        else
                        {
                            // language=c#
                            declarations.Append($"""
                                        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "{field.BackingName}")]
                                        extern public static ref {NormalizeType(field.GenericType)} Field{fieldIndex}({nameWithGeneric} @this);

                                """);
                        }

                        // language=c#
                        metadataCode.Append($"""
                                            ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref {ActivatorName(generationInfo)}Field{fieldIndex}(dummy)),

                            """);
                    }

                    if (field is { GetterName: not null })
                    {
                        if (generationInfo.GenericParameters.IsEmpty)
                        {
                            // language=c#
                            declarations.Append($$"""
                                    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{{field.GetterName}}")]
                                    extern static {{normalizedType}} Getter{{fieldIndex}}({{field.OwnerFullName}} @this);
                                    static void GenGetter{{fieldIndex}}(object @this, ref byte value)
                                    {
                                        Unsafe.As<byte, {{normalizedType}}>(ref value) = Getter{{fieldIndex}}(Unsafe.As<{{field.OwnerFullName}}>(@this));
                                    }

                                """);
                        }
                        else
                        {
                            // language=c#
                            declarations.Append($$"""
                                        [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{{field.GetterName}}")]
                                        extern public static {{NormalizeType(field.GenericType)}} Getter{{fieldIndex}}({{nameWithGeneric}} @this);
                                        public static void GenGetter{{fieldIndex}}(object @this, ref byte value)
                                        {
                                            Unsafe.As<byte, {{NormalizeType(field.GenericType)}}>(ref value) = Getter{{fieldIndex}}(Unsafe.As<{{nameWithGeneric}}>(@this));
                                        }

                                """);
                        }

                        // language=c#
                        metadataCode.Append($"""
                                            PropRefGetter = &{ActivatorName(generationInfo)}GenGetter{fieldIndex},

                            """);
                    }

                    if (field is { SetterName: not null })
                    {
                        if (generationInfo.GenericParameters.IsEmpty)
                        {
                            // language=c#
                            declarations.Append($$"""
                                    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{{field.SetterName}}")]
                                    extern static void Setter{{fieldIndex}}({{field.OwnerFullName}} @this, {{normalizedType}} value);
                                    static void GenSetter{{fieldIndex}}(object @this, ref byte value)
                                    {
                                        Setter{{fieldIndex}}(Unsafe.As<{{field.OwnerFullName}}>(@this), Unsafe.As<byte, {{normalizedType}}>(ref value));
                                    }

                                """);
                        }
                        else
                        {
                            // language=c#
                            declarations.Append($$"""
                                        [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{{field.SetterName}}")]
                                        extern public static void Setter{{fieldIndex}}({{nameWithGeneric}} @this, {{NormalizeType(field.GenericType)}} value);
                                        public static void GenSetter{{fieldIndex}}(object @this, ref byte value)
                                        {
                                            Setter{{fieldIndex}}(Unsafe.As<{{nameWithGeneric}}>(@this), Unsafe.As<byte, {{NormalizeType(field.GenericType)}}>(ref value));
                                        }

                                """);
                        }

                        // language=c#
                        metadataCode.Append($"""
                                            PropRefSetter = &{ActivatorName(generationInfo)}GenSetter{fieldIndex},

                            """);
                    }

                    // language=c#
                    metadataCode.Append("            },\n");
                }
            }

            // language=c#
            metadataCode.Append("""
                        ];

                """);

            var methodIndex = 0;
            if (generationInfo.Methods.Any(m => m.Purpose == Purpose.OnSerialize))
            {
                // language=c#
                metadataCode.Append("""
                            metadata.OnSerialize = &OnSerialize;
                            static void OnSerialize(object @this)
                            {

                    """);

                foreach (var m in generationInfo.Methods.Where(m => m.Purpose == Purpose.OnSerialize))
                {
                    if (m.IsPublic)
                    {
                        metadataCode.Append($"""
                                        Unsafe.As<{generationInfo.FullName}>(@this).{m.Name}();

                            """);
                    }
                    else
                    {
                        methodIndex++;
                        if (generationInfo.GenericParameters.IsEmpty)
                        {
                            // language=c#
                            declarations.Append($"""
                                    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{m.Name}")]
                                    extern static void OnSerialize{methodIndex}({generationInfo.FullName} @this);

                                """);
                        }
                        else
                        {
                            // language=c#
                            declarations.Append($"""
                                        [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{m.Name}")]
                                        extern static void OnSerialize{methodIndex}({nameWithGeneric} @this);

                                """);
                        }

                        // language=c#
                        metadataCode.Append($"""
                                        {ActivatorName(generationInfo)}OnSerialize{methodIndex}(Unsafe.As<{generationInfo.FullName}>(@this));

                            """);
                    }
                }

                // language=c#
                metadataCode.Append("""
                            }

                    """);
            }

            methodIndex = 0;
            if (generationInfo.Methods.Any(m => m.Purpose == Purpose.OnBeforeRemove))
            {
                // language=c#
                metadataCode.Append("""
                            metadata.OnBeforeRemoveFactory = container =>
                            {

                    """);
                var methods = generationInfo.Methods.Where(m => m.Purpose == Purpose.OnBeforeRemove).ToArray();

                var paramIocIndex = 0;
                // if container needed
                if (methods.Any(m => ContainsNonTransactionParameter(m.Parameters.AsSpan())))
                {
                    // language=c#
                    metadataCode.Append("""
                                    global::System.ArgumentNullException.ThrowIfNull(container);

                        """);
                    foreach (var methodInfo in methods)
                    {
                        foreach (var parameter in methodInfo.Parameters)
                        {
                            if (IsTransactionType(parameter.Type)) continue;

                            var keyCode = parameter.KeyCode ?? '"' + parameter.Name + '"';
                            // language=c#
                            metadataCode.Append($"""
                                            var f{paramIocIndex} = container.CreateFactory(new global::BTDB.IOC.CreateFactoryCtx(), typeof({NormalizeType(parameter.Type)}), {keyCode});

                                """);
                            if (!parameter.Optional)
                            {
                                // language=c#
                                metadataCode.Append($"""
                                                if (f{paramIocIndex} == null) throw new global::System.ArgumentException("Cannot resolve {NormalizeType(parameter.Type)} {parameter.Name} parameter of {generationInfo.FullName}.{methodInfo.Name}");

                                    """);
                            }

                            paramIocIndex++;
                        }
                    }
                }

                // language=c#
                metadataCode.Append($$"""
                                return (transaction, value) =>
                                {
                                    var val = Unsafe.As<{{generationInfo.FullName}}>(value);
                                    var res = false;

                    """);
                paramIocIndex = 0;

                foreach (var m in methods)
                {
                    var maybeOr = m.ResultType == "void" ? "" : "res |= ";
                    if (m.IsPublic)
                    {
                        metadataCode.Append($"""
                                            {maybeOr}val.{m.Name}({OnBeforeRemoveCallParams(m.Parameters, ref paramIocIndex)});

                            """);
                    }
                    else
                    {
                        methodIndex++;
                        if (generationInfo.GenericParameters.IsEmpty)
                        {
                            // language=c#
                            declarations.Append($"""
                                    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{m.Name}")]
                                    extern static void OnBeforeRemove{methodIndex}({generationInfo.FullName} @this{OnBeforeRemoveDeclareParams(m.Parameters)});

                                """);
                        }
                        else
                        {
                            // language=c#
                            declarations.Append($"""
                                        [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "{m.Name}")]
                                        extern static void OnBeforeRemove{methodIndex}({nameWithGeneric} @this{OnBeforeRemoveDeclareParams(m.Parameters)});

                                """);
                        }

                        // language=c#
                        metadataCode.Append($"""
                                            {maybeOr}{ActivatorName(generationInfo)}OnBeforeRemove{methodIndex}(val{OnBeforeRemoveCallParams(m.Parameters, ref paramIocIndex, true)});

                            """);
                    }
                }

                // language=c#
                metadataCode.Append("""
                                    return res;
                                };
                            };

                    """);
            }

            if (primaryKeyFields != null)
            {
                metadataCode.Append($"""
                            metadata.PrimaryKeyFields = [{string.Join(", ", primaryKeyFields)}];
                            metadata.IndexOfInKeyValue = {indexOfInKeyValue};
                            metadata.SecondaryKeys = [{string.Join(", ", secondaryKeys!.Select(v => $"(\"{v.Name}\", [{string.Join(", ", v.SecondaryKeyFields)}])"))}];

                    """);
            }

            // language=c#
            metadataCode.Append("        global::BTDB.Serialization.ReflectionMetadata.Register(metadata);");

            if (!generationInfo.GenericParameters.IsEmpty)
            {
                // language=c#
                declarations.Append("""
                        }

                    """);
            }
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

        var registerFactoryCode = "";
        if (generationInfo.ConstructorParameters != null)
        {
            registerFactoryCode = $$"""
                        global::BTDB.IOC.IContainer.RegisterFactory(typeof({{generationInfo.FullName}}), (container, ctx) =>
                        {
                            {{factoryCode}}return (container2, ctx2) =>
                            {
                                var res = {{(generationInfo.PrivateConstructor ? "Constr" : "new " + generationInfo.FullName)}}({{parametersCode}});
                                {{propertyCode}}return res;
                            };
                        });{{metadataCode}}{{dispatchers}}

                """;
        }
        else
        {
            registerFactoryCode = metadataCode.ToString() + dispatchers;
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
            {{registerFactoryCode}}    }
            {{(generationInfo.IsPartial ? new('}', generationInfo.ParentDeclarations.Count) : "}")}}

            """;

        context.AddSource(
            $"{generationInfo.FullName.Replace("global::", "").Replace("<", "[").Replace(">", "]").Replace(" ", "")}.g.cs",
            SourceText.From(code, Encoding.UTF8));
    }

    static (uint indexOfInKeyValue, uint[] primaryKeyFields,
        (string Name, uint[] SecondaryKeyFields, uint ExplicitPrefixLength)[] secondaryKeys)
        BuildIndexInfo(GenerationInfo generationInfo)
    {
        uint indexOfInKeyValue;
        uint[] primaryKeyFields;
        (string Name, uint[] SecondaryKeyFields, uint ExplicitPrefixLength)[] secondaryKeys;
        var primaryKeyOrder2Info = new Dictionary<uint, (uint Index, bool InKeyValue)>();
        var secondaryKeyName2Info =
            new Dictionary<string, List<(uint Index, uint Order, uint IncludePrimaryKeyOrder)>>();

        for (var index = 0; index < generationInfo.Fields.Count; index++)
        {
            var generationInfoField = generationInfo.Fields[index];
            foreach (var indexInfo in generationInfoField.Indexes)
            {
                if (indexInfo.Name == null)
                {
                    primaryKeyOrder2Info[indexInfo.Order] = ((uint)index, indexInfo.InKeyValue);
                }
                else
                {
                    if (!secondaryKeyName2Info.ContainsKey(indexInfo.Name))
                        secondaryKeyName2Info[indexInfo.Name] = [];
                    secondaryKeyName2Info[indexInfo.Name]
                        .Add(((uint)index, indexInfo.Order, indexInfo.IncludePrimaryKeyOrder));
                }
            }
        }

        var pkInfos = primaryKeyOrder2Info.OrderBy(p => p.Key).ToList();
        indexOfInKeyValue = (uint)pkInfos.Count;
        primaryKeyFields = new uint[pkInfos.Count];
        for (var i = 0; i < pkInfos.Count; i++)
        {
            primaryKeyFields[i] = pkInfos[i].Value.Index;
            if (pkInfos[i].Value.InKeyValue && i < indexOfInKeyValue) indexOfInKeyValue = (uint)i;
        }

        secondaryKeys = new (string Name, uint[] SecondaryKeyFields, uint ExplicitPrefixLength)[
            secondaryKeyName2Info.Count];
        var j = 0;
        foreach (var keyValuePair in secondaryKeyName2Info)
        {
            var ordered = keyValuePair.Value.OrderBy(v => v.Order).ToList();
            var secondaryKeyFields = new List<uint>();
            foreach (var info in ordered)
            {
                for (var i = 1; i <= info.IncludePrimaryKeyOrder; i++)
                {
                    if (primaryKeyOrder2Info.TryGetValue((uint)i, out var pkInfo))
                    {
                        secondaryKeyFields.Add(pkInfo.Index);
                    }
                }

                secondaryKeyFields.Add(info.Index);
            }

            var explicitPrefixLength = (uint)secondaryKeyFields.Count;

            for (var index = 0; index < primaryKeyFields.Length; index++)
            {
                if (index >= indexOfInKeyValue) break;
                var primaryKeyField = primaryKeyFields[index];
                if (!secondaryKeyFields.Contains(primaryKeyField))
                {
                    secondaryKeyFields.Add(primaryKeyField);
                }
            }

            secondaryKeys[j++] = (keyValuePair.Key, secondaryKeyFields.ToArray(), explicitPrefixLength);
        }

        return (indexOfInKeyValue, primaryKeyFields, secondaryKeys);
    }

    static void AppendUpdateByIdValidation(StringBuilder validationBody, MethodInfo method, int pkParamCount,
        string valueFieldsExpression, string typeConvertorGeneratorExpression)
    {
        var paramCount = method.Parameters.Count;
        var valueParamCount = paramCount - pkParamCount;
        if (valueParamCount <= 0) return;

        validationBody.Append("                {\n");
        validationBody.Append($"                    var valueFields = {valueFieldsExpression};\n");
        validationBody.Append(
            $"                    var typeConvertorGenerator = {typeConvertorGeneratorExpression};\n");
        validationBody.Append(
            "                    for (var valueFieldIndex = 0; valueFieldIndex < valueFields.Length; valueFieldIndex++)\n");
        validationBody.Append("                    {\n");
        validationBody.Append("                        var valueField = valueFields[valueFieldIndex];\n");
        validationBody.Append("                        if (valueField.Computed) continue;\n");
        validationBody.Append("                        var paramIndex = -1;\n");
        for (var i = 0; i < valueParamCount; i++)
        {
            var keyword = i == 0 ? "if" : "else if";
            var param = method.Parameters[pkParamCount + i];
            validationBody.Append(
                $"                        {keyword} (string.Equals(valueField.Name, \"{param.Name}\", StringComparison.OrdinalIgnoreCase))\n");
            validationBody.Append("                        {\n");
            validationBody.Append($"                            paramIndex = {i};\n");
            validationBody.Append("                        }\n");
        }

        validationBody.Append("                        if (paramIndex == -1) continue;\n");
        validationBody.Append("                        var handler = valueField.Handler!;\n");
        validationBody.Append("                        switch (paramIndex)\n");
        validationBody.Append("                        {\n");
        for (var i = 0; i < valueParamCount; i++)
        {
            var param = method.Parameters[pkParamCount + i];
            var paramType = NormalizeType(param.Type);
            var paramTypeForTypeof = paramType;
            if (param.IsReference && paramTypeForTypeof.EndsWith("?", StringComparison.Ordinal))
            {
                paramTypeForTypeof = paramTypeForTypeof.Substring(0, paramTypeForTypeof.Length - 1);
            }

            validationBody.Append($"                            case {i}:\n");
            validationBody.Append("                            {\n");
            validationBody.Append(
                $"                                var parameterType = typeof({paramTypeForTypeof});\n");
            validationBody.Append(
                "                                var specializedHandler = handler.SpecializeSaveForType(parameterType);\n");
            validationBody.Append(
                "                                if (typeConvertorGenerator.GenerateConversion(parameterType, specializedHandler.HandledType()!) == null)\n");
            validationBody.Append(
                $"                                    throw new global::BTDB.KVDBLayer.BTDBException(\"Method {method.Name} matched parameter {param.Name} has wrong type \" + global::BTDB.IL.EmitHelpers.ToSimpleName(parameterType) + \" not convertible to \" + global::BTDB.IL.EmitHelpers.ToSimpleName(specializedHandler.HandledType()!));\n");
            validationBody.Append("                                break;\n");
            validationBody.Append("                            }\n");
        }

        validationBody.Append("                        }\n");
        validationBody.Append("                    }\n");
        validationBody.Append("                }\n");
    }

    static string OnBeforeRemoveDeclareParams(EquatableArray<ParameterInfo> parameters)
    {
        if (parameters.IsEmpty) return "";
        return ", " + string.Join(", ", parameters.Select(p => $"{NormalizeType(p.Type)} {p.Name}"));
    }

    static string OnBeforeRemoveCallParams(EquatableArray<ParameterInfo> parameters, ref int paramIocIndex,
        bool withFirstComma = false)
    {
        if (parameters.IsEmpty) return "";
        var sb = new StringBuilder();
        foreach (var parameter in parameters)
        {
            if (withFirstComma || sb.Length > 0) sb.Append(", ");
            if (IsTransactionType(parameter.Type))
            {
                sb.Append("transaction");
                continue;
            }

            var normalizeType = NormalizeType(parameter.Type);
            var isReference = parameter.IsReference;
            if (!parameter.Optional)
            {
                sb.Append(isReference ? $"Unsafe.As<{normalizeType}>(" : $"({normalizeType})(");
                sb.Append($"f{paramIocIndex}(container, null))!");
            }
            else
            {
                sb.Append($"f{paramIocIndex} != null ? ");
                sb.Append(isReference ? $"Unsafe.As<{normalizeType}>(" : $"(({normalizeType})");
                sb.Append($"f{paramIocIndex}(container, null)) : " +
                          (parameter.DefaultValue ?? $"default({normalizeType})"));
            }

            paramIocIndex++;
        }

        return sb.ToString();
    }

    static bool ContainsNonTransactionParameter(ReadOnlySpan<ParameterInfo> parameters)
    {
        foreach (var parameter in parameters)
        {
            if (!IsTransactionType(parameter.Type)) return true;
        }

        return false;
    }

    static bool IsTransactionType(string type)
    {
        return type == "global::BTDB.ODBLayer.IObjectDBTransaction";
    }

    static string GenericConstrains(GenerationInfo generationInfo)
    {
        if (!generationInfo.GenericParameters.Any(SomeConstraint)) return "";

        return
            $" where {string.Join(", ", generationInfo.GenericParameters.Where(SomeConstraint).Select(p => $"{p.Name}: " + string.Join(", ", EnumerateConstraints(p))))}";
    }

    static IEnumerable<string> EnumerateConstraints(GenericParameter genericParameter)
    {
        if (genericParameter.IsClassConstraint) yield return "class";
        if (genericParameter.IsStructConstraint) yield return "struct";
        foreach (var constraint in
                 genericParameter.SpecificTypeConstraints.Where(t => t.TypeKind != TypeKind.Interface))
        {
            yield return constraint.FullyQualifiedName;
        }

        foreach (var constraint in
                 genericParameter.SpecificTypeConstraints.Where(t => t.TypeKind == TypeKind.Interface))
        {
            yield return constraint.FullyQualifiedName;
        }

        if (genericParameter.IsNewConstraint) yield return "new()";
    }

    static bool SomeConstraint(GenericParameter arg)
    {
        return arg.IsClassConstraint || arg.IsNewConstraint || arg.IsStructConstraint ||
               arg.SpecificTypeConstraints.Count > 0;
    }

    static string GenericParams(GenerationInfo generationInfo)
    {
        if (generationInfo.GenericParameters.IsEmpty) return "";
        return
            $"<{string.Join(", ", generationInfo.GenericParameters.Select(p => p.Value.FullyQualifiedName.Replace("global::", "")))}>";
    }

    static string ActivatorName(GenerationInfo generationInfo)
    {
        if (generationInfo.GenericParameters.IsEmpty) return "";
        return
            $"Activator<{string.Join(", ", generationInfo.GenericParameters.Select(p => p.Value.FullyQualifiedName))}>.";
    }

    static void GenerateRelationInterfaceFactory(SourceProductionContext context, GenerationInfo generationInfo)
    {
        var code = new StringBuilder();
        // language=c#
        code.Append("""
            // <auto-generated/>
            #pragma warning disable 612,618
            #nullable enable
            using System;
            using System.Runtime.CompilerServices;
            using BTDB.ODBLayer;

            """);

        code.Append($"// Name: {generationInfo.Name}\n");
        if (generationInfo.PersistedName != null)
            code.Append($"// Persisted Name: {generationInfo.PersistedName}\n");
        foreach (var f in generationInfo.Fields.GetArray()!)
        {
            code.Append(
                $"""
                // Field: {f.StoredName ?? f.Name} {f.Type}{(f.IsReference ? " reference" : "")}{(f.ReadOnly ? " computed" : "")}

                """);
            foreach (var i in f.Indexes.GetArray()!)
            {
                code.Append($"""
                    //           {(i.Name != null ? "SecondaryIndex " + i.Name : "PrimaryIndex")}: {i.Order}{(i.InKeyValue ? " InKeyValue" : "")}{(i.IncludePrimaryKeyOrder != 0 ? " IncludePrimaryKeyOrder " + i.IncludePrimaryKeyOrder : "")}

                    """);
            }
        }

        if (generationInfo.Namespace != null)
        {
            // language=c#
            code.Append($"\nnamespace {generationInfo.Namespace};\n");
        }

        // language=c#
        var declarations = new StringBuilder();
        declarations.Append("[CompilerGenerated]\n");
        declarations.Append($"file class {generationInfo.Name}Registration\n{{\n");
        var implName = $"Impl{generationInfo.Name.Substring(1)}";
        var (indexOfInKeyValue, primaryKeyFields, secondaryKeys) = BuildIndexInfo(generationInfo);
        var constructorBody = new StringBuilder();
        var creatorValidationBody = new StringBuilder();
        var needsRelationInfoAccessors = false;
        // language=C#
        declarations.Append($$"""
                public class {{implName}} : global::BTDB.ODBLayer.RelationDBManipulator<{{generationInfo.Implements[0].FullyQualifiedName}}>, {{generationInfo.FullName}}
                {
            """);
        var classBodyStart = declarations.Length;
        foreach (var method in generationInfo.Methods)
        {
            // language=C#
            declarations.Append($$"""

                        [SkipLocalsInit]
                        {{method.ResultType ?? "void"}} {{method.DefinedInType}}.{{method.Name}}({{string.Join(", ", method.Parameters.Select(p => $"{p.Type} {p.Name}"))}})
                        {

                """);
            if (method is { Name: "Insert", ResultType: null })
            {
                declarations.Append(
                    $"            base.InsertUniqueOrThrow({string.Join(", ", method.Parameters.Select(p => p.Name))});\n");
            }
            else if (method.Name is "Insert" or "Upsert" or "ShallowInsert" or "ShallowUpsert" or
                     "ShallowUpsertWithSizes")
            {
                declarations.Append(
                    $"            {(method.ResultType != null ? "return " : "")}base.{method.Name}({string.Join(", ", method.Parameters.Select(p => p.Name))});\n");
            }
            else if (method.Name is "Update" or "ShallowUpdate")
            {
                declarations.Append(
                    $"            {(method.ResultType != null ? "return " : "")}base.{method.Name}({string.Join(", ", method.Parameters.Select(p => p.Name))});\n");
            }
            else if (method.Name.StartsWith("UpdateById", StringComparison.Ordinal))
            {
                var paramCount = method.Parameters.Count;
                var pkParamCount = primaryKeyFields.Length;
                var valueParamCount = paramCount - pkParamCount;
                var returnsBool = method.ResultType != null && IsBoolType(method.ResultType);
                if (valueParamCount > 0)
                {
                    AppendUpdateByIdValidation(creatorValidationBody, method, pkParamCount,
                        "ClientRelationVersionInfoAccessor(relationInfo).Fields.Span",
                        "RelationInfoResolverAccessor(relationInfo).TypeConvertorGenerator");
                    needsRelationInfoAccessors = true;
                }

                declarations.Append(
                    "            var writer = global::BTDB.StreamLayer.MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[512]);\n");
                declarations.Append("            WriteRelationPKPrefix(ref writer);\n");
                AppendWriterCtxIfNeeded(declarations, method.Parameters.Take(pkParamCount), null);
                declarations.Append("            var lenOfPkWoInKeyValues = 0;\n");
                for (var i = 0; i < pkParamCount; i++)
                {
                    if (i == (int)indexOfInKeyValue)
                    {
                        declarations.Append("            lenOfPkWoInKeyValues = (int)writer.GetCurrentPosition();\n");
                    }

                    AppendWriteOrderableParameter(declarations, method.Parameters[i]);
                }

                declarations.Append("            var keyBytes = writer.GetScopedSpanAndReset();\n");
                if (valueParamCount <= 0)
                {
                    if (returnsBool)
                    {
                        declarations.Append(
                            "            var updated = base.UpdateByIdInKeyValues(keyBytes, lenOfPkWoInKeyValues, false);\n");
                        declarations.Append("            return updated;\n");
                    }
                    else
                    {
                        declarations.Append(
                            "            _ = base.UpdateByIdInKeyValues(keyBytes, lenOfPkWoInKeyValues, true);\n");
                    }
                }
                else
                {
                    declarations.Append(
                        "            var oldValueBytes = default(global::System.ReadOnlySpan<byte>);\n");
                    if (returnsBool)
                    {
                        declarations.Append(
                            "            var updated = base.UpdateByIdStart(keyBytes, ref writer, ref oldValueBytes, lenOfPkWoInKeyValues, false);\n");
                        declarations.Append("            if (!updated) return false;\n");
                    }
                    else
                    {
                        declarations.Append(
                            "            _ = base.UpdateByIdStart(keyBytes, ref writer, ref oldValueBytes, lenOfPkWoInKeyValues, true);\n");
                    }

                    declarations.Append("            global::BTDB.ODBLayer.DBWriterCtx? valueCtx = null;\n");
                    declarations.Append("            global::BTDB.ODBLayer.DBReaderCtx? readerCtx = null;\n");
                    declarations.Append("            unsafe\n");
                    declarations.Append("            {\n");
                    declarations.Append("                fixed (byte* _ = oldValueBytes)\n");
                    declarations.Append("                {\n");
                    declarations.Append(
                        "                    var reader = global::BTDB.StreamLayer.MemReader.CreateFromPinnedSpan(oldValueBytes);\n");
                    declarations.Append("                    reader.SkipVUInt32();\n");
                    declarations.Append("                    uint memoPos = 0;\n");
                    declarations.Append("                    var copyMode = false;\n");
                    declarations.Append("                    var valueFields = ValueFields;\n");
                    declarations.Append(
                        "                    for (var valueFieldIndex = 0; valueFieldIndex < valueFields.Length; valueFieldIndex++)\n");
                    declarations.Append("                    {\n");
                    declarations.Append("                        var valueField = valueFields[valueFieldIndex];\n");
                    declarations.Append("                        if (valueField.Computed) continue;\n");
                    declarations.Append("                        var paramIndex = -1;\n");
                    for (var i = 0; i < valueParamCount; i++)
                    {
                        var keyword = i == 0 ? "if" : "else if";
                        var param = method.Parameters[pkParamCount + i];
                        declarations.Append(
                            $"                        {keyword} (string.Equals(valueField.Name, \"{param.Name}\", StringComparison.OrdinalIgnoreCase))\n");
                        declarations.Append("                        {\n");
                        declarations.Append($"                            paramIndex = {i};\n");
                        declarations.Append("                        }\n");
                    }

                    declarations.Append("                        var newCopyMode = paramIndex == -1;\n");
                    declarations.Append("                        if (copyMode != newCopyMode)\n");
                    declarations.Append("                        {\n");
                    declarations.Append("                            if (newCopyMode)\n");
                    declarations.Append("                            {\n");
                    declarations.Append(
                        "                                memoPos = (uint)reader.GetCurrentPositionWithoutController();\n");
                    declarations.Append("                            }\n");
                    declarations.Append("                            else\n");
                    declarations.Append("                            {\n");
                    declarations.Append(
                        "                                reader.CopyFromPosToWriter(memoPos, ref writer);\n");
                    declarations.Append("                            }\n");
                    declarations.Append("                            copyMode = newCopyMode;\n");
                    declarations.Append("                        }\n");
                    declarations.Append("                        var handler = valueField.Handler!;\n");
                    declarations.Append("                        if (!newCopyMode)\n");
                    declarations.Append("                        {\n");
                    declarations.Append("                            switch (paramIndex)\n");
                    declarations.Append("                            {\n");
                    for (var i = 0; i < valueParamCount; i++)
                    {
                        var param = method.Parameters[pkParamCount + i];
                        var paramType = NormalizeType(param.Type);
                        var paramTypeForTypeof = paramType;
                        if (param.IsReference && paramTypeForTypeof.EndsWith("?", StringComparison.Ordinal))
                        {
                            paramTypeForTypeof = paramTypeForTypeof.Substring(0, paramTypeForTypeof.Length - 1);
                        }

                        declarations.Append($"                                case {i}:\n");
                        declarations.Append("                                {\n");
                        declarations.Append(
                            $"                                    var save = handler.Save(typeof({paramTypeForTypeof}), Transaction.Owner.TypeConverterFactory);\n");
                        declarations.Append("                                    if (handler.NeedsCtx())\n");
                        declarations.Append("                                    {\n");
                        declarations.Append(
                            "                                        valueCtx ??= new global::BTDB.ODBLayer.DBWriterCtx(Transaction);\n");
                        declarations.Append("                                    }\n");
                        declarations.Append(
                            $"                                    save(ref writer, valueCtx, ref Unsafe.As<{paramType}, byte>(ref {param.Name}));\n");
                        declarations.Append("                                    break;\n");
                        declarations.Append("                                }\n");
                    }

                    declarations.Append("                            }\n");
                    declarations.Append("                        }\n");
                    declarations.Append("                        if (handler.NeedsCtx())\n");
                    declarations.Append("                        {\n");
                    declarations.Append(
                        "                            readerCtx ??= new global::BTDB.ODBLayer.DBReaderCtx(Transaction);\n");
                    declarations.Append("                            handler.Skip(ref reader, readerCtx);\n");
                    declarations.Append("                        }\n");
                    declarations.Append("                        else\n");
                    declarations.Append("                        {\n");
                    declarations.Append("                            handler.Skip(ref reader, null);\n");
                    declarations.Append("                        }\n");
                    declarations.Append("                    }\n");
                    declarations.Append("                    if (copyMode)\n");
                    declarations.Append("                    {\n");
                    declarations.Append("                        reader.CopyFromPosToWriter(memoPos, ref writer);\n");
                    declarations.Append("                    }\n");
                    declarations.Append("                }\n");
                    declarations.Append("            }\n");
                    declarations.Append(
                        "            base.UpdateByIdFinish(keyBytes, oldValueBytes, writer.GetSpan());\n");
                    if (returnsBool)
                    {
                        declarations.Append("            return true;\n");
                    }
                }
            }
            else if (method.Name == "Contains")
            {
                declarations.Append(
                    "            var writer = global::BTDB.StreamLayer.MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[512]);\n");
                declarations.Append("            WriteRelationPKPrefix(ref writer);\n");
                AppendWriterCtxIfNeeded(declarations, method.Parameters, null);

                for (var i = 0; i < method.Parameters.Count; i++)
                {
                    AppendWriteOrderableParameter(declarations, method.Parameters[i]);
                }

                declarations.Append("            return base.Contains(writer.GetSpan());\n");
            }
            else if (method.Name.StartsWith("FindBy", StringComparison.Ordinal))
            {
                var paramCount = method.Parameters.Count;
                var (indexName, hasOrDefault) = StripVariant(secondaryKeys, method.Name, true);
                var isPrefixBased = IsEnumerableType(method.ResultType);
                var itemType = isPrefixBased
                    ? ExtractEnumerableItemType(method.ResultType ?? "")
                    : NormalizeType(method.ResultType ?? "");
                var loaderIndex = FindLoaderIndex(generationInfo.Implements, itemType);

                AppendWriterCtxIfNeeded(declarations, method.Parameters, null);
                declarations.Append(
                    "            var writer = global::BTDB.StreamLayer.MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[512]);\n");
                if (indexName == "Id")
                {
                    declarations.Append("            WriteRelationPKPrefix(ref writer);\n");
                }
                else
                {
                    var secondaryKeyIndex = FindSecondaryKeyIndex(secondaryKeys, indexName);
                    declarations.Append(
                        $"            var remappedSecondaryKeyIndex = RemapPrimeSK({secondaryKeyIndex}u);\n");
                    declarations.Append(
                        "            WriteRelationSKPrefix(ref writer, remappedSecondaryKeyIndex);\n");
                }

                for (var i = 0; i < paramCount; i++)
                {
                    AppendWriteOrderableParameter(declarations, method.Parameters[i]);
                }

                if (isPrefixBased)
                {
                    if (indexName == "Id")
                    {
                        declarations.Append(
                            $"            return ({method.ResultType})base.FindByPrimaryKeyPrefix<{itemType}>(writer.GetSpan(), {loaderIndex});\n");
                    }
                    else
                    {
                        declarations.Append(
                            $"            return ({method.ResultType})base.FindBySecondaryKey<{itemType}>(remappedSecondaryKeyIndex, writer.GetSpan(), {loaderIndex});\n");
                    }
                }
                else
                {
                    var throwWhenNotFound = hasOrDefault ? "false" : "true";
                    if (indexName == "Id")
                    {
                        declarations.Append(
                            $"            return base.FindByIdOrDefault<{itemType}>(writer.GetSpan(), {throwWhenNotFound}, {loaderIndex});\n");
                    }
                    else
                    {
                        declarations.Append(
                            $"            return base.FindBySecondaryKeyOrDefault<{itemType}>(remappedSecondaryKeyIndex, writer.GetSpan(), {throwWhenNotFound}, {loaderIndex});\n");
                    }
                }
            }
            else if (method.Name.StartsWith("RemoveBy", StringComparison.Ordinal) ||
                     method.Name.StartsWith("ShallowRemoveBy", StringComparison.Ordinal))
            {
                if (method.Name == "RemoveByIdPartial")
                {
                    var paramCount = method.Parameters.Count;
                    var maxCountParam = method.Parameters[paramCount - 1];
                    var prefixParamCount = paramCount - 1;

                    AppendWriterCtxIfNeeded(declarations, method.Parameters.Take(prefixParamCount), null);
                    declarations.Append(
                        "            var writer = global::BTDB.StreamLayer.MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[512]);\n");
                    declarations.Append("            WriteRelationPKPrefix(ref writer);\n");
                    for (var i = 0; i < prefixParamCount; i++)
                    {
                        AppendWriteOrderableParameter(declarations, method.Parameters[i]);
                    }

                    var removeExpr = $"base.RemoveByPrimaryKeyPrefixPartial(writer.GetSpan(), {maxCountParam.Name})";
                    declarations.Append($"            return {WrapCountResult(removeExpr, method.ResultType)};\n");
                }
                else
                {
                    var paramCount = method.Parameters.Count;
                    string? advParamType = null;
                    var advType = string.Empty;
                    var lastParam = paramCount > 0 ? method.Parameters[paramCount - 1] : null;
                    var lastParamName = lastParam?.Name ?? "";
                    var hasAdvancedEnumerator = paramCount > 0 &&
                                                TryGetAdvancedEnumeratorParamType(lastParam!.Type, out advType);
                    if (hasAdvancedEnumerator)
                    {
                        advParamType = advType;
                    }

                    var isPrefixBased = IsRemoveByCountReturnType(method.ResultType);
                    var prefixParamCount = paramCount - (hasAdvancedEnumerator ? 1 : 0);

                    if (hasAdvancedEnumerator)
                    {
                        if (!isPrefixBased)
                        {
                            declarations.Append(
                                $"            throw new global::BTDB.KVDBLayer.BTDBException(\"Return value in {method.Name} must be int, uint, long, or ulong.\");\n");
                        }
                        else
                        {
                            AppendWriterCtxIfNeeded(declarations, method.Parameters.Take(prefixParamCount),
                                advParamType);
                            AppendAdvancedKeyPrefix(declarations, false, null, method.Parameters, prefixParamCount,
                                lastParamName, advParamType!);
                            var removeExpr =
                                $"base.RemoveByIdAdvancedParam({lastParamName}.Order, {lastParamName}.StartProposition, prefixLen, startKeyBytes, {lastParamName}.EndProposition, endKeyBytes)";
                            declarations.Append(
                                $"            return {WrapCountResult(removeExpr, method.ResultType)};\n");
                        }
                    }
                    else if (isPrefixBased)
                    {
                        AppendWriterCtxIfNeeded(declarations, method.Parameters, null);
                        declarations.Append(
                            "            var writer = global::BTDB.StreamLayer.MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[512]);\n");
                        declarations.Append("            WriteRelationPKPrefix(ref writer);\n");
                        for (var i = 0; i < paramCount; i++)
                        {
                            AppendWriteOrderableParameter(declarations, method.Parameters[i]);
                        }

                        var removeExpr = (!HasOnBeforeRemove(generationInfo) &&
                                          AllKeyPrefixesAreSame(primaryKeyFields, secondaryKeys, paramCount))
                            ? "base.RemoveByKeyPrefixWithoutIterate(writer.GetSpan())"
                            : "base.RemoveByPrimaryKeyPrefix(writer.GetSpan())";
                        declarations.Append(
                            $"            return {WrapCountResult(removeExpr, method.ResultType)};\n");
                    }
                    else
                    {
                        AppendWriterCtxIfNeeded(declarations, method.Parameters, null);
                        declarations.Append(
                            "            var writer = global::BTDB.StreamLayer.MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[512]);\n");
                        declarations.Append("            WriteRelationPKPrefix(ref writer);\n");
                        for (var i = 0; i < paramCount; i++)
                        {
                            AppendWriteOrderableParameter(declarations, method.Parameters[i]);
                        }

                        var throwWhenNotFound = method.ResultType == null ? "true" : "false";
                        var removeMethodName = method.Name.StartsWith("ShallowRemoveBy", StringComparison.Ordinal)
                            ? "ShallowRemoveById"
                            : "RemoveById";
                        declarations.Append(
                            $"            var removed = base.{removeMethodName}(writer.GetSpan(), {throwWhenNotFound});\n");
                        if (method.ResultType != null)
                        {
                            declarations.Append("            return removed;\n");
                        }
                    }
                }
            }
            else if (method.Name == "RemoveWithSizesById")
            {
                var paramCount = method.Parameters.Count;
                declarations.Append($"            var c_c = new ConstraintInfo[{paramCount}];\n");
                for (var i = 0; i < paramCount; i++)
                {
                    declarations.Append($"            c_c[{i}].Constraint = {method.Parameters[i].Name};\n");
                }

                declarations.Append("            return base.RemoveWithSizesByPrimaryKey(c_c);\n");
            }
            else if (method.Name.StartsWith("ListBy", StringComparison.Ordinal))
            {
                var paramCount = method.Parameters.Count;
                string? advParamType = null;
                var advType = string.Empty;
                var lastParam = paramCount > 0 ? method.Parameters[paramCount - 1] : null;
                var lastParamName = lastParam?.Name ?? "";
                var hasAdvancedEnumerator = paramCount > 0 &&
                                            TryGetAdvancedEnumeratorParamType(lastParam!.Type,
                                                out advType);
                if (hasAdvancedEnumerator)
                {
                    advParamType = advType;
                }

                var prefixParamCount = paramCount - (hasAdvancedEnumerator ? 1 : 0);
                var (indexName, _) = StripVariant(secondaryKeys, method.Name, false);
                var usesOrderedEnumerator = TryGetOrderedDictionaryEnumeratorTypes(method.ResultType, out var keyType,
                    out var valueType);
                var itemType = usesOrderedEnumerator
                    ? valueType
                    : ExtractEnumerableItemType(method.ResultType ?? "");
                var loaderIndex = FindLoaderIndex(generationInfo.Implements, itemType);

                AppendWriterCtxIfNeeded(declarations, method.Parameters.Take(prefixParamCount), advParamType);

                if (indexName == "Id")
                {
                    if (hasAdvancedEnumerator)
                    {
                        AppendAdvancedKeyPrefix(declarations, false, null, method.Parameters, prefixParamCount,
                            lastParamName, advParamType!);

                        if (usesOrderedEnumerator)
                        {
                            declarations.Append(
                                $"            return new global::BTDB.ODBLayer.RelationAdvancedOrderedEnumerator<{keyType}, {itemType}>(this, {lastParamName}.Order, {lastParamName}.StartProposition, prefixLen, startKeyBytes, {lastParamName}.EndProposition, endKeyBytes, {loaderIndex}, {prefixParamCount});\n");
                        }
                        else
                        {
                            declarations.Append(
                                $"            return new global::BTDB.ODBLayer.RelationAdvancedEnumerator<{itemType}>(this, {lastParamName}.Order, {lastParamName}.StartProposition, prefixLen, startKeyBytes, {lastParamName}.EndProposition, endKeyBytes, {loaderIndex});\n");
                        }
                    }
                    else
                    {
                        declarations.Append(
                            "            var writer = global::BTDB.StreamLayer.MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[512]);\n");
                        declarations.Append("            WriteRelationPKPrefix(ref writer);\n");
                        for (var i = 0; i < paramCount; i++)
                        {
                            AppendWriteOrderableParameter(declarations, method.Parameters[i]);
                        }

                        declarations.Append(
                            $"            return new global::BTDB.ODBLayer.RelationAdvancedEnumerator<{itemType}>(this, writer.GetSpan(), {loaderIndex});\n");
                    }
                }
                else
                {
                    var secondaryKeyIndex = FindSecondaryKeyIndex(secondaryKeys, indexName);
                    declarations.Append(
                        $"            var remappedSecondaryKeyIndex = RemapPrimeSK({secondaryKeyIndex}u);\n");
                    if (hasAdvancedEnumerator)
                    {
                        AppendAdvancedKeyPrefix(declarations, true, "remappedSecondaryKeyIndex", method.Parameters,
                            prefixParamCount, lastParamName, advParamType!);

                        if (usesOrderedEnumerator)
                        {
                            declarations.Append(
                                $"            return new global::BTDB.ODBLayer.RelationAdvancedOrderedSecondaryKeyEnumerator<{keyType}, {itemType}>(this, {lastParamName}.Order, {lastParamName}.StartProposition, prefixLen, startKeyBytes, {lastParamName}.EndProposition, endKeyBytes, remappedSecondaryKeyIndex, {loaderIndex}, {prefixParamCount});\n");
                        }
                        else
                        {
                            declarations.Append(
                                $"            return new global::BTDB.ODBLayer.RelationAdvancedSecondaryKeyEnumerator<{itemType}>(this, {lastParamName}.Order, {lastParamName}.StartProposition, prefixLen, startKeyBytes, {lastParamName}.EndProposition, endKeyBytes, remappedSecondaryKeyIndex, {loaderIndex});\n");
                        }
                    }
                    else
                    {
                        declarations.Append(
                            "            var writer = global::BTDB.StreamLayer.MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[512]);\n");
                        declarations.Append(
                            "            WriteRelationSKPrefix(ref writer, remappedSecondaryKeyIndex);\n");
                        for (var i = 0; i < paramCount; i++)
                        {
                            AppendWriteOrderableParameter(declarations, method.Parameters[i]);
                        }

                        declarations.Append(
                            $"            return new global::BTDB.ODBLayer.RelationAdvancedSecondaryKeyEnumerator<{itemType}>(this, writer.GetSpan(), remappedSecondaryKeyIndex, {loaderIndex});\n");
                    }
                }
            }
            else if (method.Name.StartsWith("AnyBy", StringComparison.Ordinal))
            {
                var paramCount = method.Parameters.Count;
                string? advParamType = null;
                var advType = string.Empty;
                var lastParam = paramCount > 0 ? method.Parameters[paramCount - 1] : null;
                var lastParamName = lastParam?.Name ?? "";
                var hasAdvancedEnumerator = paramCount > 0 &&
                                            TryGetAdvancedEnumeratorParamType(lastParam!.Type, out advType);
                if (hasAdvancedEnumerator)
                {
                    advParamType = advType;
                }

                var prefixParamCount = paramCount - (hasAdvancedEnumerator ? 1 : 0);
                var (indexName, _) = StripVariant(secondaryKeys, method.Name, false);

                AppendWriterCtxIfNeeded(declarations, method.Parameters.Take(prefixParamCount), advParamType);

                if (hasAdvancedEnumerator)
                {
                    if (indexName == "Id")
                    {
                        AppendAdvancedKeyPrefix(declarations, false, null, method.Parameters, prefixParamCount,
                            lastParamName, advParamType!);
                    }
                    else
                    {
                        var secondaryKeyIndex = FindSecondaryKeyIndex(secondaryKeys, indexName);
                        declarations.Append(
                            $"            var remappedSecondaryKeyIndex = RemapPrimeSK({secondaryKeyIndex}u);\n");
                        AppendAdvancedKeyPrefix(declarations, true, "remappedSecondaryKeyIndex", method.Parameters,
                            prefixParamCount, lastParamName, advParamType!);
                    }

                    declarations.Append(
                        $"            return base.AnyWithProposition({lastParamName}.StartProposition, prefixLen, startKeyBytes, {lastParamName}.EndProposition, endKeyBytes);\n");
                }
                else
                {
                    declarations.Append(
                        "            var writer = global::BTDB.StreamLayer.MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[512]);\n");
                    if (indexName == "Id")
                    {
                        declarations.Append("            WriteRelationPKPrefix(ref writer);\n");
                    }
                    else
                    {
                        var secondaryKeyIndex = FindSecondaryKeyIndex(secondaryKeys, indexName);
                        declarations.Append(
                            $"            var remappedSecondaryKeyIndex = RemapPrimeSK({secondaryKeyIndex}u);\n");
                        declarations.Append(
                            "            WriteRelationSKPrefix(ref writer, remappedSecondaryKeyIndex);\n");
                    }

                    for (var i = 0; i < paramCount; i++)
                    {
                        AppendWriteOrderableParameter(declarations, method.Parameters[i]);
                    }

                    declarations.Append("            return base.AnyWithPrefix(writer.GetSpan());\n");
                }
            }
            else if (method.Name.StartsWith("CountBy", StringComparison.Ordinal))
            {
                var paramCount = method.Parameters.Count;
                string? advParamType = null;
                var advType = string.Empty;
                var lastParam = paramCount > 0 ? method.Parameters[paramCount - 1] : null;
                var lastParamName = lastParam?.Name ?? "";
                var hasAdvancedEnumerator = paramCount > 0 &&
                                            TryGetAdvancedEnumeratorParamType(lastParam!.Type, out advType);
                if (hasAdvancedEnumerator)
                {
                    advParamType = advType;
                }

                var prefixParamCount = paramCount - (hasAdvancedEnumerator ? 1 : 0);
                var (indexName, _) = StripVariant(secondaryKeys, method.Name, false);

                AppendWriterCtxIfNeeded(declarations, method.Parameters.Take(prefixParamCount), advParamType);

                if (hasAdvancedEnumerator)
                {
                    if (indexName == "Id")
                    {
                        AppendAdvancedKeyPrefix(declarations, false, null, method.Parameters, prefixParamCount,
                            lastParamName, advParamType!);
                    }
                    else
                    {
                        var secondaryKeyIndex = FindSecondaryKeyIndex(secondaryKeys, indexName);
                        declarations.Append(
                            $"            var remappedSecondaryKeyIndex = RemapPrimeSK({secondaryKeyIndex}u);\n");
                        AppendAdvancedKeyPrefix(declarations, true, "remappedSecondaryKeyIndex", method.Parameters,
                            prefixParamCount, lastParamName, advParamType!);
                    }

                    var countExpr =
                        $"base.CountWithProposition({lastParamName}.StartProposition, prefixLen, startKeyBytes, {lastParamName}.EndProposition, endKeyBytes)";
                    declarations.Append($"            return {WrapCountResult(countExpr, method.ResultType)};\n");
                }
                else
                {
                    declarations.Append(
                        "            var writer = global::BTDB.StreamLayer.MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[512]);\n");
                    if (indexName == "Id")
                    {
                        declarations.Append("            WriteRelationPKPrefix(ref writer);\n");
                    }
                    else
                    {
                        var secondaryKeyIndex = FindSecondaryKeyIndex(secondaryKeys, indexName);
                        declarations.Append(
                            $"            var remappedSecondaryKeyIndex = RemapPrimeSK({secondaryKeyIndex}u);\n");
                        declarations.Append(
                            "            WriteRelationSKPrefix(ref writer, remappedSecondaryKeyIndex);\n");
                    }

                    for (var i = 0; i < paramCount; i++)
                    {
                        AppendWriteOrderableParameter(declarations, method.Parameters[i]);
                    }

                    var countExpr = "base.CountWithPrefix(writer.GetSpan())";
                    declarations.Append($"            return {WrapCountResult(countExpr, method.ResultType)};\n");
                }
            }
            else if (method.Name.StartsWith("GatherBy", StringComparison.Ordinal))
            {
                var paramCount = method.Parameters.Count;
                var hasOrderers = paramCount > 0 && IsOrdererArrayType(method.Parameters[paramCount - 1].Type);
                var constraintCount = paramCount - 3 - (hasOrderers ? 1 : 0);
                var itemType = ExtractCollectionItemType(method.Parameters[0].Type);
                var loaderIndex = FindLoaderIndex(generationInfo.Implements, itemType);
                var (indexName, _) = StripVariant(secondaryKeys, method.Name, false);

                declarations.Append($"            var c_c = new ConstraintInfo[{constraintCount}];\n");
                for (var i = 0; i < constraintCount; i++)
                {
                    declarations.Append($"            c_c[{i}].Constraint = {method.Parameters[i + 3].Name};\n");
                }

                var orderersArg = hasOrderers ? method.Parameters[paramCount - 1].Name : "null";
                if (indexName == "Id")
                {
                    declarations.Append(
                        $"            return GatherByPrimaryKey({loaderIndex}, c_c, {method.Parameters[0].Name}, {method.Parameters[1].Name}, {method.Parameters[2].Name}, {orderersArg});\n");
                }
                else
                {
                    var secondaryKeyIndex = FindSecondaryKeyIndex(secondaryKeys, indexName);
                    declarations.Append(
                        $"            return GatherBySecondaryKey({loaderIndex}, c_c, {method.Parameters[0].Name}, {method.Parameters[1].Name}, {method.Parameters[2].Name}, {secondaryKeyIndex}u, {orderersArg});\n");
                }
            }
            else if (method.Name.StartsWith("ScanBy", StringComparison.Ordinal))
            {
                var paramCount = method.Parameters.Count;
                var itemType = ExtractEnumerableItemType(method.ResultType ?? "");
                var loaderIndex = FindLoaderIndex(generationInfo.Implements, itemType);
                var (indexName, _) = StripVariant(secondaryKeys, method.Name, false);

                declarations.Append($"            var c_c = new ConstraintInfo[{paramCount}];\n");
                for (var i = 0; i < paramCount; i++)
                {
                    declarations.Append($"            c_c[{i}].Constraint = {method.Parameters[i].Name};\n");
                }

                if (indexName == "Id")
                {
                    declarations.Append(
                        $"            return ScanByPrimaryKeyPrefix<{itemType}>({loaderIndex}, c_c);\n");
                }
                else
                {
                    var secondaryKeyIndex = FindSecondaryKeyIndex(secondaryKeys, indexName);
                    declarations.Append(
                        $"            return ScanBySecondaryKeyPrefix<{itemType}>({loaderIndex}, c_c, {secondaryKeyIndex}u);\n");
                }
            }
            else if (method.Name.StartsWith("FirstBy", StringComparison.Ordinal))
            {
                var paramCount = method.Parameters.Count;
                var hasOrderers = paramCount > 0 && IsOrdererArrayType(method.Parameters[paramCount - 1].Type);
                var constraintCount = paramCount - (hasOrderers ? 1 : 0);
                var itemType = NormalizeType(method.ResultType ?? "");
                var loaderIndex = FindLoaderIndex(generationInfo.Implements, itemType);
                var (indexName, hasOrDefault) = StripVariant(secondaryKeys, method.Name, true);

                declarations.Append($"            var c_c = new ConstraintInfo[{constraintCount}];\n");
                for (var i = 0; i < constraintCount; i++)
                {
                    declarations.Append($"            c_c[{i}].Constraint = {method.Parameters[i].Name};\n");
                }

                var orderersArg = hasOrderers ? method.Parameters[paramCount - 1].Name : "null";
                var hasOrDefaultLiteral = hasOrDefault ? "true" : "false";
                if (indexName == "Id")
                {
                    declarations.Append(
                        $"            return FirstByPrimaryKey<{itemType}>({loaderIndex}, c_c, null, {orderersArg}, {hasOrDefaultLiteral});\n");
                }
                else
                {
                    var secondaryKeyIndex = FindSecondaryKeyIndex(secondaryKeys, indexName);
                    declarations.Append(
                        $"            return FirstBySecondaryKey<{itemType}>({loaderIndex}, c_c, {secondaryKeyIndex}u, {orderersArg}, {hasOrDefaultLiteral});\n");
                }
            }
            else
            {
                declarations.Append("            throw new NotImplementedException();\n");
            }

            // language=C#
            declarations.Append("""
                        }

                """);
        }

        var constructorDeclaration = new StringBuilder();
        constructorDeclaration.Append(
            $"\n        public {implName}(IObjectDBTransaction transaction, RelationInfo relationInfo) : base(transaction, relationInfo)\n");
        constructorDeclaration.Append("        {\n");
        constructorDeclaration.Append(constructorBody);
        constructorDeclaration.Append("        }\n");
        declarations.Insert(classBodyStart, constructorDeclaration);
        declarations.Append("    }\n");
        if (needsRelationInfoAccessors)
        {
            // language=c#
            declarations.Append("""
                    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_relationInfoResolver")]
                    extern static ref global::BTDB.ODBLayer.IRelationInfoResolver RelationInfoResolverAccessor(global::BTDB.ODBLayer.RelationInfo @this);

                    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_ClientRelationVersionInfo")]
                    extern static global::BTDB.ODBLayer.RelationVersionInfo ClientRelationVersionInfoAccessor(global::BTDB.ODBLayer.RelationInfo @this);

                """);
        }

        code.Append(declarations);

        var relationCreatorArgument = new StringBuilder();
        if (creatorValidationBody.Length == 0)
        {
            relationCreatorArgument.Append(
                $"            info => {{ return transaction => new {implName}(transaction, info); }},\n");
        }
        else
        {
            relationCreatorArgument.Append("            relationInfo =>\n");
            relationCreatorArgument.Append("            {\n");
            relationCreatorArgument.Append(creatorValidationBody);
            relationCreatorArgument.Append(
                $"                return transaction => new {implName}(transaction, relationInfo);\n");
            relationCreatorArgument.Append("            },\n");
        }

        // language=c#
        code.Append("    [ModuleInitializer]\n");
        code.Append("    internal static unsafe void Register4BTDB()\n");
        code.Append("    {\n");
        code.Append(
            $"        BTDB.Serialization.ReflectionMetadata.RegisterRelation(typeof({generationInfo.FullName}),\n");
        code.Append(relationCreatorArgument);
        code.Append(
            $"            [{string.Join(", ", generationInfo.Implements.Select(f => $"typeof({NormalizeType(f.FullyQualifiedName)})"))}]);\n");
        code.Append("    }\n");
        code.Append("}\n");

        context.AddSource(
            $"{generationInfo.FullName.Replace("global::", "").Replace("<", "[").Replace(">", "]")}.g.cs",
            SourceText.From(code.ToString(), Encoding.UTF8));
    }

    static string NormalizeType(string type)
    {
        if (type == "dynamic") return "object";
        return type;
    }

    static string? GetEnumUnderlyingType(ITypeSymbol typeSymbol)
    {
        if (typeSymbol is INamedTypeSymbol
            {
                TypeKind: TypeKind.Struct,
                OriginalDefinition.SpecialType: SpecialType.System_Nullable_T
            } nullableType &&
            nullableType.TypeArguments.Length == 1 &&
            nullableType.TypeArguments[0] is INamedTypeSymbol { TypeKind: TypeKind.Enum } nullableEnum &&
            nullableEnum.EnumUnderlyingType != null)
        {
            return nullableEnum.EnumUnderlyingType.ToDisplayString();
        }

        if (typeSymbol is INamedTypeSymbol { TypeKind: TypeKind.Enum } enumType &&
            enumType.EnumUnderlyingType != null)
        {
            return enumType.EnumUnderlyingType.ToDisplayString();
        }

        return null;
    }

    static bool IsOrdererArrayType(string type)
    {
        return type == "BTDB.ODBLayer.IOrderer[]";
    }

    static string ExtractCollectionItemType(string collectionType)
    {
        var start = collectionType.IndexOf('<');
        var end = collectionType.LastIndexOf('>');
        if (start < 0 || end <= start) return collectionType;
        var typeArg = collectionType.Substring(start + 1, end - start - 1).Trim();
        return typeArg.Replace("global::", "");
    }

    static string ExtractEnumerableItemType(string enumerableType)
    {
        if (string.IsNullOrWhiteSpace(enumerableType)) return enumerableType;
        var start = enumerableType.IndexOf('<');
        var end = enumerableType.LastIndexOf('>');
        if (start < 0 || end <= start) return enumerableType;
        var typeArg = enumerableType.Substring(start + 1, end - start - 1).Trim();
        return typeArg;
    }

    static bool IsEnumerableType(string? enumerableType)
    {
        if (enumerableType is null) return false;
        var normalized = enumerableType.StartsWith("global::", StringComparison.Ordinal)
            ? enumerableType.Substring("global::".Length)
            : enumerableType;
        return normalized.StartsWith("System.Collections.Generic.IEnumerable<", StringComparison.Ordinal);
    }

    static bool IsRemoveByCountReturnType(string? resultType)
    {
        if (resultType is null) return false;
        return NormalizeIntegralType(resultType) is IntegralType.Int32 or IntegralType.UInt32 or IntegralType.Int64
            or IntegralType.UInt64;
    }

    static bool HasOnBeforeRemove(GenerationInfo generationInfo)
    {
        return generationInfo.Nested[0].Methods.Any(m => m.Purpose == Purpose.OnBeforeRemove);
    }

    static bool AllKeyPrefixesAreSame(uint[] primaryKeyFields,
        (string Name, uint[] SecondaryKeyFields, uint ExplicitPrefixLength)[] secondaryKeys, int count)
    {
        for (var i = 0; i < secondaryKeys.Length; i++)
        {
            var skFields = secondaryKeys[i].SecondaryKeyFields;
            if (skFields.Length < count)
            {
                return false;
            }

            for (var idx = 0; idx < count; idx++)
            {
                if (skFields[idx] != primaryKeyFields[idx])
                {
                    return false;
                }
            }
        }

        return true;
    }

    static int FindLoaderIndex(EquatableArray<TypeRef> loadTypes, string itemType)
    {
        var normalizedItemType = NormalizeType(itemType);
        var normalizedItemTypeNoGlobal = normalizedItemType.Replace("global::", "");
        var usesNamespace = normalizedItemTypeNoGlobal.IndexOf('.') >= 0;
        for (var i = 0; i < loadTypes.Count; i++)
        {
            var typeRef = loadTypes[i];
            var candidate = NormalizeType(typeRef.FullyQualifiedName);
            if (string.Equals(candidate, normalizedItemType, StringComparison.Ordinal) ||
                string.Equals(candidate.Replace("global::", ""), normalizedItemTypeNoGlobal,
                    StringComparison.Ordinal))
                return i;
            if (!usesNamespace && string.Equals(typeRef.Name, normalizedItemTypeNoGlobal, StringComparison.Ordinal))
                return i;
        }

        return -1;
    }

    static bool ContainsEncryptedStringType(string type)
    {
        var normalized = NormalizeType(type).Replace("global::", "");
        if (normalized == "BTDB.Encrypted.EncryptedString") return true;

        var nullableUnderlying = TryGetNullableUnderlyingType(normalized);
        if (nullableUnderlying != null && ContainsEncryptedStringType(nullableUnderlying))
        {
            return true;
        }

        if (TryGetTupleElementTypes(normalized, out var elementTypes))
        {
            for (var i = 0; i < elementTypes.Length; i++)
            {
                if (ContainsEncryptedStringType(elementTypes[i]))
                {
                    return true;
                }
            }
        }

        return false;
    }

    static bool NeedsWriterCtx(IEnumerable<ParameterInfo> parameters, string? extraType)
    {
        if (parameters.Any(p => ContainsEncryptedStringType(p.Type)))
        {
            return true;
        }

        return extraType != null && ContainsEncryptedStringType(extraType);
    }

    static void AppendWriterCtxIfNeeded(StringBuilder declarations, IEnumerable<ParameterInfo> parameters,
        string? extraType)
    {
        if (!NeedsWriterCtx(parameters, extraType))
        {
            return;
        }

        declarations.Append("            var ctx_ctx = new global::BTDB.ODBLayer.DBWriterCtx(Transaction);\n");
    }

    static void AppendWriteOrderableParameter(StringBuilder declarations, ParameterInfo parameter)
    {
        AppendWriteOrderableValue(declarations, parameter.Name, parameter.Type, parameter.EnumUnderlyingType,
            "            ");
    }

    static void AppendWriteOrderableValue(StringBuilder declarations, string valueExpression, string valueType,
        string? enumUnderlyingType = null, string indent = "            ")
    {
        var nullableUnderlyingType = TryGetNullableUnderlyingType(valueType);
        if (nullableUnderlyingType != null)
        {
            declarations.Append($"{indent}if (!{valueExpression}.HasValue)\n");
            declarations.Append($"{indent}{{\n");
            declarations.Append($"{indent}    writer.WriteBool(false);\n");
            declarations.Append($"{indent}}}\n");
            declarations.Append($"{indent}else\n");
            declarations.Append($"{indent}{{\n");
            declarations.Append($"{indent}    writer.WriteBool(true);\n");
            AppendWriteOrderableValue(declarations, $"{valueExpression}.Value", nullableUnderlyingType,
                enumUnderlyingType, indent + "    ");
            declarations.Append($"{indent}}}\n");
            return;
        }

        if (enumUnderlyingType != null)
        {
            var normalizedUnderlying = NormalizeType(enumUnderlyingType);
            switch (normalizedUnderlying)
            {
                case "sbyte":
                case "short":
                case "int":
                case "long":
                    declarations.Append($"{indent}writer.WriteVInt64((long){valueExpression});\n");
                    return;
                case "byte":
                case "ushort":
                case "uint":
                case "ulong":
                    declarations.Append($"{indent}writer.WriteVUInt64((ulong){valueExpression});\n");
                    return;
                default:
                    declarations.Append(
                        $"{indent}throw new NotSupportedException(\"Key does not support enum type '{normalizedUnderlying}'.\");\n");
                    return;
            }
        }

        var normalizedType = NormalizeType(valueType);
        if (TryGetTupleElementTypes(normalizedType, out var tupleElementTypes))
        {
            for (var i = 0; i < tupleElementTypes.Length; i++)
            {
                AppendWriteOrderableValue(declarations, $"{valueExpression}.Item{i + 1}", tupleElementTypes[i], null,
                    indent);
            }

            return;
        }

        switch (normalizedType)
        {
            case "string":
                declarations.Append($"{indent}writer.WriteStringOrdered({valueExpression});\n");
                return;
            case "bool":
                declarations.Append($"{indent}writer.WriteBool({valueExpression});\n");
                return;
            case "byte":
                declarations.Append($"{indent}writer.WriteUInt8({valueExpression});\n");
                return;
            case "sbyte":
                declarations.Append($"{indent}writer.WriteInt8Ordered({valueExpression});\n");
                return;
            case "short":
            case "int":
            case "long":
                declarations.Append($"{indent}writer.WriteVInt64({valueExpression});\n");
                return;
            case "ushort":
            case "uint":
            case "ulong":
                declarations.Append($"{indent}writer.WriteVUInt64({valueExpression});\n");
                return;
            case "System.DateTime":
                declarations.Append($"{indent}writer.WriteDateTimeForbidUnspecifiedKind({valueExpression});\n");
                return;
            case "System.DateTimeOffset":
                declarations.Append($"{indent}writer.WriteDateTimeOffset({valueExpression});\n");
                return;
            case "System.TimeSpan":
                declarations.Append($"{indent}writer.WriteTimeSpan({valueExpression});\n");
                return;
            case "System.Guid":
                declarations.Append($"{indent}writer.WriteGuid({valueExpression});\n");
                return;
            case "System.Decimal":
                declarations.Append($"{indent}writer.WriteDecimal({valueExpression});\n");
                return;
            case "System.Net.IPAddress":
                declarations.Append($"{indent}writer.WriteIPAddress({valueExpression});\n");
                return;
            case "System.Collections.Generic.List<string>":
                declarations.Append(
                    $"{indent}if ({valueExpression} == null) writer.WriteVUInt32(0); else {{ writer.WriteVUInt32((uint){valueExpression}.Count); foreach (var item in {valueExpression}) writer.WriteString(item); }}\n");
                return;
            case "System.Collections.Generic.List<ulong>":
                declarations.Append(
                    $"{indent}if ({valueExpression} == null) writer.WriteVUInt32(0); else {{ writer.WriteVUInt32((uint){valueExpression}.Count); foreach (var item in {valueExpression}) writer.WriteVUInt64(item); }}\n");
                return;
            case "byte[]":
            case "BTDB.Buffer.ByteBuffer":
            case "System.ReadOnlyMemory<byte>":
                declarations.Append($"{indent}writer.WriteByteArray({valueExpression});\n");
                return;
            case "System.Version":
                declarations.Append($"{indent}writer.WriteVersion({valueExpression});\n");
                return;
            case "BTDB.Encrypted.EncryptedString":
                declarations.Append(
                    $"{indent}ctx_ctx.WriteOrderedEncryptedString(ref writer, {valueExpression});\n");
                return;
            case "Microsoft.Extensions.Primitives.StringValues":
                declarations.Append($"{indent}writer.WriteStringValues({valueExpression});\n");
                return;
            default:
                declarations.Append(
                    $"{indent}throw new NotSupportedException(\"Key does not support type '{normalizedType}'.\");\n");
                return;
        }
    }

    static bool TryGetTupleElementTypes(string type, out string[] elementTypes)
    {
        var normalized = type.Replace("global::", "");
        if (normalized.StartsWith("System.Tuple<", StringComparison.Ordinal) ||
            normalized.StartsWith("Tuple<", StringComparison.Ordinal) ||
            normalized.StartsWith("System.ValueTuple<", StringComparison.Ordinal) ||
            normalized.StartsWith("ValueTuple<", StringComparison.Ordinal))
        {
            var start = normalized.IndexOf('<');
            var end = normalized.LastIndexOf('>');
            if (start >= 0 && end > start)
            {
                var args = normalized.Substring(start + 1, end - start - 1);
                elementTypes = SplitArguments(args)
                    .Select(arg => RemoveTupleElementName(arg.Replace("global::", "")))
                    .ToArray();
                if (elementTypes.Length is 0 or > 7)
                {
                    elementTypes = Array.Empty<string>();
                    return false;
                }

                return true;
            }
        }

        if (normalized.StartsWith("(", StringComparison.Ordinal) &&
            normalized.EndsWith(")", StringComparison.Ordinal))
        {
            var inner = normalized.Substring(1, normalized.Length - 2);
            elementTypes = SplitArguments(inner)
                .Select(arg => RemoveTupleElementName(arg.Replace("global::", "")))
                .ToArray();
            if (elementTypes.Length is 0 or > 7)
            {
                elementTypes = Array.Empty<string>();
                return false;
            }

            return true;
        }

        elementTypes = Array.Empty<string>();
        return false;
    }

    static string[] SplitArguments(string args)
    {
        var parts = new List<string>();
        var angleDepth = 0;
        var parenDepth = 0;
        var start = 0;
        for (var i = 0; i < args.Length; i++)
        {
            var ch = args[i];
            switch (ch)
            {
                case '<':
                    angleDepth++;
                    break;
                case '>':
                    angleDepth--;
                    break;
                case '(':
                    parenDepth++;
                    break;
                case ')':
                    parenDepth--;
                    break;
                case ',':
                    if (angleDepth == 0 && parenDepth == 0)
                    {
                        parts.Add(args.Substring(start, i - start));
                        start = i + 1;
                    }

                    break;
            }
        }

        parts.Add(args.Substring(start));
        return parts.ToArray();
    }

    static string RemoveTupleElementName(string element)
    {
        var trimmed = element.Trim();
        var depth = 0;
        for (var i = trimmed.Length - 1; i >= 0; i--)
        {
            var ch = trimmed[i];
            switch (ch)
            {
                case '>':
                    depth++;
                    break;
                case '<':
                    depth--;
                    break;
            }

            if (depth == 0 && char.IsWhiteSpace(ch))
            {
                var before = trimmed.Substring(0, i).Trim();
                var after = trimmed.Substring(i).Trim();
                if (IsIdentifier(after))
                {
                    return before;
                }
            }
        }

        return trimmed;
    }

    static bool IsIdentifier(string value)
    {
        if (string.IsNullOrEmpty(value)) return false;
        var start = value[0] == '@' ? 1 : 0;
        if (start >= value.Length) return false;
        if (!IsIdentifierStart(value[start])) return false;
        for (var i = start + 1; i < value.Length; i++)
        {
            if (!IsIdentifierPart(value[i])) return false;
        }

        return true;
    }

    static bool IsIdentifierStart(char ch)
    {
        return ch == '_' || char.IsLetter(ch);
    }

    static bool IsIdentifierPart(char ch)
    {
        return ch == '_' || char.IsLetterOrDigit(ch);
    }

    static string? TryGetNullableUnderlyingType(string valueType)
    {
        if (valueType.EndsWith("?", StringComparison.Ordinal))
        {
            return valueType.Substring(0, valueType.Length - 1);
        }

        const string prefix = "System.Nullable<";
        if (valueType.StartsWith(prefix, StringComparison.Ordinal))
        {
            var start = valueType.IndexOf('<');
            var end = valueType.LastIndexOf('>');
            if (start >= 0 && end > start)
            {
                return valueType.Substring(start + 1, end - start - 1).Trim();
            }
        }

        return null;
    }

    static void AppendAdvancedKeyPrefix(StringBuilder declarations, bool useSecondaryKey,
        string? remappedSecondaryKeyIndexVar, EquatableArray<ParameterInfo> parameters, int prefixParamCount,
        string advParamName, string advParamType)
    {
        declarations.Append(
            "            var writer = global::BTDB.StreamLayer.MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[512]);\n");
        if (useSecondaryKey)
        {
            declarations.Append($"            WriteRelationSKPrefix(ref writer, {remappedSecondaryKeyIndexVar});\n");
        }
        else
        {
            declarations.Append("            WriteRelationPKPrefix(ref writer);\n");
        }

        for (var i = 0; i < prefixParamCount; i++)
        {
            AppendWriteOrderableParameter(declarations, parameters[i]);
        }

        declarations.Append("            var prefixLen = (int)writer.GetCurrentPosition();\n");
        declarations.Append(
            $"            if ({advParamName}.StartProposition != global::BTDB.ODBLayer.KeyProposition.Ignored)\n");
        declarations.Append("            {\n");
        AppendWriteOrderableValue(declarations, $"{advParamName}.Start", advParamType, null, "                ");
        declarations.Append("            }\n");
        declarations.Append("            var startKeyBytes = writer.GetScopedSpanAndReset();\n");

        if (useSecondaryKey)
        {
            declarations.Append($"            WriteRelationSKPrefix(ref writer, {remappedSecondaryKeyIndexVar});\n");
        }
        else
        {
            declarations.Append("            WriteRelationPKPrefix(ref writer);\n");
        }

        for (var i = 0; i < prefixParamCount; i++)
        {
            AppendWriteOrderableParameter(declarations, parameters[i]);
        }

        declarations.Append(
            $"            if ({advParamName}.EndProposition != global::BTDB.ODBLayer.KeyProposition.Ignored)\n");
        declarations.Append("            {\n");
        AppendWriteOrderableValue(declarations, $"{advParamName}.End", advParamType, null, "                ");
        declarations.Append("            }\n");
        declarations.Append("            var endKeyBytes = writer.GetSpan();\n");
    }

    static string WrapCountResult(string expression, string? resultType)
    {
        if (resultType is null)
        {
            return expression;
        }

        var normalized = NormalizeIntegralType(resultType);
        return normalized switch
        {
            IntegralType.Int32 => $"(int){expression}",
            IntegralType.UInt32 => $"(uint){expression}",
            IntegralType.UInt64 => $"(ulong){expression}",
            _ => expression
        };
    }

    static bool TryGetAdvancedEnumeratorParamType(string type, out string genericType)
    {
        var normalized = type.Replace("global::", "");
        const string prefix = "BTDB.ODBLayer.AdvancedEnumeratorParam<";
        const string shortPrefix = "AdvancedEnumeratorParam<";
        if (normalized.StartsWith(prefix, StringComparison.Ordinal) ||
            normalized.StartsWith(shortPrefix, StringComparison.Ordinal))
        {
            var start = normalized.IndexOf('<');
            var end = normalized.LastIndexOf('>');
            if (start >= 0 && end > start)
            {
                genericType = normalized.Substring(start + 1, end - start - 1).Trim();
                return true;
            }
        }

        genericType = "";
        return false;
    }

    static bool TryGetOrderedDictionaryEnumeratorTypes(string? type, out string keyType, out string valueType)
    {
        keyType = "";
        valueType = "";
        if (string.IsNullOrWhiteSpace(type))
        {
            return false;
        }

        var normalized = type!.Replace("global::", "");
        const string prefix = "BTDB.ODBLayer.IOrderedDictionaryEnumerator<";
        const string shortPrefix = "IOrderedDictionaryEnumerator<";
        if (!normalized.StartsWith(prefix, StringComparison.Ordinal) &&
            !normalized.StartsWith(shortPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        var start = normalized.IndexOf('<');
        var end = normalized.LastIndexOf('>');
        if (start < 0 || end <= start)
        {
            return false;
        }

        var args = normalized.Substring(start + 1, end - start - 1);
        var split = SplitArguments(args);
        if (split.Length != 2)
        {
            return false;
        }

        keyType = split[0].Trim();
        valueType = split[1].Trim();
        return true;
    }

    static bool IsFloatOrDoubleType(string type)
    {
        var normalized = NormalizeType(type);
        if (normalized is "float" or "double")
        {
            return true;
        }

        return false;
    }

    static bool IsBoolType(string type)
    {
        var normalized = NormalizeType(type).Replace("global::", "");
        return normalized is "bool" or "System.Boolean";
    }

    static uint FindSecondaryKeyIndex(
        (string Name, uint[] SecondaryKeyFields, uint ExplicitPrefixLength)[] secondaryKeys,
        string indexName)
    {
        for (var i = 0; i < secondaryKeys.Length; i++)
        {
            if (secondaryKeys[i].Name == indexName)
                return (uint)i;
        }

        return 0;
    }
}

enum GenerationType
{
    Class,
    Delegate,
    Interface,
    Struct,
    RelationIface,
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
    bool ForceMetadata,
    bool IsRelationItem,
    EquatableArray<ParameterInfo>? ConstructorParameters,
    EquatableArray<PropertyInfo> Properties,
    EquatableArray<string> ParentDeclarations,
    EquatableArray<DispatcherInfo> Dispatchers,
    EquatableArray<FieldsInfo> Fields,
    EquatableArray<MethodInfo> Methods,
    EquatableArray<TypeRef> Implements,
    EquatableArray<CollectionInfo> CollectionInfos,
    EquatableArray<GenericParameter> GenericParameters,
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

record GenericParameter(
    string Name,
    TypeRef Value,
    bool IsClassConstraint,
    bool IsStructConstraint,
    bool IsNewConstraint,
    EquatableArray<TypeRef> SpecificTypeConstraints);

record ParameterInfo(
    string Name,
    string Type,
    string? KeyCode,
    bool IsReference,
    bool Optional,
    string? DefaultValue,
    string? EnumUnderlyingType = null);

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
    string GenericType,
    string? StoredName,
    bool IsReference,
    string? BackingName,
    string? GetterName,
    string? SetterName,
    bool ReadOnly,
    string OwnerFullName,
    EquatableArray<IndexInfo> Indexes);

enum Purpose
{
    None = 0,
    OnSerialize = 1,
    OnBeforeRemove = 2,
}

record MethodInfo(
    string Name,
    string? ResultType,
    EquatableArray<ParameterInfo> Parameters,
    bool IsPublic,
    string? DefinedInType,
    Purpose Purpose = Purpose.None);

// Name == null for primary key, InKeyValue could be true only for primary key, IncludePrimaryKeyOrder is used only for secondary key
record IndexInfo(string? Name, uint Order, bool InKeyValue, uint IncludePrimaryKeyOrder);

record DispatcherInfo(string Name, string? Type, string? ResultType, string IfaceName);
