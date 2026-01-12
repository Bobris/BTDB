using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;

namespace BTDB.SourceGenerator;

public static class Extensions
{
    extension(ISymbol symbol)
    {
        public IEnumerable<AttributeData> GetAllAttributes()
        {
            // attributes declared on the symbol itself
            foreach (var attribute in symbol.GetAttributes())
                yield return attribute;

            // attributes declared on base types
            for (var current = symbol.ContainingType.BaseType; current != null; current = current.BaseType)
            {
                foreach (var attribute in current.GetAttributes())
                    yield return attribute;
            }

            // attributes declared on interfaces
            var seen = new HashSet<INamedTypeSymbol>(SymbolEqualityComparer.Default);
            foreach (var iface in symbol.ContainingType.AllInterfaces)
            {
                if (!seen.Add(iface)) continue;
                foreach (var attribute in iface.GetAttributes())
                    yield return attribute;
            }
        }

        public bool InBTDBNamespace() => symbol.InNamespace("BTDB");
        public bool InBTDBIOCNamespace() => symbol.InNamespace("BTDB", "IOC");
        public bool InODBLayerNamespace() => symbol.InNamespace("BTDB", "ODBLayer");
    }

    // this cannot use C# 14 feature because of bug https://github.com/dotnet/roslyn/issues/80024
    public static bool InNamespace(this ISymbol symbol, params ReadOnlySpan<string> namespaces)
    {
        if (symbol.ContainingNamespace is null) return false;
        var cs = symbol.ContainingNamespace;
        var index = namespaces.Length - 1;
        while (true)
        {
            switch (cs.IsGlobalNamespace)
            {
                case true when index < 0:
                    return true;
                case true:
                case false when index < 0:
                    return false;
            }

            if (cs.Name != namespaces[index]) return false;
            cs = cs.ContainingNamespace;
            index--;
        }
    }
}

/// <summary>
/// Extension methods for type analysis and pattern matching
/// </summary>
public static class TypeAnalysisExtensions
{
    extension(ITypeSymbol type)
    {
        /// <summary>
        /// Checks if a type is IEnumerable&lt;T&gt; where T is a class type
        /// </summary>
        public bool IsIEnumerableOfTWhereTIsClass()
        {
            return type is INamedTypeSymbol
            {
                TypeKind: TypeKind.Interface,
                OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_IEnumerable_T
            } namedType && namedType.TypeArguments.FirstOrDefault()?.TypeKind == TypeKind.Class;
        }

        /// <summary>
        /// Checks if a type is IOrderedDictionaryEnumerator&lt;TKey, TValue&gt; with matching key type
        /// </summary>
        public bool IsIOrderedDictionaryEnumerator(ITypeSymbol expectedKeyType)
        {
            return type is INamedTypeSymbol
                   {
                       TypeKind: TypeKind.Interface,
                       OriginalDefinition.Name: "IOrderedDictionaryEnumerator"
                   } named &&
                   named.OriginalDefinition.InODBLayerNamespace() &&
                   named.TypeArguments.Length == 2 &&
                   SymbolEqualityComparer.Default.Equals(named.TypeArguments[0], expectedKeyType);
        }

        /// <summary>
        /// Checks if a type is IEnumerator&lt;T&gt;
        /// </summary>
        public bool IsIEnumeratorOfT()
        {
            return type is INamedTypeSymbol
            {
                TypeKind: TypeKind.Interface,
                OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_IEnumerator_T
            };
        }

        /// <summary>
        /// Checks if a type is ICollection&lt;T&gt;
        /// </summary>
        public bool IsICollectionOfT()
        {
            return type is INamedTypeSymbol
            {
                TypeKind: TypeKind.Interface,
                OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_ICollection_T
            };
        }

        /// <summary>
        /// Checks if a type is compatible with list field handlers (IList&lt;T&gt; or ISet&lt;T&gt;)
        /// </summary>
        public bool IsListFieldHandlerCompatible()
        {
            if (type is not INamedTypeSymbol namedType || !namedType.IsGenericType)
                return false;

            if (namedType.ConstructedFrom.IsListOrSetType())
                return true;

            return namedType.AllInterfaces.Any(iface =>
                iface.IsGenericType && iface.ConstructedFrom.IsListOrSetType());
        }

        /// <summary>
        /// Checks if a type is compatible with dictionary field handlers
        /// </summary>
        public bool IsDictionaryFieldHandlerCompatible()
        {
            if (type is not INamedTypeSymbol { IsGenericType: true } namedType)
                return false;

            var constructedType = namedType.ConstructedFrom.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            return constructedType is "global::System.Collections.Generic.IDictionary<TKey, TValue>"
                or "global::System.Collections.Generic.Dictionary<TKey, TValue>";
        }
    }

    /// <summary>
    /// Checks if a type is IList&lt;T&gt; or ISet&lt;T&gt;
    /// </summary>
    extension(INamedTypeSymbol type)
    {
        public bool IsListOrSetType()
        {
            var typeName = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            return typeName is "global::System.Collections.Generic.IList<T>"
                or "global::System.Collections.Generic.ISet<T>";
        }
    }

    extension(ITypeSymbol type)
    {
        /// <summary>
        /// Checks if a type represents a numeric return type (int, uint, long, ulong)
        /// </summary>
        public bool IsNumericReturnType()
        {
            return type.SpecialType is SpecialType.System_Int32 or
                SpecialType.System_UInt32 or
                SpecialType.System_Int64 or
                SpecialType.System_UInt64;
        }

        /// <summary>
        /// Checks if a type represents a valid return type for removal operations
        /// </summary>
        public bool IsValidRemoveReturnType()
        {
            return type.SpecialType is SpecialType.System_Void or
                SpecialType.System_Boolean or
                SpecialType.System_Int32 or
                SpecialType.System_UInt32 or
                SpecialType.System_Int64 or
                SpecialType.System_UInt64;
        }

        /// <summary>
        /// Checks if a type represents a boolean type
        /// </summary>
        public bool IsBooleanType()
        {
            return type.SpecialType == SpecialType.System_Boolean;
        }

        /// <summary>
        /// Checks if a type represents a void type
        /// </summary>
        public bool IsVoidType()
        {
            return type.SpecialType == SpecialType.System_Void;
        }

        /// <summary>
        /// Checks if a type represents an unsigned 64-bit integer
        /// </summary>
        public bool IsUInt64Type()
        {
            return type.SpecialType == SpecialType.System_UInt64;
        }

        /// <summary>
        /// Checks if a type represents a signed 64-bit integer
        /// </summary>
        public bool IsInt64Type()
        {
            return type.SpecialType == SpecialType.System_Int64;
        }

        /// <summary>
        /// Checks if a type represents a string type
        /// </summary>
        public bool IsStringType()
        {
            return type.SpecialType == SpecialType.System_String;
        }
    }

    extension(ITypeSymbol type)
    {
        /// <summary>
        /// Checks if a type is a tuple with a specific number of elements
        /// </summary>
        public bool IsTupleWithElementCount(int expectedCount)
        {
            return type is INamedTypeSymbol { IsTupleType: true } namedType &&
                   namedType.TupleElements.Length == expectedCount;
        }

        /// <summary>
        /// Checks if a type is a 3-element tuple where all elements are UInt64
        /// </summary>
        public bool IsUInt64TripleTuple()
        {
            return type.IsTupleWithElementCount(3) &&
                   type is INamedTypeSymbol namedType &&
                   namedType.TupleElements.All(e => e.Type.SpecialType == SpecialType.System_UInt64);
        }
    }

    extension(ITypeSymbol type)
    {
        /// <summary>
        /// Gets the first type argument from a generic type
        /// </summary>
        public ITypeSymbol? GetFirstTypeArgument()
        {
            return type is INamedTypeSymbol { IsGenericType: true } namedType
                ? namedType.TypeArguments.FirstOrDefault()
                : null;
        }

        /// <summary>
        /// Gets the type arguments from a generic type
        /// </summary>
        public IEnumerable<ITypeSymbol> GetTypeArguments()
        {
            return type is INamedTypeSymbol { IsGenericType: true } namedType
                ? namedType.TypeArguments
                : Enumerable.Empty<ITypeSymbol>();
        }

        /// <summary>
        /// Checks if a generic type matches a specific generic type definition
        /// </summary>
        public bool IsGenericTypeOf(string genericTypeName)
        {
            return type is INamedTypeSymbol namedType &&
                   namedType.OriginalDefinition.Name == genericTypeName &&
                   namedType.IsGenericType;
        }
    }

    extension(ITypeSymbol type)
    {
        /// <summary>
        /// Checks if a type is an array with a specific element type
        /// </summary>
        public bool IsArrayOf(string elementTypeName)
        {
            return type is IArrayTypeSymbol arrayType &&
                   arrayType.ElementType.Name == elementTypeName;
        }

        /// <summary>
        /// Checks if a type is an array of IOrderer from ODBLayer namespace
        /// </summary>
        public bool IsIOrdererArray()
        {
            return type is IArrayTypeSymbol arrayType &&
                   arrayType.ElementType.InODBLayerNamespace() &&
                   arrayType.ElementType.Name == "IOrderer";
        }
    }

    extension(ITypeSymbol type)
    {
        /// <summary>
        /// Checks if a type is a nullable reference type or Nullable&lt;T&gt;
        /// </summary>
        public bool IsNullable()
        {
            return type.NullableAnnotation == NullableAnnotation.Annotated ||
                   type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T };
        }

        /// <summary>
        /// Gets the underlying type from a nullable type
        /// </summary>
        public ITypeSymbol GetUnderlyingType()
        {
            return type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T } namedType
                ? namedType.TypeArguments[0]
                : type;
        }
    }
}
