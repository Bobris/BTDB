using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;

namespace BTDB.SourceGenerator;

public static class Extensions
{
    public static IEnumerable<AttributeData> GetAllAttributes(this ISymbol symbol)
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

    public static bool InNamespace(this ISymbol symbol, params ReadOnlySpan<string> namespaces)
    {
        if (symbol.ContainingNamespace is null) return false;
        var cs = symbol.ContainingNamespace;
        while (true)
        {
            switch (cs.IsGlobalNamespace)
            {
                case true when namespaces.Length == 0:
                    return true;
                case true when namespaces.Length > 0:
                case false when namespaces.Length == 0:
                    return false;
            }

            if (cs.Name != namespaces[namespaces.Length - 1]) return false;
            cs = cs.ContainingNamespace;
            namespaces = namespaces.Slice(0, namespaces.Length - 1);
        }
    }

    public static bool InBTDBNamespace(this ISymbol symbol) => symbol.InNamespace("BTDB");
    public static bool InBTDBIOCNamespace(this ISymbol symbol) => symbol.InNamespace("BTDB", "IOC");
    public static bool InODBLayerNamespace(this ISymbol symbol) => symbol.InNamespace("BTDB", "ODBLayer");
}
