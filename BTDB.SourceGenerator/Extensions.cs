using System;
using Microsoft.CodeAnalysis;

namespace BTDB.SourceGenerator;

public static class Extensions
{
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
