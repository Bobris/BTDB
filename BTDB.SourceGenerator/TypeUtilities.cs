using System;
using System.Globalization;

namespace BTDB.SourceGenerator;

/// <summary>
/// Utility methods for type compatibility and string operations
/// </summary>
public static class TypeUtilities
{
    /// <summary>
    /// Checks if two types are compatible, handling various normalization scenarios
    /// </summary>
    public static bool AreTypesCompatible(string parameterType, string fieldType)
    {
        if (parameterType == fieldType) return true;

        var normalizedParamType = RemoveGlobalPrefix(parameterType.AsSpan());
        var normalizedFieldType = RemoveGlobalPrefix(fieldType.AsSpan());
        
        if (normalizedParamType.SequenceEqual(normalizedFieldType)) return true;

        if (IsUnsignedIntegralType(normalizedParamType) && IsUnsignedIntegralType(normalizedFieldType)) return true;
        if (IsSignedIntegralType(normalizedParamType) && IsSignedIntegralType(normalizedFieldType)) return true;

        return false;
    }

/// <summary>
    /// Removes "global::" prefix from type names (ReadOnlySpan overload for performance)
    /// </summary>
    public static ReadOnlySpan<char> RemoveGlobalPrefix(ReadOnlySpan<char> type)
    {
        const string globalPrefix = "global::";
        if (type.StartsWith(globalPrefix, StringComparison.Ordinal))
        {
            return type.Slice(globalPrefix.Length);
        }
        return type;
    }

    /// <summary>
    /// Removes "global::" prefix from type names (string overload for convenience)
    /// </summary>
    public static string RemoveGlobalPrefix(string type)
    {
        return RemoveGlobalPrefix(type.AsSpan()).ToString();
    }

    /// <summary>
    /// Checks if a type string represents an unsigned integral type
    /// </summary>
    public static bool IsUnsignedIntegralType(string type)
    {
        return NormalizeIntegralType(type.AsSpan()) is IntegralType.Byte or IntegralType.UInt16 or IntegralType.UInt32
            or IntegralType.UInt64;
    }

    /// <summary>
    /// Checks if a type string represents an unsigned integral type (ReadOnlySpan overload for performance)
    /// </summary>
    public static bool IsUnsignedIntegralType(ReadOnlySpan<char> type)
    {
        return NormalizeIntegralType(type) is IntegralType.Byte or IntegralType.UInt16 or IntegralType.UInt32
            or IntegralType.UInt64;
    }

    /// <summary>
    /// Checks if a type string represents a signed integral type
    /// </summary>
    public static bool IsSignedIntegralType(string type)
    {
        return NormalizeIntegralType(type.AsSpan()) is IntegralType.SByte or IntegralType.Int16 or IntegralType.Int32
            or IntegralType.Int64;
    }

    /// <summary>
    /// Checks if a type string represents a signed integral type (ReadOnlySpan overload for performance)
    /// </summary>
    public static bool IsSignedIntegralType(ReadOnlySpan<char> type)
    {
        return NormalizeIntegralType(type) is IntegralType.SByte or IntegralType.Int16 or IntegralType.Int32
            or IntegralType.Int64;
    }

    /// <summary>
    /// Normalizes integral type strings to enum values
    /// </summary>
    public static IntegralType NormalizeIntegralType(string type)
    {
        return NormalizeIntegralType(type.AsSpan());
    }

    /// <summary>
    /// Normalizes integral type strings to enum values (ReadOnlySpan overload for performance)
    /// </summary>
    public static IntegralType NormalizeIntegralType(ReadOnlySpan<char> type)
    {
        return type switch
        {
            "byte" => IntegralType.Byte,
            "ushort" => IntegralType.UInt16,
            "uint" => IntegralType.UInt32,
            "ulong" => IntegralType.UInt64,
            "sbyte" => IntegralType.SByte,
            "short" => IntegralType.Int16,
            "int" => IntegralType.Int32,
            "long" => IntegralType.Int64,
            _ => IntegralType.None
        };
    }
}

/// <summary>
/// Enum representing integral types
/// </summary>
public enum IntegralType
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