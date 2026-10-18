using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class TypeUtilitiesTests
{
    [Theory]
    [InlineData("int", "int", true)]
    [InlineData("global::System.Guid", "System.Guid", true)]
    [InlineData("byte", "ulong", true)]
    [InlineData("sbyte", "long", true)]
    [InlineData("int", "uint", false)]
    [InlineData("global::System.Collections.Generic.List<string>?", "global::System.Collections.Generic.IList<string>", true)]
    [InlineData("global::System.Collections.Generic.List<string>", "global::System.Collections.Generic.IList<int>", false)]
    [InlineData("global::System.Collections.Generic.HashSet<string>", "global::System.Collections.Generic.IList<string>", false)]
    [InlineData("string", "global::System.Collections.Generic.IList<string>", false)]
    public void AreTypesCompatible(string parameterType, string fieldType, bool expected)
    {
        Assert.Equal(expected, TypeUtilities.AreTypesCompatible(parameterType, fieldType));
    }

    [Theory]
    [InlineData("byte", IntegralType.Byte, true, false)]
    [InlineData("ushort", IntegralType.UInt16, true, false)]
    [InlineData("uint", IntegralType.UInt32, true, false)]
    [InlineData("ulong", IntegralType.UInt64, true, false)]
    [InlineData("sbyte", IntegralType.SByte, false, true)]
    [InlineData("short", IntegralType.Int16, false, true)]
    [InlineData("int", IntegralType.Int32, false, true)]
    [InlineData("long", IntegralType.Int64, false, true)]
    [InlineData("string", IntegralType.None, false, false)]
    public void IntegralTypes(string type, IntegralType expected, bool unsigned, bool signed)
    {
        Assert.Equal(expected, TypeUtilities.NormalizeIntegralType(type));
        Assert.Equal(unsigned, TypeUtilities.IsUnsignedIntegralType(type));
        Assert.Equal(signed, TypeUtilities.IsSignedIntegralType(type));
    }

    [Theory]
    [InlineData("global::System.Int32", "System.Int32")]
    [InlineData("int", "int")]
    public void RemoveGlobalPrefix(string type, string expected)
    {
        Assert.Equal(expected, TypeUtilities.RemoveGlobalPrefix(type));
    }
}
