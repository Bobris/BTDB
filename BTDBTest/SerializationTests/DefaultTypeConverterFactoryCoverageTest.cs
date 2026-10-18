using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest.SerializationTests;

public class DefaultTypeConverterFactoryCoverageTest
{
    static TTo Convert<TFrom, TTo>(TFrom from)
    {
        var converter = new DefaultTypeConverterFactory().GetConverter(typeof(TFrom), typeof(TTo));
        Assert.NotNull(converter);
        TTo result = default!;
        converter(ref Unsafe.As<TFrom, byte>(ref from), ref Unsafe.As<TTo, byte>(ref result));
        return result;
    }

    [Fact]
    public void SameValueTypeDictionaryIsCopied()
    {
        var source = new Dictionary<string, int> { ["a"] = 1, ["removed"] = 2, ["b"] = 3 };
        source.Remove("removed");
        var result = Convert<IDictionary<string, int>, Dictionary<string, int>>(source);
        Assert.NotSame(source, result);
        Assert.Equal(new Dictionary<string, int> { ["a"] = 1, ["b"] = 3 }, result);
        Assert.Empty(Convert<IDictionary<string, int>, Dictionary<string, int>>(new Dictionary<string, int>()));
        Assert.Null(Convert<IDictionary<string, int>, Dictionary<string, int>>(null!));
    }

    [Fact]
    public void ReferenceValueDictionaryIsConverted()
    {
        var source = new Dictionary<string, int> { ["a"] = 1, ["removed"] = 2, ["b"] = 3 };
        source.Remove("removed");
        var result = Convert<IDictionary<string, int>, Dictionary<string, object>>(source);
        Assert.Equal(2, result.Count);
        Assert.Equal(1, result["a"]);
        Assert.Equal(3, result["b"]);
        Assert.Empty(Convert<IDictionary<string, int>, Dictionary<string, object>>(new Dictionary<string, int>()));
        Assert.Null(Convert<IDictionary<string, int>, Dictionary<string, object>>(null!));
    }

    [Fact]
    public void ObjectToReferenceTypeKeepsOnlyAssignableValues()
    {
        Assert.Equal("text", Convert<object, string>("text"));
        Assert.Null(Convert<object, string>(42));
        Assert.Null(Convert<object, string>(null!));
        var list = new List<int> { 1 };
        Assert.Same(list, Convert<object, IList<int>>(list));
    }

    [Fact]
    public void IntegersConvertToBool()
    {
        Assert.True(Convert<int, bool>(5));
        Assert.False(Convert<int, bool>(0));
        Assert.True(Convert<uint, bool>(uint.MaxValue));
        Assert.False(Convert<uint, bool>(0));
    }

    [Fact]
    public void ArraysConvertElementwise()
    {
        Assert.Equal(new long[] { 1, -2 }, Convert<int[], long[]>([1, -2]));
        Assert.Null(Convert<int[], long[]>(null!));
        Assert.Equal(new long[] { 3 }, Convert<int, long[]>(3));
        Assert.Equal(new object[] { "x" }, Convert<string, object[]>("x"));
        Assert.Empty(Convert<string, object[]>(null!));
        Assert.Equal(new List<long> { 4, 5 }, Convert<int[], List<long>>([4, 5]));
    }
}
