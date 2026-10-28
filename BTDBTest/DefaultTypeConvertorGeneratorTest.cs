using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest;

public class DefaultTypeConvertorGeneratorTest
{
    static readonly Dictionary<Type, object[]> Samples = new()
    {
        [typeof(byte)] = [(byte)0, (byte)1, (byte)127, (byte)128, byte.MaxValue],
        [typeof(sbyte)] = [(sbyte)0, (sbyte)1, (sbyte)-1, sbyte.MinValue, sbyte.MaxValue],
        [typeof(ushort)] = [(ushort)0, (ushort)1, (ushort)32768, ushort.MaxValue],
        [typeof(short)] = [(short)0, (short)-1, short.MinValue, short.MaxValue],
        [typeof(uint)] = [0u, 1u, 1u << 31, 3_000_000_000u, uint.MaxValue],
        [typeof(int)] = [0, -1, 200, 40000, int.MinValue, int.MaxValue],
        [typeof(ulong)] = [0ul, 1ul, 1ul << 63, 10_000_000_000_000_000_000ul, ulong.MaxValue],
        [typeof(long)] = [0L, -1L, 3_000_000_000L, long.MinValue, long.MaxValue],
        [typeof(float)] = [0f, -1.5f, 200.75f, 3e9f],
        [typeof(double)] = [0d, -1.5d, 40000.25d, 3e9d]
    };

    public static IEnumerable<object[]> NumericPairs() =>
        from f in Samples.Keys from t in Samples.Keys where f != t select new object[] { f, t };

    /// Every numeric conversion the emitted code performs gives what an unchecked C# cast gives.
    [Theory]
    [MemberData(nameof(NumericPairs))]
    public void NumericConversionMatchesCast(Type from, Type to)
    {
        var generator = DefaultTypeConvertorGenerator.Instance.GenerateConversion(from, to);
        Assert.NotNull(generator);
        var method = new ILBuilderRelease().NewMethod<Func<object, object>>($"Convert{from.Name}To{to.Name}");
        method.Generator.Ldarg(0).UnboxAny(from).Do(generator).Box(to).Ret();
        var emitted = method.Create();
        var parameter = Expression.Parameter(typeof(object));
        var cast = Expression.Lambda<Func<object, object>>(
            Expression.Convert(Expression.Convert(Expression.Convert(parameter, from), to), typeof(object)),
            parameter).Compile();
        foreach (var value in Samples[from])
            Assert.Equal(cast(value), emitted(value));
    }

    /// The converters used without IL emitting agree with the casts too.
    [Theory]
    [MemberData(nameof(NumericPairs))]
    public void NoEmitNumericConversionMatchesCast(Type from, Type to)
    {
        var converter = DefaultTypeConverterFactory.Instance.GetConverter(from, to);
        Assert.NotNull(converter);
        var check = typeof(DefaultTypeConvertorGeneratorTest).GetMethod(nameof(CheckNoEmit),
            BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(from, to);
        foreach (var value in Samples[from])
            check.Invoke(null, [converter, value]);
    }

    static void CheckNoEmit<TFrom, TTo>(Converter converter, object value)
    {
        var from = (TFrom)value;
        TTo to = default!;
        converter(ref Unsafe.As<TFrom, byte>(ref from),
            ref Unsafe.As<TTo, byte>(ref to));
        var parameter = Expression.Parameter(typeof(TFrom));
        var cast = Expression.Lambda<Func<TFrom, TTo>>(Expression.Convert(parameter, typeof(TTo)), parameter).Compile();
        Assert.Equal(cast(from), to);
    }
}
