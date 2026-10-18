using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using BTDB.IL;
using BTDB.IL.Caching;
using Xunit;

namespace BTDBTest.ILBuilderTest;

public class EmitHelpersCoverageTest
{
    public class WithProperties
    {
        public int Number { get; set; }
        public string Text { get; set; } = "";
        public static int Twice(int x) => x * 2;
    }

    public class BaseWithPrivateSetter
    {
        public int Value { get; private set; }
    }

    public class DerivedFromPrivateSetter : BaseWithPrivateSetter
    {
    }

    public class NoDefaultConstructor
    {
        public NoDefaultConstructor(int x)
        {
        }
    }

    public class PrivateDefaultConstructor
    {
        PrivateDefaultConstructor()
        {
        }
    }

    public enum Color
    {
        Red,
        Green
    }

    public struct WithStaticEquals
    {
        public int A;

        public static bool Equals(WithStaticEquals left, WithStaticEquals right) => left.A % 10 == right.A % 10;
    }

    public struct WithOperatorOnly
    {
        public int A;

        public static bool operator ==(WithOperatorOnly left, WithOperatorOnly right) => left.A == right.A;
        public static bool operator !=(WithOperatorOnly left, WithOperatorOnly right) => left.A != right.A;
        public override bool Equals(object? obj) => obj is WithOperatorOnly o && o.A == A;
        public override int GetHashCode() => A;
    }

    public struct NotComparable
    {
        public int A;
    }

    public class GenericBase<T>
    {
    }

    public class GenericDerived : GenericBase<int>, IComparable<string>
    {
        public int CompareTo(string? other) => 0;
    }

    public class Outer
    {
        public class Inner
        {
        }
    }

    [Fact]
    public void GetMethodInfoAndPropertyInfoFromExpressions()
    {
        var mi = EmitHelpers.GetMethodInfo(() => WithProperties.Twice(0));
        Assert.Equal(nameof(WithProperties.Twice), mi.Name);
        Expression<Func<WithProperties, string>> text = p => p.Text;
        Assert.Equal(nameof(WithProperties.Text), text.GetPropertyInfo().Name);
        Expression<Func<WithProperties, object>> boxed = p => p.Number;
        Assert.Equal(nameof(WithProperties.Number), boxed.GetPropertyInfo().Name);
    }

    [Fact]
    public void CreateDelegateFromMethodInfo()
    {
        var mi = typeof(WithProperties).GetMethod(nameof(WithProperties.Twice))!;
        var f = mi.CreateDelegate<Func<int, int>>();
        Assert.Equal(14, f(7));
    }

    [Fact]
    public void UnwrapTaskReturnsResultType()
    {
        Assert.Null(((Type?)null).UnwrapTask());
        Assert.Equal(typeof(void), typeof(Task).UnwrapTask());
        Assert.Equal(typeof(int), typeof(Task<int>).UnwrapTask());
        Assert.Equal(typeof(string), typeof(string).UnwrapTask());
        Assert.Equal(typeof(ValueTask<int>), typeof(ValueTask<int>).UnwrapTask());
        Assert.Equal(typeof(List<int>), typeof(List<int>).UnwrapTask());
    }

    [Fact]
    public void IsDelegateDetectsDelegates()
    {
        Assert.True(typeof(Action).IsDelegate());
        Assert.True(typeof(Func<int, int>).IsDelegate());
        Assert.False(typeof(Delegate).IsDelegate());
        Assert.False(typeof(string).IsDelegate());
    }

    [Fact]
    public void InheritsOrImplementsHandlesGenericsAndInterfaces()
    {
        Assert.True(typeof(GenericDerived).InheritsOrImplements(typeof(GenericBase<>)));
        // Current behavior: closed generic parents are never matched because children are compared as open definitions
        Assert.False(typeof(GenericDerived).InheritsOrImplements(typeof(GenericBase<int>)));
        Assert.False(typeof(List<int>).InheritsOrImplements(typeof(IList<int>)));
        Assert.True(typeof(GenericDerived).InheritsOrImplements(typeof(IComparable<>)));
        Assert.True(typeof(List<int>).InheritsOrImplements(typeof(IList<>)));
        Assert.True(typeof(List<int>).InheritsOrImplements(typeof(List<>)));
        Assert.True(typeof(DerivedFromPrivateSetter).InheritsOrImplements(typeof(BaseWithPrivateSetter)));
        Assert.False(typeof(string).InheritsOrImplements(typeof(IList<>)));
        Assert.False(typeof(object).InheritsOrImplements(typeof(string)));
        Assert.False(typeof(IList<int>).InheritsOrImplements(typeof(string)));
    }

    [Fact]
    public void SpecializationOfFindsClosedGenericBaseOrInterface()
    {
        Assert.Equal(typeof(GenericBase<int>), typeof(GenericDerived).SpecializationOf(typeof(GenericBase<>)));
        Assert.Equal(typeof(IComparable<string>), typeof(GenericDerived).SpecializationOf(typeof(IComparable<>)));
        Assert.Equal(typeof(IList<int>), typeof(List<int>).SpecializationOf(typeof(IList<>)));
        Assert.Null(typeof(GenericDerived).SpecializationOf(typeof(IList<>)));
        Assert.Null(typeof(IDisposable).SpecializationOf(typeof(IList<>)));
        Assert.Null(typeof(object).SpecializationOf(typeof(IList<>)));
    }

    [Fact]
    public void ToSimpleNameFormatsTypes()
    {
        Assert.Equal("", ((Type?)null).ToSimpleName());
        Assert.Equal("int[]", typeof(int[]).ToSimpleName());
        Assert.Equal("int[,]", typeof(int[,]).ToSimpleName());
        Assert.Equal("string[][]", typeof(string[][]).ToSimpleName());
        Assert.Equal("Func<int,string>", typeof(Func<int, string>).ToSimpleName());
        Assert.Equal("System.Collections.Generic.Dictionary<long,ulong>",
            typeof(Dictionary<long, ulong>).ToSimpleName());
        Assert.Equal("Nullable<decimal>", typeof(decimal?).ToSimpleName());
        var primitives = new Dictionary<Type, string>
        {
            [typeof(byte)] = "byte", [typeof(sbyte)] = "sbyte", [typeof(ushort)] = "ushort",
            [typeof(short)] = "short", [typeof(uint)] = "uint", [typeof(float)] = "float",
            [typeof(double)] = "double", [typeof(char)] = "char", [typeof(bool)] = "bool",
            [typeof(void)] = "void", [typeof(object)] = "object", [typeof(nint)] = "nint",
            [typeof(nuint)] = "nuint"
        };
        foreach (var (type, name) in primitives)
        {
            Assert.Equal(name, type.ToSimpleName());
        }

        Assert.Equal("BTDBTest.ILBuilderTest.EmitHelpersCoverageTest.Outer.Inner", typeof(Outer.Inner).ToSimpleName());
        Assert.Equal("System.Guid", typeof(Guid).ToSimpleName());
        var noNamespace = new ILBuilderRelease().NewType("NoNamespaceCoverageType", typeof(object), Type.EmptyTypes)
            .CreateType();
        Assert.Equal("NoNamespaceCoverageType", noNamespace.ToSimpleName());
    }

    [Fact]
    public void PropertyAccessorHelpersFindNonPublicAccessors()
    {
        var number = typeof(WithProperties).GetProperty(nameof(WithProperties.Number))!;
        Assert.Equal("get_Number", number.GetAnyGetMethod()!.Name);
        Assert.Equal("set_Number", number.GetAnySetMethod()!.Name);
        var inherited = typeof(DerivedFromPrivateSetter).GetProperty(nameof(BaseWithPrivateSetter.Value))!;
        Assert.Equal("set_Value", inherited.GetAnySetMethod()!.Name);
        Assert.Equal("get_Value", inherited.GetAnyGetMethod()!.Name);
        var getterOnly = typeof(string).GetProperty(nameof(string.Length))!;
        Assert.Null(getterOnly.GetAnySetMethod());
    }

    [Fact]
    public void GetDefaultConstructorFindsPublicAndPrivateConstructors()
    {
        Assert.NotNull(typeof(WithProperties).GetDefaultConstructor());
        Assert.NotNull(typeof(PrivateDefaultConstructor).GetDefaultConstructor());
        Assert.Null(typeof(NoDefaultConstructor).GetDefaultConstructor());
    }

    static Func<T, T, bool> BuildEquals<T>(bool caching)
    {
        IILBuilder builder = caching ? new CachingILBuilder(new ILBuilderRelease()) : new ILBuilderRelease();
        var method = builder.NewMethod<Func<T, T, bool>>("JumpIfEqual");
        var il = method.Generator;
        var equal = il.DefineLabel("equal");
        EmitHelpers.GenerateJumpIfEqual(il, typeof(T), equal, g => g.Ldarg(0), g => g.Ldarg(1));
        il.LdcI4(0).Ret().Mark(equal).LdcI4(1).Ret();
        return method.Create();
    }

    static void CheckEquals<T>(bool caching, T a, T b, bool expected)
    {
        var f = BuildEquals<T>(caching);
        Assert.Equal(expected, f(a, b));
        Assert.Equal(expected, f(b, a));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GenerateJumpIfEqualComparesSupportedTypes(bool caching)
    {
        CheckEquals(caching, (sbyte)-1, (sbyte)-1, true);
        CheckEquals(caching, (byte)1, (byte)2, false);
        CheckEquals(caching, (short)-5, (short)-5, true);
        CheckEquals(caching, (ushort)5, (ushort)6, false);
        CheckEquals(caching, 7, 7, true);
        CheckEquals(caching, 7u, 8u, false);
        CheckEquals(caching, long.MaxValue, long.MaxValue, true);
        CheckEquals(caching, 1ul, 2ul, false);
        CheckEquals(caching, 1.5f, 1.5f, true);
        CheckEquals(caching, double.NaN, double.NaN, false);
        CheckEquals(caching, true, false, false);
        CheckEquals(caching, Color.Green, Color.Green, true);
        CheckEquals(caching, Color.Green, Color.Red, false);
        CheckEquals(caching, "abc", "ab" + "c".ToLowerInvariant(), true);
        CheckEquals(caching, "abc", null!, false);
        CheckEquals(caching, (string)null!, null!, true);
        CheckEquals(caching, 1.5m, 1.50m, true);
        CheckEquals(caching, new WithStaticEquals { A = 1 }, new WithStaticEquals { A = 11 }, true);
        CheckEquals(caching, new WithStaticEquals { A = 1 }, new WithStaticEquals { A = 2 }, false);
        CheckEquals(caching, new WithOperatorOnly { A = 3 }, new WithOperatorOnly { A = 3 }, true);
        CheckEquals(caching, new WithOperatorOnly { A = 3 }, new WithOperatorOnly { A = 4 }, false);
        CheckEquals(caching, (int?)null, null, true);
        CheckEquals(caching, (int?)1, null, false);
        CheckEquals(caching, (int?)1, 1, true);
        CheckEquals(caching, (int?)1, 2, false);
        CheckEquals(caching, (decimal?)2.0m, 2m, true);
    }

    [Theory]
    [InlineData(typeof(MostlyNonNullable), nameof(MostlyNonNullable.NotNull), false)]
    [InlineData(typeof(MostlyNonNullable), nameof(MostlyNonNullable.MaybeNull), true)]
    [InlineData(typeof(MostlyNonNullable), nameof(MostlyNonNullable.ListOfNullable), false)]
    [InlineData(typeof(MostlyNonNullable), nameof(MostlyNonNullable.NullableList), true)]
    [InlineData(typeof(MostlyNonNullable), nameof(MostlyNonNullable.Value), false)]
    [InlineData(typeof(MostlyNullable), nameof(MostlyNullable.A), true)]
    [InlineData(typeof(MostlyNullable), nameof(MostlyNullable.NotNull), false)]
    [InlineData(typeof(Oblivious), nameof(Oblivious.Text), false)]
    public void IsNullableReadsNullableMetadata(Type type, string propertyName, bool expected)
    {
        Assert.Equal(expected, EmitHelpers.IsNullable(type, type.GetProperty(propertyName)!));
    }

#if DEBUG
    [Fact]
    public void IsNullableRequiresDeclaringType()
    {
        Assert.Throws<ArgumentException>(() =>
            EmitHelpers.IsNullable(typeof(Oblivious), typeof(MostlyNullable).GetProperty(nameof(MostlyNullable.A))!));
    }
#endif

    [Fact]
    public void GenerateJumpIfEqualRejectsTypesWithoutEquality()
    {
        var method = new ILBuilderRelease().NewMethod<Func<NotComparable, NotComparable, bool>>("x");
        var il = method.Generator;
        var label = il.DefineLabel();
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
            EmitHelpers.GenerateJumpIfEqual(il, typeof(NotComparable), label, g => g.Ldarg(0), g => g.Ldarg(1)));
        Assert.Contains("NotComparable", ex.Message);
    }
}

#nullable enable
public class MostlyNonNullable
{
    public string NotNull { get; set; } = "";
    public string NotNull2 { get; set; } = "";
    public string? MaybeNull { get; set; }
    public List<string?> ListOfNullable { get; set; } = new();
    public List<string>? NullableList { get; set; }
    public int Value { get; set; }
}

public class MostlyNullable
{
    public string? A { get; set; }
    public string? B { get; set; }
    public string? C { get; set; }
    public string NotNull { get; set; } = "";
}
#nullable disable

public class Oblivious
{
    public string Text { get; set; }
}
#nullable restore
