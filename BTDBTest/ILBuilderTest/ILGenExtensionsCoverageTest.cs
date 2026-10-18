using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.IL.Caching;
using Xunit;

namespace BTDBTest.ILBuilderTest;

public class ILGenExtensionsCoverageTest
{
    public delegate T LoadRef<T>(ref T value);

    public delegate void StoreRef<T>(ref T target, T value);

    public class Holder
    {
        public int Field;
        public static int StaticField;
        public string? Name;
        public static int StaticProp => 42;
        public int InstanceProp => Field * 2;
    }

    static IILBuilder Builder(bool caching) =>
        caching ? new CachingILBuilder(new ILBuilderRelease()) : new ILBuilderRelease();

    static TDelegate Build<TDelegate>(bool caching, Action<IILGen> body) where TDelegate : Delegate
    {
        var method = Builder(caching).NewMethod<TDelegate>("Test");
        body(method.Generator);
        return method.Create();
    }

    sealed class RecordingILGen : IILGen
    {
        public readonly List<(OpCode OpCode, object? Param)> Emitted = new();

        public IILLabel DefineLabel(string? name = null) => throw new NotSupportedException();
        public IILLocal DeclareLocal(Type type, string? name = null, bool pinned = false) =>
            throw new NotSupportedException();
        public IILGen Comment(string text) => this;
        public IILGen Mark(IILLabel label) => this;
        public IILGen Ldftn(IILMethod method) => this;
        public IILGen Ldstr(string str) => this;
        public IILGen Try() => this;
        public IILGen Catch(Type exceptionType) => this;
        public IILGen Finally() => this;
        public IILGen EndTry() => this;
        public void Emit(OpCode opCode) => Emitted.Add((opCode, null));
        public void Emit(OpCode opCode, sbyte param) => Emitted.Add((opCode, param));
        public void Emit(OpCode opCode, byte param) => Emitted.Add((opCode, param));
        public void Emit(OpCode opCode, ushort param) => Emitted.Add((opCode, param));
        public void Emit(OpCode opCode, int param) => Emitted.Add((opCode, param));
        public void Emit(OpCode opCode, FieldInfo param) => Emitted.Add((opCode, param));
        public void Emit(OpCode opCode, ConstructorInfo param) => Emitted.Add((opCode, param));
        public void Emit(OpCode opCode, MethodInfo param) => Emitted.Add((opCode, param));
        public void Emit(OpCode opCode, Type type) => Emitted.Add((opCode, type));
        public void Emit(OpCode opCode, IILLocal ilLocal) => Emitted.Add((opCode, ilLocal));
        public void Emit(OpCode opCode, IILLabel ilLabel) => Emitted.Add((opCode, ilLabel));
        public void Emit(OpCode opCode, IILField ilField) => Emitted.Add((opCode, ilField));
        public void Emit(OpCode opCode, long value) => Emitted.Add((opCode, value));
        public void Emit(OpCode opCode, float value) => Emitted.Add((opCode, value));
        public void Emit(OpCode opCode, double value) => Emitted.Add((opCode, value));
    }

    static (OpCode OpCode, object? Param) Single(Action<IILGen> action)
    {
        var rec = new RecordingILGen();
        action(rec);
        return Assert.Single(rec.Emitted);
    }

    public static IEnumerable<object[]> LdcI4Encodings()
    {
        yield return [0, "ldc.i4.0", null!];
        yield return [1, "ldc.i4.1", null!];
        yield return [2, "ldc.i4.2", null!];
        yield return [3, "ldc.i4.3", null!];
        yield return [4, "ldc.i4.4", null!];
        yield return [5, "ldc.i4.5", null!];
        yield return [6, "ldc.i4.6", null!];
        yield return [7, "ldc.i4.7", null!];
        yield return [8, "ldc.i4.8", null!];
        yield return [-1, "ldc.i4.m1", null!];
        yield return [9, "ldc.i4.s", (sbyte)9];
        yield return [127, "ldc.i4.s", (sbyte)127];
        yield return [-128, "ldc.i4.s", (sbyte)-128];
        yield return [-2, "ldc.i4.s", (sbyte)-2];
        yield return [128, "ldc.i4", 128];
        yield return [-129, "ldc.i4", -129];
        yield return [int.MaxValue, "ldc.i4", int.MaxValue];
        yield return [int.MinValue, "ldc.i4", int.MinValue];
    }

    [Theory]
    [MemberData(nameof(LdcI4Encodings))]
    public void LdcI4UsesShortestEncodingAndLoadsValue(int value, string opCodeName, object? param)
    {
        var (opCode, emittedParam) = Single(il => il.LdcI4(value));
        Assert.Equal(opCodeName, opCode.Name);
        Assert.Equal(param, emittedParam);
        foreach (var caching in new[] { false, true })
        {
            Assert.Equal(value, Build<Func<int>>(caching, il => il.LdcI4(value).Ret())());
        }
    }

    [Theory]
    [InlineData((ushort)0, "ldarg.0")]
    [InlineData((ushort)1, "ldarg.1")]
    [InlineData((ushort)2, "ldarg.2")]
    [InlineData((ushort)3, "ldarg.3")]
    [InlineData((ushort)4, "ldarg.s")]
    [InlineData((ushort)255, "ldarg.s")]
    [InlineData((ushort)256, "ldarg")]
    public void LdargChoosesEncodingByIndex(ushort index, string opCodeName)
    {
        var (opCode, param) = Single(il => il.Ldarg(index));
        Assert.Equal(opCodeName, opCode.Name);
        if (index > 3) Assert.Equal(index <= 255 ? (object)(byte)index : index, param);
    }

    [Theory]
    [InlineData((ushort)0, "starg.s")]
    [InlineData((ushort)255, "starg.s")]
    [InlineData((ushort)256, "starg")]
    public void StargChoosesEncodingByIndex(ushort index, string opCodeName)
    {
        var (opCode, _) = Single(il => il.Starg(index));
        Assert.Equal(opCodeName, opCode.Name);
    }

    [Theory]
    [InlineData((ushort)0, "ldloc.0", "stloc.0")]
    [InlineData((ushort)1, "ldloc.1", "stloc.1")]
    [InlineData((ushort)2, "ldloc.2", "stloc.2")]
    [InlineData((ushort)3, "ldloc.3", "stloc.3")]
    [InlineData((ushort)4, "ldloc.s", "stloc.s")]
    [InlineData((ushort)255, "ldloc.s", "stloc.s")]
    [InlineData((ushort)256, "ldloc", "stloc")]
    [InlineData((ushort)65534, "ldloc", "stloc")]
    public void LdlocStlocChooseEncodingByIndex(ushort index, string ldName, string stName)
    {
        Assert.Equal(ldName, Single(il => il.Ldloc(index)).OpCode.Name);
        Assert.Equal(stName, Single(il => il.Stloc(index)).OpCode.Name);
    }

    [Fact]
    public void LdlocStlocRejectIndex65535()
    {
        var rec = new RecordingILGen();
        Assert.Throws<ArgumentOutOfRangeException>(() => rec.Ldloc(65535));
        Assert.Throws<ArgumentOutOfRangeException>(() => rec.Stloc(65535));
        Assert.Empty(rec.Emitted);
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 3)]
    [InlineData(false, 4)]
    [InlineData(false, 255)]
    [InlineData(false, 256)]
    [InlineData(false, 299)]
    [InlineData(true, 0)]
    [InlineData(true, 3)]
    [InlineData(true, 4)]
    [InlineData(true, 255)]
    [InlineData(true, 256)]
    [InlineData(true, 299)]
    public void LocalsByIndexWorkInAllEncodings(bool caching, int index)
    {
        var f = Build<Func<int, int>>(caching, il =>
        {
            for (var i = 0; i < 300; i++) il.DeclareLocal(typeof(int));
            il.Ldarg(0).Stloc((ushort)index).LdcI4(1).Stloc((ushort)(index == 0 ? 1 : 0))
                .Ldloc((ushort)index).Ret();
        });
        Assert.Equal(1234, f(1234));
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 3)]
    [InlineData(false, 200)]
    [InlineData(false, 299)]
    [InlineData(true, 1)]
    [InlineData(true, 255)]
    [InlineData(true, 256)]
    public void ArgumentsByIndexWorkInAllEncodings(bool caching, int index)
    {
        var types = Enumerable.Repeat(typeof(int), 300).Append(typeof(int)).ToArray();
        var delegateType = Expression.GetDelegateType(types);
        var method = Builder(caching).NewMethod("ManyArgs", delegateType);
        method.Generator
            .Ldarg((ushort)index).LdcI4(1000).Add().Starg((ushort)index)
            .Ldarg((ushort)index).Ret();
        var d = (Delegate)method.Create();
        var args = Enumerable.Range(0, 300).Select(i => (object)i).ToArray();
        Assert.Equal(index + 1000, d.DynamicInvoke(args));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void WideConstantsLoad(bool caching)
    {
        Assert.Equal(long.MinValue, Build<Func<long>>(caching, il => il.LdcI8(long.MinValue).Ret())());
        Assert.Equal(1.5f, Build<Func<float>>(caching, il => il.LdcR4(1.5f).Ret())());
        Assert.Equal(-2.25, Build<Func<double>>(caching, il => il.LdcR8(-2.25).Ret())());
        Assert.True(float.IsNaN(Build<Func<float>>(caching, il => il.LdcR4(float.NaN).Ret())()));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void LdLoadsSupportedConstants(bool caching)
    {
        Assert.Null(Build<Func<object>>(caching, il => il.Ld(null).Ret())());
        Assert.False(Build<Func<bool>>(caching, il => il.Ld(false).Ret())());
        Assert.True(Build<Func<bool>>(caching, il => il.Ld(true).Ret())());
        Assert.Equal((short)-300, Build<Func<short>>(caching, il => il.Ld((short)-300).Ret())());
        Assert.Equal(123456, Build<Func<int>>(caching, il => il.Ld(123456).Ret())());
        Assert.Equal(1L << 40, Build<Func<long>>(caching, il => il.Ld(1L << 40).Ret())());
        Assert.Equal(0.5f, Build<Func<float>>(caching, il => il.Ld(0.5f).Ret())());
        Assert.Equal(0.25, Build<Func<double>>(caching, il => il.Ld(0.25).Ret())());
        Assert.Equal("text", Build<Func<string>>(caching, il => il.Ld("text").Ret())());
    }

    [Fact]
    public void LdRejectsUnsupportedConstant()
    {
        var rec = new RecordingILGen();
        var ex = Assert.Throws<ArgumentException>(() => rec.Ld(1.5m));
        Assert.Equal("value", ex.ParamName);
        Assert.Throws<ArgumentException>(() => rec.Ld((byte)1));
    }

    public static IEnumerable<object[]> BranchCases()
    {
        var pairs = new (int, int)[] { (1, 2), (2, 1), (5, 5), (-1, 1), (1, -1), (int.MinValue, int.MaxValue) };
        foreach (var caching in new[] { false, true })
        {
            foreach (var (a, b) in pairs)
            {
                yield return [caching, a, b];
            }
        }
    }

    static int RunBranch(bool caching, Action<IILGen, IILLabel> branch, int a, int b)
    {
        return Build<Func<int, int, int>>(caching, il =>
        {
            var taken = il.DefineLabel("taken");
            il.Ldarg(0).Ldarg(1);
            branch(il, taken);
            il.LdcI4(0).Ret().Mark(taken).LdcI4(1).Ret();
        })(a, b);
    }

    [Theory]
    [MemberData(nameof(BranchCases))]
    public void TwoOperandBranchesMatchCSharpComparisons(bool caching, int a, int b)
    {
        var ua = unchecked((uint)a);
        var ub = unchecked((uint)b);
        Assert.Equal(a < b ? 1 : 0, RunBranch(caching, (il, l) => il.Blt(l), a, b));
        Assert.Equal(a > b ? 1 : 0, RunBranch(caching, (il, l) => il.Bgt(l), a, b));
        Assert.Equal(ua <= ub ? 1 : 0, RunBranch(caching, (il, l) => il.BleUnS(l), a, b));
        Assert.Equal(a != b ? 1 : 0, RunBranch(caching, (il, l) => il.BneUnS(l), a, b));
        Assert.Equal(a == b ? 1 : 0, RunBranch(caching, (il, l) => il.BeqS(l), a, b));
        Assert.Equal(a == b ? 1 : 0, RunBranch(caching, (il, l) => il.Beq(l), a, b));
        Assert.Equal(ua >= ub ? 1 : 0, RunBranch(caching, (il, l) => il.BgeUnS(l), a, b));
        Assert.Equal(ua >= ub ? 1 : 0, RunBranch(caching, (il, l) => il.BgeUn(l), a, b));
    }

    [Theory]
    [MemberData(nameof(BranchCases))]
    public void ComparisonsMatchCSharp(bool caching, int a, int b)
    {
        var ua = unchecked((uint)a);
        var ub = unchecked((uint)b);
        Assert.Equal(a == b, Build<Func<int, int, bool>>(caching, il => il.Ldarg(0).Ldarg(1).Ceq().Ret())(a, b));
        Assert.Equal(a > b, Build<Func<int, int, bool>>(caching, il => il.Ldarg(0).Ldarg(1).Cgt().Ret())(a, b));
        Assert.Equal(ua > ub, Build<Func<int, int, bool>>(caching, il => il.Ldarg(0).Ldarg(1).CgtUn().Ret())(a, b));
    }

    [Theory]
    [MemberData(nameof(BranchCases))]
    public void ArithmeticMatchesCSharp(bool caching, int a, int b)
    {
        Assert.Equal(unchecked(a + b), Build<Func<int, int, int>>(caching, il => il.Ldarg(0).Ldarg(1).Add().Ret())(a, b));
        Assert.Equal(unchecked(a - b), Build<Func<int, int, int>>(caching, il => il.Ldarg(0).Ldarg(1).Sub().Ret())(a, b));
        Assert.Equal(unchecked(a * b), Build<Func<int, int, int>>(caching, il => il.Ldarg(0).Ldarg(1).Mul().Ret())(a, b));
        Assert.Equal(a | b, Build<Func<int, int, int>>(caching, il => il.Ldarg(0).Ldarg(1).Or().Ret())(a, b));
        Assert.Equal(a / b, Build<Func<int, int, int>>(caching, il => il.Ldarg(0).Ldarg(1).Div().Ret())(a, b));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DivisionErrorsPropagate(bool caching)
    {
        var div = Build<Func<int, int, int>>(caching, il => il.Ldarg(0).Ldarg(1).Div().Ret());
        Assert.Throws<DivideByZeroException>(() => div(1, 0));
        Assert.Throws<OverflowException>(() => div(int.MinValue, -1));
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 7)]
    [InlineData(true, 0)]
    [InlineData(true, -3)]
    public void SingleOperandAndUnconditionalBranches(bool caching, int value)
    {
        int Run(Action<IILGen, IILLabel> branch) => Build<Func<int, int>>(caching, il =>
        {
            var taken = il.DefineLabel();
            il.Ldarg(0);
            branch(il, taken);
            il.LdcI4(0).Ret().Mark(taken).LdcI4(1).Ret();
        })(value);

        Assert.Equal(value == 0 ? 1 : 0, Run((il, l) => il.Brfalse(l)));
        Assert.Equal(value == 0 ? 1 : 0, Run((il, l) => il.BrfalseS(l)));
        Assert.Equal(value != 0 ? 1 : 0, Run((il, l) => il.Brtrue(l)));
        Assert.Equal(value != 0 ? 1 : 0, Run((il, l) => il.BrtrueS(l)));
        Assert.Equal(1, Run((il, l) => il.Pop().Br(l)));
        Assert.Equal(1, Run((il, l) => il.Pop().BrS(l)));
    }

    [Theory]
    [InlineData(false, 0L)]
    [InlineData(false, -1L)]
    [InlineData(false, 0x1_2345_6789_ABCDL)]
    [InlineData(true, long.MinValue)]
    [InlineData(true, 255L)]
    [InlineData(true, -129L)]
    public void ConversionsMatchCSharpUncheckedCasts(bool caching, long value)
    {
        Assert.Equal(unchecked((byte)value), Build<Func<long, byte>>(caching, il => il.Ldarg(0).ConvU1().Ret())(value));
        Assert.Equal(unchecked((sbyte)value), Build<Func<long, sbyte>>(caching, il => il.Ldarg(0).ConvI1().Ret())(value));
        Assert.Equal(unchecked((ushort)value), Build<Func<long, ushort>>(caching, il => il.Ldarg(0).ConvU2().Ret())(value));
        Assert.Equal(unchecked((short)value), Build<Func<long, short>>(caching, il => il.Ldarg(0).ConvI2().Ret())(value));
        Assert.Equal(unchecked((uint)value), Build<Func<long, uint>>(caching, il => il.Ldarg(0).ConvU4().Ret())(value));
        Assert.Equal(unchecked((int)value), Build<Func<long, int>>(caching, il => il.Ldarg(0).ConvI4().Ret())(value));
        var i32 = unchecked((int)value);
        Assert.Equal(unchecked((ulong)(uint)i32), Build<Func<int, ulong>>(caching, il => il.Ldarg(0).ConvU8().Ret())(i32));
        Assert.Equal((long)i32, Build<Func<int, long>>(caching, il => il.Ldarg(0).ConvI8().Ret())(i32));
        Assert.Equal((float)i32, Build<Func<int, float>>(caching, il => il.Ldarg(0).ConvR4().Ret())(i32));
        Assert.Equal((double)value, Build<Func<long, double>>(caching, il => il.Ldarg(0).ConvR8().Ret())(value));
    }

    static void CheckArray<T>(bool caching, T value)
    {
        var load = Build<Func<T[], int, T>>(caching, il => il.Ldarg(0).Ldarg(1).Ldelem(typeof(T)).Ret());
        var store = Build<Action<T[], int, T>>(caching, il => il.Ldarg(0).Ldarg(1).Ldarg(2).Stelem(typeof(T)).Ret());
        var array = new T[3];
        store(array, 1, value);
        Assert.Equal(default(T), array[0]);
        Assert.Equal(value, array[1]);
        Assert.Equal(default(T), array[2]);
        Assert.Equal(value, load(array, 1));
        Assert.Throws<IndexOutOfRangeException>(() => load(array, 3));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void LdelemAndStelemSupportAllElementKinds(bool caching)
    {
        CheckArray(caching, -12345678);
        CheckArray(caching, (short)-1234);
        CheckArray(caching, (sbyte)-12);
        CheckArray(caching, long.MinValue + 7);
        CheckArray(caching, (ushort)65000);
        CheckArray(caching, (byte)250);
        CheckArray(caching, 4000000000u);
        CheckArray(caching, ulong.MaxValue - 3);
        CheckArray(caching, 1.25f);
        CheckArray(caching, -3.5);
        CheckArray(caching, "str");
        CheckArray(caching, new Guid("9c5b0e16-1e4b-4b7a-8f3f-3d1a2b3c4d5e"));
        CheckArray(caching, 12.5m);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ArrayHelpersWork(bool caching)
    {
        var newArr = Build<Func<int, string[]>>(caching, il => il.Ldarg(0).Newarr(typeof(string)).Ret());
        Assert.Equal(4, newArr(4).Length);
        var refStore = Build<Action<object[], object>>(caching,
            il => il.Ldarg(0).LdcI4(0).Ldarg(1).StelemRef().Ret());
        var refLoad = Build<Func<object[], object>>(caching, il => il.Ldarg(0).LdcI4(0).LdelemRef().Ret());
        var objects = new object[1];
        refStore(objects, "x");
        Assert.Equal("x", refLoad(objects));
        Assert.Throws<ArrayTypeMismatchException>(() => refStore(new string[1], 1));
        var increment = Build<Action<int[], int>>(caching, il => il
            .Ldarg(0).Ldarg(1).Ldelema(typeof(int)).Dup().Ldind(typeof(int)).LdcI4(1).Add().Stind(typeof(int))
            .Ret());
        var ints = new[] { 10, 20 };
        increment(ints, 1);
        Assert.Equal([10, 21], ints);
    }

    static void CheckIndirect<T>(bool caching, T value)
    {
        var load = Build<LoadRef<T>>(caching, il => il.Ldarg(0).Ldind(typeof(T)).Ret());
        var store = Build<StoreRef<T>>(caching, il => il.Ldarg(0).Ldarg(1).Stind(typeof(T)).Ret());
        T target = default!;
        store(ref target, value);
        Assert.Equal(value, target);
        Assert.Equal(value, load(ref target));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void LdindAndStindSupportAllKinds(bool caching)
    {
        CheckIndirect(caching, -7);
        CheckIndirect(caching, (short)-7);
        CheckIndirect(caching, (sbyte)-7);
        CheckIndirect(caching, -7L);
        CheckIndirect(caching, (ushort)65535);
        CheckIndirect(caching, (byte)255);
        CheckIndirect(caching, uint.MaxValue);
        CheckIndirect(caching, ulong.MaxValue);
        CheckIndirect(caching, 2.5f);
        CheckIndirect(caching, 2.5);
        CheckIndirect(caching, "ref");
        CheckIndirect(caching, new DateTime(2024, 1, 2));
    }

    [Fact]
    public void IndirectAndElementOpCodesAreChosenByType()
    {
        Assert.Equal("ldind.u2", Single(il => il.Ldind(typeof(ushort))).OpCode.Name);
        Assert.Equal("ldind.i8", Single(il => il.Ldind(typeof(ulong))).OpCode.Name);
        Assert.Equal("ldobj", Single(il => il.Ldind(typeof(Guid))).OpCode.Name);
        Assert.Equal("stind.i2", Single(il => il.Stind(typeof(ushort))).OpCode.Name);
        Assert.Equal("stind.i1", Single(il => il.Stind(typeof(byte))).OpCode.Name);
        Assert.Equal("stobj", Single(il => il.Stind(typeof(Guid))).OpCode.Name);
        Assert.Equal("ldelem.u4", Single(il => il.Ldelem(typeof(uint))).OpCode.Name);
        Assert.Equal("ldelem.ref", Single(il => il.Ldelem(typeof(string))).OpCode.Name);
        Assert.Equal("ldelem", Single(il => il.Ldelem(typeof(ulong))).OpCode.Name);
        Assert.Equal("stelem.r8", Single(il => il.Stelem(typeof(double))).OpCode.Name);
        Assert.Equal("stelem", Single(il => il.Stelem(typeof(byte))).OpCode.Name);
        Assert.Equal("break", Single(il => il.Break()).OpCode.Name);
        Assert.Equal("tail.", Single(il => il.Tail()).OpCode.Name);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void FieldAccessHelpersWork(bool caching)
    {
        var field = typeof(Holder).GetField(nameof(Holder.Field))!;
        var staticField = typeof(Holder).GetField(nameof(Holder.StaticField))!;
        var holder = new Holder { Field = 5 };
        Assert.Equal(5, Build<Func<Holder, int>>(caching, il => il.Ldarg(0).Ldfld(field).Ret())(holder));
        Build<Action<Holder, int>>(caching, il => il.Ldarg(0).Ldarg(1).Stfld(field).Ret())(holder, 9);
        Assert.Equal(9, holder.Field);
        Build<Action<Holder>>(caching, il => il.Ldarg(0).Ldflda(field).LdcI4(77).Stind(typeof(int)).Ret())(holder);
        Assert.Equal(77, holder.Field);
        Assert.Equal(77, Build<Func<Holder, int>>(caching, il => il.Ldarg(0).Ldfld(() => holder.Field).Ret())(holder));
        Build<Action<int>>(caching, il => il.Ldarg(0).Stsfld(staticField).Ret())(31);
        Assert.Equal(31, Holder.StaticField);
        Assert.Equal(31, Build<Func<int>>(caching, il => il.Ldsfld(staticField).Ret())());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BoxingAndCastingHelpersWork(bool caching)
    {
        Assert.Equal((object)42, Build<Func<int, object>>(caching, il => il.Ldarg(0).Box(typeof(int)).Ret())(42));
        var unboxAny = Build<Func<object, int>>(caching, il => il.Ldarg(0).UnboxAny(typeof(int)).Ret());
        Assert.Equal(7, unboxAny(7));
        Assert.Throws<InvalidCastException>(() => unboxAny("x"));
        Assert.Throws<NullReferenceException>(() => unboxAny(null!));
        Assert.Equal(8, Build<Func<object, int>>(caching, il => il.Ldarg(0).Unbox(typeof(int)).Ldind(typeof(int)).Ret())(8));
        var castClass = Build<Func<object, string>>(caching, il => il.Ldarg(0).Castclass(typeof(string)).Ret());
        Assert.Equal("s", castClass("s"));
        Assert.Null(castClass(null!));
        Assert.Throws<InvalidCastException>(() => castClass(1));
        var isInst = Build<Func<object, string>>(caching, il => il.Ldarg(0).Isinst(typeof(string)).Ret());
        Assert.Equal("s", isInst("s"));
        Assert.Null(isInst(1));
    }

    [Fact]
    public void UnboxRejectsReferenceTypes()
    {
        var rec = new RecordingILGen();
        Assert.Throws<ArgumentException>(() => rec.Unbox(typeof(string)));
        Assert.Empty(rec.Emitted);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MiscellaneousOpCodesWork(bool caching)
    {
        Assert.Equal(Guid.Empty, Build<Func<Guid>>(caching, il =>
        {
            var local = il.DeclareLocal(typeof(Guid), "g");
            il.Ldloca(local).Do(g => g.InitObj(typeof(Guid))).Ldloc(local).Ret();
        })());
        Assert.Equal(typeof(string).TypeHandle,
            Build<Func<RuntimeTypeHandle>>(caching, il => il.Ldtoken(typeof(string)).Ret())());
        Assert.Equal(6, Build<Func<int, int>>(caching, il => il.Ldarg(0).Dup().Add().Ret())(3));
        Assert.Equal(1, Build<Func<int>>(caching, il => il.LdcI4(1).LdcI4(2).Pop().Ret())());
        Assert.Equal(1234, Build<Func<int>>(caching, il => il
            .Localloc(16).Dup().LdcI4(1234).Stind(typeof(int)).Ldind(typeof(int)).Ret())());
        Assert.Equal(99, Build<Func<int>>(caching, il => il
            .LdcI4(8).Do(g => g.Emit(OpCodes.Conv_U)).Localloc().Dup().LdcI4(99).Stind(typeof(int))
            .Ldind(typeof(int)).Ret())());
        Assert.Equal(5, Build<Func<int, int>>(caching, il => il
            .Comment("tail call").Ldarg(0).Tail().Call(() => Math.Abs(0)).Ret())(-5));
        var ex = Assert.Throws<InvalidOperationException>(Build<Action>(caching, il => il
            .Ldstr("boom").Newobj(() => new InvalidOperationException("")).Throw()));
        Assert.Equal("boom", ex.Message);
        Assert.Null(Build<Func<string>>(caching, il => il.Ldnull().Ret())());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CallHelpersFromExpressionsWork(bool caching)
    {
        Assert.Equal(42, Build<Func<int>>(caching, il => il.Call(() => Holder.StaticProp).Ret())());
        Assert.Equal(3, Build<Func<int>>(caching, il => il.LdcI4(-3).Call(() => Math.Abs(0)).Ret())());
        Assert.Equal("5", Build<Func<int, string>>(caching, il =>
        {
            var local = il.DeclareLocal(typeof(int));
            il.Ldarg(0).Stloc(local).Ldloca(local).Constrained(typeof(int))
                .Callvirt(() => default(object)!.ToString()).Ret();
        })(5));
        Assert.Equal(6, Build<Func<Holder, int>>(caching, il => il
            .Ldarg(0).Callvirt(() => default(Holder)!.InstanceProp).Ret())(new Holder { Field = 3 }));
        Assert.Equal(3, Build<Func<string, int>>(caching, il => il
            .Ldarg(0).Callvirt(() => default(string)!.Length).Ret())("abc"));
        var disposed = new DisposableProbe();
        Build<Action<IDisposable>>(caching, il => il
            .Ldarg(0).Callvirt(() => default(IDisposable)!.Dispose()).Ret())(disposed);
        Assert.True(disposed.Disposed);
        var obj = Build<Func<object>>(caching, il => il.Newobj(() => new List<int>()).Ret())();
        Assert.IsType<List<int>>(obj);
        Build<Action<object>>(caching, il => il.Ldarg(0).Call(() => GC.KeepAlive(null)).Ret())(obj);
        var func = Build<Func<Func<int>>>(caching, il => il
            .Ldnull().Ldftn(typeof(ILGenExtensionsCoverageTest).GetMethod(nameof(Answer),
                BindingFlags.Static | BindingFlags.NonPublic)!)
            .Newobj(typeof(Func<int>).GetConstructors()[0]).Ret())();
        Assert.Equal(42, func());
    }

    static int Answer() => 42;

    sealed class DisposableProbe : IDisposable
    {
        public bool Disposed;
        public void Dispose() => Disposed = true;
    }

    [Fact]
    public void CallvirtRejectsStaticMethod()
    {
        var rec = new RecordingILGen();
        Assert.Throws<ArgumentException>(() =>
            rec.Callvirt(typeof(Math).GetMethod(nameof(Math.Abs), [typeof(int)])!));
    }

    [Fact]
    public void CallFromNewExpressionsEmitsConstructorCall()
    {
        var ctor = typeof(object).GetConstructor(Type.EmptyTypes);
        Assert.Equal((OpCodes.Call, (object?)ctor), Single(il => il.Call(() => new object())));
        Assert.Equal((OpCodes.Call, (object?)ctor), Single(il => il.Call<object>(() => new object())));
    }

    [Theory]
    [InlineData(false, 10, 10, 1)]
    [InlineData(false, 0, -1, 1)]
    [InlineData(true, 4, 25, 1)]
    [InlineData(true, 0, -1, 1)]
    public void TryCatchFinallyWorks(bool caching, int divisor, int expected, int finallyCount)
    {
        var counter = typeof(TryCounter).GetField(nameof(TryCounter.Count))!;
        TryCounter.Count = 0;
        var f = Build<Func<int, int>>(caching, il =>
        {
            var result = il.DeclareLocal(typeof(int), "result");
            il
                .Try()
                .LdcI4(100).Ldarg(0).Div().Stloc(result)
                .Catch(typeof(DivideByZeroException))
                .Pop().LdcI4(-1).Stloc(result)
                .Finally()
                .Ldsfld(counter).LdcI4(1).Add().Stsfld(counter)
                .EndTry()
                .Ldloc(result).Ret();
        });
        Assert.Equal(expected, f(divisor));
        Assert.Equal(finallyCount, TryCounter.Count);
    }

    static class TryCounter
    {
        public static int Count;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DeclaredLocalsExposeTheirProperties(bool caching)
    {
        var method = Builder(caching).NewMethod<Func<int>>("Locals");
        var il = method.Generator;
        var l0 = il.DeclareLocal(typeof(int), "a");
        var l1 = il.DeclareLocal(typeof(byte[]), "pinned", true);
        Assert.Equal(0, l0.Index);
        Assert.Equal(1, l1.Index);
        Assert.False(l0.Pinned);
        Assert.True(l1.Pinned);
        Assert.Equal(typeof(int), l0.LocalType);
        Assert.Equal(typeof(byte[]), l1.LocalType);
        il.LdcI4(3).Stloc(l0).Ldloc(l0).Ret();
        Assert.Equal(3, method.Create()());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MethodPropertiesCanBeSet(bool caching)
    {
        var method = Builder(caching).NewMethod<Func<int>>("Props");
        Assert.True(method.InitLocals);
        method.InitLocals = false;
        Assert.False(method.InitLocals);
        method.ExpectedLength(256);
        method.Generator.LdcI4(11).Ret();
        Assert.Equal(11, method.Create()());

        var untyped = Builder(caching).NewMethod("Untyped", typeof(Func<int>));
        untyped.ExpectedLength(16);
        untyped.InitLocals = false;
        untyped.Generator.LdcI4(12).Ret();
        Assert.Equal(12, ((Func<int>)untyped.Create())());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void NewMethodValidatesArguments(bool caching)
    {
        var builder = Builder(caching);
        Assert.Throws<ArgumentException>(() => builder.NewMethod("x", typeof(string)));
        Assert.Throws<ArgumentException>(() => builder.NewMethod("x", typeof(int), typeof(Holder)));
        Assert.Throws<ArgumentNullException>(() => builder.NewMethod("x", typeof(Func<int>), null!));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MethodWithThisBindsFirstArgument(bool caching)
    {
        var method = Builder(caching).NewMethod("WithThis", typeof(Func<int, int>), typeof(Holder));
        method.InitLocals = true;
        Assert.True(method.InitLocals);
        method.ExpectedLength(32);
        method.Generator.Ldarg(0).Ldfld(typeof(Holder).GetField(nameof(Holder.Field))!).Ldarg(1).Add().Ret();
        method.FinalizeCreation();
        var f = (Func<int, int>)method.Create(new Holder { Field = 10 });
        Assert.Equal(15, f(5));
        var g = (Func<int, int>)method.Create(new Holder { Field = 20 });
        Assert.Equal(25, g(5));
    }

    [Fact]
    public void PublicDynamicMethodWithThisImplWorks()
    {
        var method = new ILDynamicMethodWithThisImpl("WithThisImpl", typeof(Func<string>), typeof(Holder));
        method.ExpectedLength(32);
        method.InitLocals = false;
        Assert.False(method.InitLocals);
        method.Generator.Ldarg(0).Ldfld(typeof(Holder).GetField(nameof(Holder.Name))!).Ret();
        method.FinalizeCreation();
        var f = (Func<string>)method.Create(new Holder { Name = "bound" });
        Assert.Equal("bound", f());
    }

    [Fact]
    public void ReleaseDynamicMethodExposesInvokeAsTrueMethodInfo()
    {
        var method = (ILDynamicMethodImpl)new ILBuilderRelease().NewMethod("x", typeof(Func<int, string>));
        Assert.Equal("Invoke", method.TrueMethodInfo.Name);
        Assert.Equal(typeof(string), method.TrueMethodInfo.ReturnType);
    }

    [Fact]
    public void CachingBuilderReusesIdenticalMethods()
    {
        var builder = new CachingILBuilder(new ILBuilderRelease());
        Func<int, int> Make(string name, int constant, bool initLocals = true)
        {
            var m = builder.NewMethod<Func<int, int>>(name);
            m.InitLocals = initLocals;
            var local = m.Generator.DeclareLocal(typeof(int), "l");
            var label = m.Generator.DefineLabel("lbl");
            m.Generator.Ldarg(0).LdcI4(constant).Add().Stloc(local).Br(label).Mark(label).Ldloc(local).Ret();
            return m.Create();
        }

        var a = Make("m", 1);
        var b = Make("m", 1);
        var c = Make("m", 2);
        var d = Make("other", 1);
        var e = Make("m", 1, false);
        Assert.Same(a, b);
        Assert.NotSame(a, c);
        Assert.NotSame(a, d);
        Assert.NotSame(a, e);
        Assert.Equal(11, a(10));
        Assert.Equal(12, c(10));
        Assert.Equal(11, d(10));
        Assert.Equal(11, e(10));
    }

    [Fact]
    public void CachingBuilderReusesIdenticalMethodsWithThis()
    {
        var builder = new CachingILBuilder(new ILBuilderRelease());
        Func<int> Make(object @this, string constant)
        {
            var m = builder.NewMethod("WithThis", typeof(Func<int>), typeof(string));
            m.Generator.Ldarg(0).Ldstr(constant).Call(() => string.Concat("", "")).Callvirt(() => "".Length).Ret();
            return (Func<int>)m.Create(@this);
        }

        var a = Make("ab", "c");
        var b = Make("abcd", "c");
        var c = Make("ab", "cd");
        Assert.Same(a.Method, b.Method);
        Assert.NotSame(a.Method, c.Method);
        Assert.Equal(3, a());
        Assert.Equal(5, b());
        Assert.Equal(4, c());
    }

    [Fact]
    public void CachingBuilderDistinguishesNegativeZeroAndNaNConstants()
    {
        var builder = new CachingILBuilder(new ILBuilderRelease());
        double MakeDouble(double value)
        {
            var m = builder.NewMethod<Func<double>>("d");
            m.Generator.LdcR8(value).Ret();
            return m.Create()();
        }

        float MakeFloat(float value)
        {
            var m = builder.NewMethod<Func<float>>("f");
            m.Generator.LdcR4(value).Ret();
            return m.Create()();
        }

        Assert.False(double.IsNegative(MakeDouble(0.0)));
        Assert.True(double.IsNegative(MakeDouble(-0.0)));
        Assert.False(float.IsNegative(MakeFloat(0.0f)));
        Assert.True(float.IsNegative(MakeFloat(-0.0f)));
        Assert.True(double.IsNaN(MakeDouble(double.NaN)));
        Assert.True(double.IsNaN(MakeDouble(double.NaN)));
    }

    public static IEnumerable<object[]> DifferentInstructionPairs()
    {
        var field1 = typeof(Holder).GetField(nameof(Holder.Field))!;
        var field2 = typeof(Holder).GetField(nameof(Holder.StaticField))!;
        var ctor1 = typeof(object).GetConstructor(Type.EmptyTypes)!;
        var ctor2 = typeof(List<int>).GetConstructor(Type.EmptyTypes)!;
        var method1 = typeof(Math).GetMethod(nameof(Math.Abs), [typeof(int)])!;
        var method2 = typeof(Math).GetMethod(nameof(Math.Abs), [typeof(long)])!;
        Action<IILGen>[] actions =
        [
            il => il.Emit(OpCodes.Ret),
            il => il.Emit(OpCodes.Nop),
            il => il.Emit(OpCodes.Ldc_I4_S, (sbyte)1),
            il => il.Emit(OpCodes.Ldc_I4_S, (sbyte)2),
            il => il.Emit(OpCodes.Ldarg_S, (byte)1),
            il => il.Emit(OpCodes.Ldarg_S, (byte)2),
            il => il.Emit(OpCodes.Ldarg, (ushort)300),
            il => il.Emit(OpCodes.Ldarg, (ushort)301),
            il => il.Emit(OpCodes.Ldc_I4, 1000),
            il => il.Emit(OpCodes.Ldc_I4, 1001),
            il => il.Emit(OpCodes.Ldc_I8, 1L),
            il => il.Emit(OpCodes.Ldc_I8, 2L),
            il => il.Emit(OpCodes.Ldc_R4, 1f),
            il => il.Emit(OpCodes.Ldc_R4, 2f),
            il => il.Emit(OpCodes.Ldc_R8, 1d),
            il => il.Emit(OpCodes.Ldc_R8, 2d),
            il => il.Emit(OpCodes.Ldfld, field1),
            il => il.Emit(OpCodes.Ldfld, field2),
            il => il.Emit(OpCodes.Newobj, ctor1),
            il => il.Emit(OpCodes.Newobj, ctor2),
            il => il.Emit(OpCodes.Call, method1),
            il => il.Emit(OpCodes.Call, method2),
            il => il.Emit(OpCodes.Box, typeof(int)),
            il => il.Emit(OpCodes.Box, typeof(long)),
            il => il.Comment("a"),
            il => il.Comment("b"),
            il => il.Ldstr("a"),
            il => il.Ldstr("b"),
            il => il.Try(),
            il => il.Finally(),
            il => il.EndTry(),
            il => il.Catch(typeof(Exception)),
            il => il.Catch(typeof(ArgumentException)),
            il => il.DefineLabel("x"),
            il => il.DefineLabel("y"),
            il => il.DeclareLocal(typeof(int), "x"),
            il => il.DeclareLocal(typeof(long), "x"),
            il => il.DeclareLocal(typeof(int), "y"),
            il => il.DeclareLocal(typeof(int), "x", true),
        ];
        for (var i = 0; i < actions.Length; i++)
        {
            for (var j = 0; j < actions.Length; j++)
            {
                if (i == j) continue;
                yield return [i, j, actions[i], actions[j]];
            }
        }
    }

    [Fact]
    public void CachingILGenEqualityDistinguishesEveryInstructionKindAndParameter()
    {
        foreach (var pair in DifferentInstructionPairs())
        {
            var a = new CachingILGen();
            var b = new CachingILGen();
            ((Action<IILGen>)pair[2])(a);
            ((Action<IILGen>)pair[3])(b);
            Assert.False(a.Equals(b), $"Instructions {pair[0]} and {pair[1]} must differ");
            var c = new CachingILGen();
            ((Action<IILGen>)pair[2])(c);
            Assert.True(a.Equals(c), $"Instruction {pair[0]} must equal itself");
            Assert.Equal(a.GetHashCode(), c.GetHashCode());
        }

        Assert.False(new CachingILGen().Equals("not a gen"));
        var shorter = new CachingILGen();
        var longer = new CachingILGen();
        longer.Emit(OpCodes.Ret);
        Assert.False(shorter.Equals(longer));
    }

    [Fact]
    public void CachingILGenComparesLocalsAndLabelsByIndex()
    {
        CachingILGen Make(int localIndex, int labelIndex)
        {
            var gen = new CachingILGen();
            var locals = new[] { gen.DeclareLocal(typeof(int)), gen.DeclareLocal(typeof(int)) };
            var labels = new[] { gen.DefineLabel(), gen.DefineLabel() };
            gen.Ldloc(locals[localIndex]).Br(labels[labelIndex]).Mark(labels[labelIndex]);
            return gen;
        }

        Assert.True(Make(0, 0).Equals(Make(0, 0)));
        Assert.False(Make(0, 0).Equals(Make(1, 0)));
        Assert.False(Make(0, 0).Equals(Make(0, 1)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void NewEnumCreatesEnumWithLiterals(bool caching)
    {
        var builder = Builder(caching);
        var literals = new[] { new KeyValuePair<string, object>("A", 1), new KeyValuePair<string, object>("B", 5) };
        var type = builder.NewEnum("CoverageEnum<int>", typeof(int), literals);
        Assert.True(type.IsEnum);
        Assert.Equal(typeof(int), Enum.GetUnderlyingType(type));
        Assert.Equal(["A", "B"], Enum.GetNames(type));
        Assert.Equal(5, (int)Enum.Parse(type, "B"));
        Assert.DoesNotContain('<', type.Name);
        var again = builder.NewEnum("CoverageEnum<int>", typeof(int), literals.ToList());
        if (caching) Assert.Same(type, again);
        else Assert.NotSame(type, again);
        var different = builder.NewEnum("CoverageEnum<int>", typeof(int),
            [new KeyValuePair<string, object>("A", 2)]);
        Assert.NotSame(type, different);
        Assert.Equal(2, (int)Enum.Parse(different, "A"));
    }

    [Fact]
    public void UniqueNameSanitizesAndNumbersNames()
    {
        var baseName = "Uniq" + Guid.NewGuid().ToString("N");
        var first = ILBuilderRelease.UniqueName(baseName + "<A, B>");
        var second = ILBuilderRelease.UniqueName(baseName + "<A, B>");
        Assert.Equal(baseName + "_A_B_", first);
        Assert.Equal(baseName + "_A_B___1", second);
    }

    [Fact]
    public void GlobalBuilderInstancesAreAvailable()
    {
        Assert.IsType<CachingILBuilder>(ILBuilder.Instance);
        Assert.IsType<ILBuilderRelease>(ILBuilder.NoCachingInstance);
        Assert.Same(ILBuilder.NoCachingInstance, ((CachingILBuilder)ILBuilder.Instance).Wrapping);
    }

    [Fact]
    public void DynamicILDirectoryPathIsUnderTempAndSettable()
    {
        var original = DynamicILDirectoryPath.DynamicIL;
        try
        {
            Assert.Equal(System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dynamicIL"), original);
            DynamicILDirectoryPath.DynamicIL = "custom";
            Assert.Equal("custom", DynamicILDirectoryPath.DynamicIL);
        }
        finally
        {
            DynamicILDirectoryPath.DynamicIL = original;
        }
    }
}
