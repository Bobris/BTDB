using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using BTDB.IL;
using BTDB.IL.Caching;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest.ILBuilderTest;

public class ILDynamicTypeCoverageTest
{
    public interface IValueProvider
    {
        int GetValue(int add);
    }

    public class Target
    {
        public int Seed = 1;

        int Secret(int x) => x * 10 + Seed;

        internal virtual int VirtualSecret(int x) => x + 100;

        int TooMany(int a, int b, int c, int d, int e) => a + b + c + d + e;
    }

    public class DerivedTarget : Target
    {
        internal override int VirtualSecret(int x) => x + 200;
    }

    internal class InternalWithInternalCtor
    {
        internal InternalWithInternalCtor(int a)
        {
            A = a;
        }

        public int A { get; }
    }

    public class PublicWithPrivateCtor
    {
        PublicWithPrivateCtor(string s)
        {
            S = s;
        }

        public string S { get; }
    }

    internal class InternalWithPublicCtor
    {
        public int B { get; set; } = 7;
    }

    public class PublicWithPublicCtor
    {
        public int C { get; } = 3;
    }

    static IILBuilder Builder(bool caching) =>
        caching ? new CachingILBuilder(new ILBuilderRelease()) : new ILBuilderRelease();

    static string UniqueTypeName(string prefix) => prefix + Guid.NewGuid().ToString("N");

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DynamicTypeWithFieldsConstructorsMethodsAndOverridesWorks(bool caching)
    {
        var type = BuildValueProviderTypeSimple(Builder(caching), UniqueTypeName("ValueProvider"), 1000);
        var counter = type.GetField("Counter")!;
        var instance = (IValueProvider)Activator.CreateInstance(type, 5)!;
        Assert.Equal(1105, instance.GetValue(100));
        Assert.Equal(1, counter.GetValue(null));
        Activator.CreateInstance(type, 6);
        Assert.Equal(2, counter.GetValue(null));
        var ctorParameters = type.GetConstructors().Single().GetParameters();
        Assert.Equal("value", ctorParameters.Single().Name);
        var func = (Func<int>)type.GetMethod("GetFunc")!.Invoke(null, null)!;
        Assert.Equal(1000, func());
    }

    static Type BuildValueProviderTypeSimple(IILBuilder builder, string name, int constant, bool withLdftn = true)
    {
        var typeBuilder = builder.NewType(name, typeof(object), [typeof(IValueProvider)]);
        var field = typeBuilder.DefineField("_value", typeof(int), FieldAttributes.Private);
        Assert.Equal("_value", field.Name);
        Assert.Equal(typeof(int), field.FieldType);
        var staticField =
            typeBuilder.DefineField("Counter", typeof(int), FieldAttributes.Public | FieldAttributes.Static);
        var ctor = typeBuilder.DefineConstructor([typeof(int)], ["value"]);
        ctor.ExpectedLength(32);
        ctor.InitLocals = false;
        Assert.False(ctor.InitLocals);
        ctor.Generator
            .Ldarg(0).Call(() => new object())
            .Ldarg(0).Ldarg(1).Stfld(field)
            .Ldsfld(staticField).LdcI4(1).Add().Stsfld(staticField)
            .Ret();
        var method = typeBuilder.DefineMethod("GetValue", typeof(int), [typeof(int)],
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.NewSlot |
            MethodAttributes.Final | MethodAttributes.HideBySig);
        method.ExpectedLength(64);
        method.InitLocals = true;
        Assert.True(method.InitLocals);
        method.Generator
            .Ldarg(0).Ldflda(field).Ldind(typeof(int))
            .Ldarg(1).Add()
            .LdcI4(constant).Add()
            .Ret();
        typeBuilder.DefineMethodOverride(method,
            typeof(IValueProvider).GetMethod(nameof(IValueProvider.GetValue))!);
        var target = typeBuilder.DefineMethod("Constant", typeof(int), Type.EmptyTypes,
            MethodAttributes.Public | MethodAttributes.Static);
        target.Generator.LdcI4(constant).Ret();
        if (withLdftn)
        {
            var getFunc = typeBuilder.DefineMethod("GetFunc", typeof(Func<int>), Type.EmptyTypes,
                MethodAttributes.Public | MethodAttributes.Static);
            getFunc.Generator.Ldnull().Ldftn(target).Newobj(typeof(Func<int>).GetConstructors()[0]).Ret();
        }

        return typeBuilder.CreateType();
    }

    [Fact]
    public void ReleaseMethodExposesSignature()
    {
        var typeBuilder = new ILBuilderRelease().NewType(UniqueTypeName("Signature"), typeof(object), Type.EmptyTypes);
        var method = (IILMethodPrivate)typeBuilder.DefineMethod("M", typeof(long), [typeof(int), typeof(string)],
            MethodAttributes.Public | MethodAttributes.Static);
        Assert.Equal(typeof(long), method.ReturnType);
        Assert.Equal("M", method.TrueMethodInfo.Name);
        method.Generator.Ldarg(0).ConvI8().Ret();
        var type = typeBuilder.CreateType();
        Assert.Equal(5L, type.GetMethod("M")!.Invoke(null, [5, "x"]));
    }

    [Fact]
    public void CachingMethodExposesSignatureBeforeCreation()
    {
        var typeBuilder = new CachingILBuilder(new ILBuilderRelease())
            .NewType(UniqueTypeName("Signature"), typeof(object), Type.EmptyTypes);
        var method = (IILMethodPrivate)typeBuilder.DefineMethod("M", typeof(long), [typeof(int), typeof(string)]);
        Assert.Equal(typeof(long), method.ReturnType);
        Assert.Equal([typeof(int), typeof(string)], method.Parameters);
    }

    [Fact]
    public void CachingBuilderReusesStructurallyIdenticalTypes()
    {
        var builder = new CachingILBuilder(new ILBuilderRelease());
        var name = UniqueTypeName("Cached");
        var a = BuildValueProviderTypeSimple(builder, name, 1, false);
        var b = BuildValueProviderTypeSimple(builder, name, 1, false);
        var c = BuildValueProviderTypeSimple(builder, name, 2, false);
        Assert.Same(a, b);
        Assert.NotSame(a, c);
        // Ldftn of IILMethod is compared by reference, so types using it are never reused (only cache misses)
        var d = BuildValueProviderTypeSimple(builder, name, 1);
        var e = BuildValueProviderTypeSimple(builder, name, 1);
        Assert.NotSame(d, e);
        Assert.Equal(1, ((Func<int>)e.GetMethod("GetFunc")!.Invoke(null, null)!)());
        Assert.Equal(3, ((IValueProvider)Activator.CreateInstance(c, 0)!).GetValue(1));
        Assert.Equal(2, ((IValueProvider)Activator.CreateInstance(a, 0)!).GetValue(1));
    }

    public static IEnumerable<object[]> TypeVariations()
    {
        Action<IILDynamicType>[] variations =
        [
            t => t.DefineField("f", typeof(int), FieldAttributes.Public),
            t => t.DefineField("g", typeof(int), FieldAttributes.Public),
            t => t.DefineField("f", typeof(long), FieldAttributes.Public),
            t => t.DefineField("f", typeof(int), FieldAttributes.Private),
            t => t.DefineMethod("m", typeof(int), Type.EmptyTypes).Generator.LdcI4(1).Ret(),
            t => t.DefineMethod("m", typeof(int), Type.EmptyTypes).Generator.LdcI4(2).Ret(),
            t => t.DefineMethod("n", typeof(int), Type.EmptyTypes).Generator.LdcI4(1).Ret(),
            t => t.DefineMethod("m", typeof(long), Type.EmptyTypes).Generator.LdcI8(1).Ret(),
            t => t.DefineMethod("m", typeof(int), [typeof(int)]).Generator.LdcI4(1).Ret(),
            t => t.DefineMethod("m", typeof(int), Type.EmptyTypes, MethodAttributes.Public | MethodAttributes.Static)
                .Generator.LdcI4(1).Ret(),
            t =>
            {
                var m = t.DefineMethod("m", typeof(int), Type.EmptyTypes);
                m.InitLocals = false;
                m.Generator.LdcI4(1).Ret();
            },
            t => t.DefineConstructor([typeof(int)]).Generator.Ldarg(0).Call(() => new object()).Ret(),
            t => t.DefineConstructor([typeof(long)]).Generator.Ldarg(0).Call(() => new object()).Ret(),
            t => t.DefineConstructor([typeof(int)], ["x"]).Generator.Ldarg(0).Call(() => new object()).Ret(),
            t =>
            {
                var c = t.DefineConstructor([typeof(int)]);
                c.InitLocals = false;
                c.Generator.Ldarg(0).Call(() => new object()).Ret();
            },
            t => EmitHelpers.GenerateINotifyPropertyChangedImpl(t),
        ];
        for (var i = 0; i < variations.Length; i++)
        for (var j = 0; j < variations.Length; j++)
        {
            yield return [i, j, variations[i], variations[j]];
        }
    }

    [Fact]
    public void CachingDynamicTypeEqualityDistinguishesEveryMemberKind()
    {
        var builder = new CachingILBuilder(new ILBuilderRelease());
        foreach (var variation in TypeVariations())
        {
            var i = (int)variation[0];
            var j = (int)variation[1];
            var a = (CachingILDynamicType)builder.NewType("T", typeof(object),
                i == 15 ? [typeof(INotifyPropertyChanged)] : Type.EmptyTypes);
            var b = (CachingILDynamicType)builder.NewType("T", typeof(object),
                j == 15 ? [typeof(INotifyPropertyChanged)] : Type.EmptyTypes);
            ((Action<IILDynamicType>)variation[2])(a);
            ((Action<IILDynamicType>)variation[3])(b);
            Assert.Equal(i == j, a.Equals(b));
            if (i == j) Assert.Equal(a.GetHashCode(), b.GetHashCode());
        }

        var plain = builder.NewType("T", typeof(object), Type.EmptyTypes);
        Assert.False(plain.Equals("T"));
        Assert.False(plain.Equals(builder.NewType("U", typeof(object), Type.EmptyTypes)));
        Assert.False(plain.Equals(builder.NewType("T", typeof(Target), Type.EmptyTypes)));
        Assert.False(plain.Equals(builder.NewType("T", typeof(object), [typeof(IDisposable)])));
    }

    [Fact]
    public void CachingDynamicTypeMembersCompareStructurally()
    {
        var builder = new CachingILBuilder(new ILBuilderRelease());
        var t1 = builder.NewType("T", typeof(object), Type.EmptyTypes);
        var t2 = builder.NewType("T", typeof(object), Type.EmptyTypes);
        var f1 = t1.DefineField("f", typeof(int), FieldAttributes.Public);
        var f2 = t2.DefineField("f", typeof(int), FieldAttributes.Public);
        var m1 = t1.DefineMethod("m", typeof(void), Type.EmptyTypes);
        var m2 = t2.DefineMethod("m", typeof(void), Type.EmptyTypes);
        var c1 = t1.DefineConstructor(Type.EmptyTypes);
        var c2 = t2.DefineConstructor(Type.EmptyTypes);
        var e1 = t1.DefineEvent("E", EventAttributes.None, typeof(EventHandler));
        var e2 = t2.DefineEvent("E", EventAttributes.None, typeof(EventHandler));
        Assert.True(f1.Equals(f2));
        Assert.Equal(f1.GetHashCode(), f2.GetHashCode());
        Assert.True(m1.Equals(m2));
        Assert.Equal(m1.GetHashCode(), m2.GetHashCode());
        Assert.True(c1.Equals(c2));
        Assert.Equal(c1.GetHashCode(), c2.GetHashCode());
        Assert.True(e1.Equals(e2));
        Assert.Equal(e1.GetHashCode(), e2.GetHashCode());
        Assert.False(f1.Equals(m1));
        Assert.False(m1.Equals(f1));
        Assert.False(c1.Equals(m1));
        Assert.False(e1.Equals(f1));
        Assert.False(f1.Equals(null));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GeneratedINotifyPropertyChangedRaisesAndUnsubscribes(bool caching)
    {
        var typeBuilder = Builder(caching).NewType(UniqueTypeName("Notify"), typeof(object),
            [typeof(INotifyPropertyChanged)]);
        var raise = EmitHelpers.GenerateINotifyPropertyChangedImpl(typeBuilder);
        Assert.NotNull(raise);
        typeBuilder.DefineConstructor(Type.EmptyTypes).Generator.Ldarg(0).Call(() => new object()).Ret();
        var type = typeBuilder.CreateType();
        var instance = (INotifyPropertyChanged)Activator.CreateInstance(type)!;
        var raiseMethod = type.GetMethod("RaisePropertyChanged", BindingFlags.Instance | BindingFlags.NonPublic)!;
        raiseMethod.Invoke(instance, ["Nobody listens"]);
        var names = new List<string?>();
        PropertyChangedEventHandler handler = (sender, args) =>
        {
            Assert.Same(instance, sender);
            names.Add(args.PropertyName);
        };
        instance.PropertyChanged += handler;
        instance.PropertyChanged += handler;
        raiseMethod.Invoke(instance, ["A"]);
        instance.PropertyChanged -= handler;
        raiseMethod.Invoke(instance, ["B"]);
        instance.PropertyChanged -= handler;
        raiseMethod.Invoke(instance, ["C"]);
        Assert.Equal(["A", "A", "B"], names);
        Assert.NotNull(type.GetEvent("PropertyChanged"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DynamicTypeCanCallNonPublicInstanceMethods(bool caching)
    {
        var secret = typeof(Target).GetMethod("Secret", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var virtualSecret = typeof(Target).GetMethod("VirtualSecret", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var typeBuilder = Builder(caching).NewType(UniqueTypeName("Cheater"), typeof(object), Type.EmptyTypes);
        var callTwice = typeBuilder.DefineMethod("CallTwice", typeof(int), [typeof(Target), typeof(int)],
            MethodAttributes.Public | MethodAttributes.Static);
        callTwice.Generator
            .Ldarg(0).Ldarg(1).Call(secret)
            .Ldarg(0).Ldarg(1).Call(secret)
            .Add().Ret();
        var callVirtual = typeBuilder.DefineMethod("CallVirtual", typeof(int), [typeof(Target), typeof(int)],
            MethodAttributes.Public | MethodAttributes.Static);
        callVirtual.Generator.Ldarg(0).Ldarg(1).Callvirt(virtualSecret).Ret();
        var callPublic = typeBuilder.DefineMethod("CallPublic", typeof(string), [typeof(object)],
            MethodAttributes.Public | MethodAttributes.Static);
        callPublic.Generator.Ldarg(0).Callvirt(() => default(object)!.ToString()).Ret();
        var type = typeBuilder.CreateType();
        var target = new Target { Seed = 3 };
        Assert.Equal(2 * (4 * 10 + 3), type.GetMethod("CallTwice")!.Invoke(null, [target, 4]));
        Assert.Equal(105, type.GetMethod("CallVirtual")!.Invoke(null, [target, 5]));
        Assert.Equal(205, type.GetMethod("CallVirtual")!.Invoke(null, [new DerivedTarget(), 5]));
        Assert.Equal("x", type.GetMethod("CallPublic")!.Invoke(null, ["x"]));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DynamicTypeCanCreateInstancesUsingNonPublicConstructors(bool caching)
    {
        var internalCtor = typeof(InternalWithInternalCtor).GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)
            .Single();
        var privateCtor = typeof(PublicWithPrivateCtor).GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)
            .Single();
        var publicCtorOfInternal = typeof(InternalWithPublicCtor).GetConstructor(Type.EmptyTypes)!;
        var publicCtor = typeof(PublicWithPublicCtor).GetConstructor(Type.EmptyTypes)!;
        var typeBuilder = Builder(caching).NewType(UniqueTypeName("CtorCheater"), typeof(object), Type.EmptyTypes);
        typeBuilder.DefineMethod("MakeInternal", typeof(object), [typeof(int)],
                MethodAttributes.Public | MethodAttributes.Static)
            .Generator.Ldarg(0).Newobj(internalCtor).Pop().Ldarg(0).Newobj(internalCtor).Ret();
        typeBuilder.DefineMethod("MakePrivate", typeof(PublicWithPrivateCtor), [typeof(string)],
                MethodAttributes.Public | MethodAttributes.Static)
            .Generator.Ldarg(0).Newobj(privateCtor).Ret();
        typeBuilder.DefineMethod("MakePublicOfInternal", typeof(object), Type.EmptyTypes,
                MethodAttributes.Public | MethodAttributes.Static)
            .Generator.Newobj(publicCtorOfInternal).Ret();
        typeBuilder.DefineMethod("MakePublic", typeof(PublicWithPublicCtor), Type.EmptyTypes,
                MethodAttributes.Public | MethodAttributes.Static)
            .Generator.Newobj(publicCtor).Ret();
        var type = typeBuilder.CreateType();
        Assert.Equal(7, ((InternalWithInternalCtor)type.GetMethod("MakeInternal")!.Invoke(null, [7])!).A);
        Assert.Equal("s", ((PublicWithPrivateCtor)type.GetMethod("MakePrivate")!.Invoke(null, ["s"])!).S);
        Assert.Equal(7, ((InternalWithPublicCtor)type.GetMethod("MakePublicOfInternal")!.Invoke(null, null)!).B);
        Assert.Equal(3, ((PublicWithPublicCtor)type.GetMethod("MakePublic")!.Invoke(null, null)!).C);
    }

    [Fact]
    public void CheatingRejectsMethodsWithTooManyParameters()
    {
        var tooMany = typeof(Target).GetMethod("TooMany", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var typeBuilder = new ILBuilderRelease().NewType(UniqueTypeName("TooMany"), typeof(object), Type.EmptyTypes);
        var method = typeBuilder.DefineMethod("Call", typeof(int), [typeof(Target)],
            MethodAttributes.Public | MethodAttributes.Static);
        var il = method.Generator.Ldarg(0).LdcI4(1).LdcI4(2).LdcI4(3).LdcI4(4).LdcI4(5);
        var ex = Assert.Throws<BTDBException>(() => il.Call(tooMany));
        Assert.Contains("Unsupported number of parameters", ex.Message);
        Assert.Contains("TooMany", ex.Message);
    }

    [Fact]
    public void IsPublicRealChecksWholeNestingChain()
    {
        Assert.True(IilGenForbiddenInstructionsCheating.IsPublicReal(typeof(PublicWithPublicCtor)));
        Assert.True(IilGenForbiddenInstructionsCheating.IsPublicReal(typeof(string)));
        Assert.False(IilGenForbiddenInstructionsCheating.IsPublicReal(typeof(InternalWithPublicCtor)));
        Assert.False(IilGenForbiddenInstructionsCheating.IsPublicReal(typeof(PrivateOuter.PublicInner)));
    }

    class PrivateOuter
    {
        public class PublicInner
        {
        }
    }
}
