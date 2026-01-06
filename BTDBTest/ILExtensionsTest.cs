using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using Xunit;
using BTDB.IL;

namespace BTDBTest;

public class ILExtensionsTest
{
    public class Nested
    {
        public string PassedParam { get; private set; }

        public void Fun(string a)
        {
            PassedParam = a;
        }

        public void Fun(int noFun)
        {
            Assert.Fail("Fail");
        }

        int PrivateProperty { get; set; }
    }

    [Fact]
    public void NoILWay()
    {
        var n = new Nested();
        n.Fun("Test");
        Assert.Equal("Test", n.PassedParam);
    }

    [Fact]
    public void ILOldWay()
    {
        var method = new DynamicMethod("SampleCall", typeof(Nested), Type.EmptyTypes);
        var il = method.GetILGenerator();
        il.DeclareLocal(typeof(Nested));
        il.Emit(OpCodes.Newobj, typeof(Nested).GetDefaultConstructor()!);
        il.Emit(OpCodes.Stloc_0);
        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldstr, "Test");
        il.Emit(OpCodes.Call, typeof(Nested).GetMethod("Fun", new[] { typeof(string) })!);
        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ret);
        var action = (Func<Nested>)method.CreateDelegate(typeof(Func<Nested>));
        var n = action();
        Assert.Equal("Test", n.PassedParam);
    }

    [Fact]
    public void ILNewestWayRelease()
    {
        var method = new ILBuilderRelease().NewMethod<Func<Nested>>("SampleCall");
        var il = method.Generator;
        var local = il.DeclareLocal(typeof(Nested), "n");
        il
            .Newobj(() => new Nested())
            .Dup()
            .Stloc(local)
            .Ldstr("Test")
            .Call(() => default(Nested).Fun(""))
            .Ldloc(local)
            .Ret();
        var action = method.Create();
        var n = action();
        Assert.Equal("Test", n.PassedParam);
    }

    [Fact]
    public void ILNewestWayDebug()
    {
        var method = new ILBuilderRelease().NewMethod<Func<Nested>>("SampleCall");
        var il = method.Generator;
        var local = il.DeclareLocal(typeof(Nested), "n");
        il
            .Newobj(() => new Nested())
            .Dup()
            .Stloc(local)
            .Ldstr("Test")
            .Call(() => default(Nested).Fun(""))
            .Ldloc(local)
            .Ret();
        var action = method.Create();
        var n = action();
        Assert.Equal("Test", n.PassedParam);
    }

    [Fact]
    public void CanAccessPrivateProperties()
    {
        var method = new ILBuilderRelease().NewMethod<Func<int>>("PrivateAccess");
        var il = method.Generator;
        var local = il.DeclareLocal(typeof(Nested), "n");
        var propertyInfos = typeof(Nested).GetProperties(BindingFlags.Public | BindingFlags.NonPublic |
                                                         BindingFlags.Instance | BindingFlags.FlattenHierarchy);
        var propertyInfo = propertyInfos.First(p => p.Name == "PrivateProperty");
        il
            .Newobj(() => new Nested())
            .Dup()
            .Stloc(local)
            .LdcI4(42)
            .Call(propertyInfo.GetSetMethod(true)!)
            .Ldloc(local)
            .Call(propertyInfo.GetGetMethod(true)!)
            .Ret();
        var d = method.Create();
        Assert.Equal(42, d());
    }

    [Fact]
    public void CanFixFirstParameterRelease()
    {
        var method = new ILBuilderRelease().NewMethod("SampleCall", typeof(Func<Nested>), typeof(string));
        var il = method.Generator;
        var local = il.DeclareLocal(typeof(Nested), "n");
        il
            .Newobj(() => new Nested())
            .Dup()
            .Stloc(local)
            .Ldarg(0)
            .Call(() => default(Nested).Fun(""))
            .Ldloc(local)
            .Ret();
        var action = (Func<Nested>)method.Create("Test");
        var n = action();
        Assert.Equal("Test", n.PassedParam);
    }

    [Fact]
    public void CanFixFirstParameterDebug()
    {
        var method = ILBuilder.Instance.NewMethod("SampleCall", typeof(Func<Nested>), typeof(string));
        var il = method.Generator;
        var local = il.DeclareLocal(typeof(Nested), "n");
        il
            .Newobj(() => new Nested())
            .Dup()
            .Stloc(local)
            .Ldarg(0)
            .Call(() => default(Nested).Fun(""))
            .Ldloc(local)
            .Ret();
        var action = (Func<Nested>)method.Create("Test");
        var n = action();
        Assert.Equal("Test", n.PassedParam);
    }

    public class PrivateConstructor
    {
        readonly int _a;

        PrivateConstructor(int a)
        {
            _a = a;
        }

        public int A => _a;
    }

    [Fact]
    public void CanCallPrivateConstructor()
    {
        var method = new ILBuilderRelease().NewMethod<Func<PrivateConstructor>>("PrivateConstructorCall");
        var il = method.Generator;
        il
            .LdcI4(42)
            .Newobj(typeof(PrivateConstructor).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)[0])
            .Ret();
        Assert.Equal(42, method.Create()().A);
    }

    public int Factorial(int n)
    {
        var ret = n;
        while (n > 2)
            ret *= --n;
        return ret;
    }

    [Fact]
    public void FactorialWorks()
    {
        var method = new ILBuilderRelease().NewMethod<Func<int, int>>("FactorialIL");
        var il = method.Generator;
        var finish = il.DefineLabel();
        var next = il.DefineLabel();
        var ret = il.DeclareLocal(typeof(int), "ret");
        il
            .Ldarg(0) //[n]
            .Stloc(ret) //[]
            .Mark(next)
            .Ldarg(0) //[n]
            .LdcI4(2) // [ret, 2]
            .Blt(finish) //[]
            .Ldarg(0) //[n]
            .LdcI4(1) //[n, 1]
            .Sub() //[n-1]
            .Dup() //[n-1, n-1]
            .Starg(0) //[n-1]
            .Ldloc(ret) //[n-1, ret]
            .Mul() //[(n-1)*ret] -> ret
            .Stloc(ret) //[ret]
            .Br(next)
            .Mark(finish)
            .Ldloc(ret)
            .Ret();

        Assert.Equal(24, method.Create()(4));
    }

    [Fact]
    public void AllocationLessDictionaryIteration()
    {
        var method = new ILBuilderRelease().NewMethod<Func<Dictionary<int, int>, int>>("PrintDict");
        var il = method.Generator;
        var sumLocal = il.DeclareLocal(typeof(int), "sum");
        var dictType = typeof(Dictionary<int, int>);
        var getEnumeratorMethod =
            dictType.GetMethods().Single(m =>
                m.Name == "GetEnumerator" && m.ReturnType.IsValueType && m.GetParameters().Length == 0);
        var enumeratorType = getEnumeratorMethod.ReturnType;
        var moveNextMethod = enumeratorType.GetMethod("MoveNext");
        var currentGetter =
            enumeratorType.GetProperties()
                .Single(m => m.Name == "Current" && m.PropertyType.IsValueType)
                .GetGetMethod();
        var keyValuePairType = currentGetter!.ReturnType;
        var enumeratorLocal = il.DeclareLocal(enumeratorType);
        var keyValuePairLocal = il.DeclareLocal(keyValuePairType);
        var againLabel = il.DefineLabel("again");
        var finishedLabel = il.DefineLabel("finished");
        il
            .LdcI4(0)
            .Stloc(sumLocal)
            .Ldarg(0)
            .Callvirt(getEnumeratorMethod)
            .Stloc(enumeratorLocal)
            .Mark(againLabel)
            .Ldloca(enumeratorLocal)
            .Call(moveNextMethod!)
            .BrfalseS(finishedLabel)
            .Ldloca(enumeratorLocal)
            .Call(currentGetter)
            .Stloc(keyValuePairLocal)
            .Ldloca(keyValuePairLocal)
            .Call(keyValuePairType.GetProperty("Key")!.GetGetMethod()!)
            .Ldloc(sumLocal)
            .Add()
            .Stloc(sumLocal)
            .Ldloca(keyValuePairLocal)
            .Call(keyValuePairType.GetProperty("Value")!.GetGetMethod()!)
            .Ldloc(sumLocal)
            .Add()
            .Stloc(sumLocal)
            .BrS(againLabel)
            .Mark(finishedLabel)
            .Ldloca(enumeratorLocal)
            .Constrained(enumeratorType)
            .Callvirt(() => default(IDisposable).Dispose())
            .Ldloc(sumLocal)
            .Ret();
        Assert.Equal(10, method.Create()(new Dictionary<int, int> { { 1, 2 }, { 3, 4 } }));
    }

    [Fact(Skip = "Does not work anymore")]
    public void UninitializedLocalsWorks()
    {
        var method = new ILBuilderRelease().NewMethod<Func<int>>("Uninitialized");
        method.InitLocals = false;
        var il = method.Generator;
        il
            .Localloc(128)
            .LdcI4(96)
            .Add()
            .Ldind(typeof(int))
            .Ret();

        void MakeStackDirty()
        {
            Span<byte> stack = stackalloc byte[256];
            stack.Fill(42);
        }

        MakeStackDirty();
        Assert.NotEqual(0, method.Create()());
    }
}
