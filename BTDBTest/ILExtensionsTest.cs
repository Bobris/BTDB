using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using Xunit;
using BTDB.IL;

namespace BTDBTest
{
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
                Assert.True(false, "Fail");
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
            il.Emit(OpCodes.Newobj, typeof(Nested).GetConstructor(Type.EmptyTypes));
            il.Emit(OpCodes.Stloc_0);
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ldstr, "Test");
            il.Emit(OpCodes.Call, typeof(Nested).GetMethod("Fun", new[] { typeof(string) }));
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
            var method = new ILBuilderDebug().NewMethod<Func<Nested>>("SampleCall");
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
            var method = new ILBuilderDebug().NewMethod<Func<int>>("PrivateAccess");
            var il = method.Generator;
            var local = il.DeclareLocal(typeof(Nested), "n");
            var propertyInfos = typeof(Nested).GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.FlattenHierarchy);
            var propertyInfo = propertyInfos.First(p => p.Name == "PrivateProperty");
            il
                .Newobj(() => new Nested())
                .Dup()
                .Stloc(local)
                .LdcI4(42)
                .Call(propertyInfo.GetSetMethod(true))
                .Ldloc(local)
                .Call(propertyInfo.GetGetMethod(true))
                .Ret();
            var d = method.Create();
            Assert.Equal(42, d());
        }

        [Fact]
        public void CanFixFirstParameterRelease()
        {
            var method = new ILBuilderRelease().NewMethod("SampleCall", typeof(Func<Nested>),typeof(string));
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
            var method = new ILBuilderDebug().NewMethod("SampleCall", typeof(Func<Nested>), typeof(string));
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

            public int A
            {
                get { return _a; }
            }
        }

        [Fact]
        public void CanCallPrivateConstructor()
        {
            var method = new ILBuilderDebug().NewMethod<Func<PrivateConstructor>>("PrivateConstructorCall");
            var il = method.Generator;
            il
                .LdcI4(42)
                .Newobj(typeof (PrivateConstructor).GetConstructors(BindingFlags.NonPublic|BindingFlags.Instance)[0])
                .Ret();
            Assert.Equal(42, method.Create()().A);
        }

    }
}
