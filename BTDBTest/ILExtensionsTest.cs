using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using NUnit.Framework;
using BTDB.IL;

namespace BTDBTest
{
    [TestFixture]
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
                Assert.Fail();
            }

            int PrivateProperty { get; set; }
        }

        [Test]
        public void NoILWay()
        {
            var n = new Nested();
            n.Fun("Test");
            Assert.AreEqual("Test", n.PassedParam);
        }

        [Test]
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
            Assert.AreEqual("Test", n.PassedParam);
        }

        [Test]
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
            Assert.AreEqual("Test", n.PassedParam);
        }

        [Test]
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
            Assert.AreEqual("Test", n.PassedParam);
        }

        [Test]
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
            Assert.AreEqual(42, d());
        }

        [Test]
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
            Assert.AreEqual("Test", n.PassedParam);
        }

        [Test]
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
            Assert.AreEqual("Test", n.PassedParam);
        }
    
        public class PrivateConstructor
        {
            int _a;

            private PrivateConstructor(int a)
            {
                _a = a;
            }

            public int A
            {
                get { return _a; }
            }
        }

        [Test]
        public void CanCallPrivateConstructor()
        {
            var method = new ILBuilderDebug().NewMethod<Func<PrivateConstructor>>("PrivateConstructorCall");
            var il = method.Generator;
            il
                .LdcI4(42)
                .Newobj(typeof (PrivateConstructor).GetConstructors(BindingFlags.NonPublic|BindingFlags.Instance)[0])
                .Ret();
            Assert.AreEqual(42, method.Create()().A);
        }

    }
}
