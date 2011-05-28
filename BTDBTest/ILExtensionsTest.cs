using System;
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
        public void ILNewWay()
        {
            var method = new DynamicMethod("SampleCall", typeof(Nested), Type.EmptyTypes);
            var il = method.GetILGenerator();
            il.DeclareLocal(typeof(Nested));
            il
                .Newobj(() => new Nested())
                .Stloc(0)
                .Ldloc(0)
                .Ldstr("Test")
                .Call(() => ((Nested)null).Fun(""))
                .Ldloc(0)
                .Ret();
            var action = method.CreateDelegate<Func<Nested>>();
            var n = action();
            Assert.AreEqual("Test", n.PassedParam);
        }

    }
}
