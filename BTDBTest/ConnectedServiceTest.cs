using System;
using System.Threading.Tasks;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ServiceLayer;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class ConnectedServiceTest
    {
        PipedTwoChannels _pipedTwoChannels;
        IService _first;
        IService _second;

        [SetUp]
        public void Setup()
        {
            _pipedTwoChannels = new PipedTwoChannels();
            _first = new Service(_pipedTwoChannels.First);
            _second = new Service(_pipedTwoChannels.Second);
            _pipedTwoChannels.Connect();
        }

        [TearDown]
        public void TearDown()
        {
            _pipedTwoChannels.Disconnect();
            _first.Dispose();
            _second.Dispose();
        }

        public interface IAdder
        {
            int Add(int a, int b);
        }

        public class Adder : IAdder
        {
            public int Add(int a, int b)
            {
                return a + b;
            }
        }

        [Test]
        public void BasicTest()
        {
            _first.RegisterMyService(new Adder());
            Assert.AreEqual(3, _second.QueryOtherService<IAdder>().Add(1, 2));
        }

        public interface IIface1
        {
            int Meth1(string param);
            string Meth2();
            bool Meth3(bool a, bool b);
            void Meth4();
        }

        public interface IIface1Async
        {
            Task<int> Meth1(string param);
            Task<string> Meth2();
            Task<bool> Meth3(bool a, bool b);
            Task Meth4();
        }

        public class Class1 : Adder, IIface1
        {
            internal bool Meth4Called;

            public int Meth1(string param)
            {
                return param.Length;
            }

            public string Meth2()
            {
                return "Hello World";
            }

            public bool Meth3(bool a, bool b)
            {
                return a && b;
            }

            public void Meth4()
            {
                Meth4Called = true;
            }
        }

        [Test]
        public void ServiceWithIAdderAndIIface1()
        {
            var service = new Class1();
            _first.RegisterMyService(service);
            Assert.AreEqual(5, _second.QueryOtherService<IAdder>().Add(10000, -9995));
            var i1 = _second.QueryOtherService<IIface1>();
            Assert.AreEqual(2, i1.Meth1("Hi"));
            Assert.AreEqual("Hello World", i1.Meth2());
            Assert.AreEqual(true, i1.Meth3(true, true));
            i1.Meth4();
            Assert.True(service.Meth4Called);
        }

        [Test]
        public void AutomaticConversionToAsyncIface()
        {
            var service = new Class1();
            _first.RegisterMyService(service);
            var i1 = _second.QueryOtherService<IIface1Async>();
            AssertIIface1Async(i1);
            Assert.True(service.Meth4Called);
        }

        static void AssertIIface1Async(IIface1Async i1)
        {
            Assert.AreEqual(2, i1.Meth1("Hi").Result);
            Assert.AreEqual("Hello World", i1.Meth2().Result);
            Assert.AreEqual(true, i1.Meth3(true, true).Result);
            i1.Meth4().Wait();
        }

        [Test]
        public void DelegateAsService()
        {
            _first.RegisterMyService((Func<double, double, double>)((a, b) => a + b));
            var d = _second.QueryOtherService<Func<double, double, double>>();
            Assert.AreEqual(31.0, d(10.5, 20.5), 1e-10);
        }

        public class Class1Async : IIface1Async
        {
            internal bool Meth4Called;

            public Task<int> Meth1(string param)
            {
                var tco = new TaskCompletionSource<int>();
                tco.SetResult(param.Length);
                return tco.Task;
            }

            public Task<string> Meth2()
            {
                return Task.Factory.StartNew(() => "Hello World");
            }

            public Task<bool> Meth3(bool a, bool b)
            {
                return Task.Factory.StartNew(() => a && b);
            }

            public Task Meth4()
            {
                return Task.Factory.StartNew(() => { Meth4Called = true; });
            }
        }

        [Test]
        public void AsyncIfaceOnServerSide()
        {
            var service = new Class1Async();
            _first.RegisterMyService(service);
            var i1 = _second.QueryOtherService<IIface1Async>();
            AssertIIface1Async(i1);
            Assert.True(service.Meth4Called);
        }

        [Test]
        public void MultipleServicesAndIdentity()
        {
            _first.RegisterMyService(new Adder());
            _first.RegisterMyService(new Class1Async());
            var adder = _second.QueryOtherService<IAdder>();
            Assert.AreSame(adder, _second.QueryOtherService<IAdder>());
            var i1 = _second.QueryOtherService<IIface1>();
            Assert.AreSame(i1, _second.QueryOtherService<IIface1>());
        }

        [Test]
        public void ClientServiceDeallocedWhenNotneeded()
        {
            _first.RegisterMyService(new Adder());
            var adder = _second.QueryOtherService<IAdder>();
            var weakAdder = new WeakReference(adder);
            adder = null;
            GC.Collect();
            GC.WaitForPendingFinalizers();
            Assert.False(weakAdder.IsAlive);
            adder = _second.QueryOtherService<IAdder>();
            Assert.AreEqual(2, adder.Add(1, 1));
        }

        [Test]
        public void IfaceToDelegateConversion()
        {
            _first.RegisterMyService(new Adder());
            var adder = _second.QueryOtherService<Func<int, int, int>>();
            Assert.AreEqual(5,adder(2,3));
        }

        [Test]
        public void DelegateToIfaceConversion()
        {
            _first.RegisterMyService((Func<int, int, int>)((a, b) => a + b));
            Assert.AreEqual(30, _second.QueryOtherService<IAdder>().Add(10, 20));
        }

    }
}