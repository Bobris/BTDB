using System;
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

    }
}