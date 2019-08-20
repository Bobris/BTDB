using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.Service;
using Xunit;

namespace BTDBTest
{
    public class ConnectedServiceTest : IDisposable
    {
        PipedTwoChannels _pipedTwoChannels;
        IService _first;
        IService _second;

        public ConnectedServiceTest()
        {
            _pipedTwoChannels = new PipedTwoChannels();
            _first = new Service(_pipedTwoChannels.First);
            _second = new Service(_pipedTwoChannels.Second);
        }

        public void Dispose()
        {
            Disconnect();
            _first.Dispose();
            _second.Dispose();
        }

        void Disconnect()
        {
            _pipedTwoChannels.Disconnect();
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

        [Fact]
        public void BasicTest()
        {
            _first.RegisterLocalService(new Adder());
            Assert.Equal(3, _second.QueryRemoteService<IAdder>().Add(1, 2));
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

        [Fact]
        public void ServiceWithIAdderAndIIface1()
        {
            var service = new Class1();
            _first.RegisterLocalService(service);
            Assert.Equal(5, _second.QueryRemoteService<IAdder>().Add(10000, -9995));
            var i1 = _second.QueryRemoteService<IIface1>();
            Assert.Equal(2, i1.Meth1("Hi"));
            Assert.Equal("Hello World", i1.Meth2());
            Assert.True(i1.Meth3(true, true));
            i1.Meth4();
            Assert.True(service.Meth4Called);
        }

        [Fact]
        public void OnNewServiceNotification()
        {
            var service = new Class1();
            var remoteServiceNames = new List<string>();
            _second.OnNewRemoteService.Subscribe(remoteServiceNames.Add);
            _first.RegisterLocalService(service);
            remoteServiceNames.Sort();
            Assert.Equal(new List<string> { "Class1", "IAdder", "IIface1" }, remoteServiceNames);
        }

        [Fact]
        public void AutomaticConversionToAsyncIface()
        {
            var service = new Class1();
            _first.RegisterLocalService(service);
            var i1 = _second.QueryRemoteService<IIface1Async>();
            AssertIIface1Async(i1);
            Assert.True(service.Meth4Called);
        }

        static void AssertIIface1Async(IIface1Async i1)
        {
            Assert.Equal(2, i1.Meth1("Hi").Result);
            Assert.Equal("Hello World", i1.Meth2().Result);
            Assert.True(i1.Meth3(true, true).Result);
            i1.Meth4().Wait();
        }

        [Fact]
        public void DelegateAsService()
        {
            _first.RegisterLocalService((Func<double, double, double>)((a, b) => a + b));
            var d = _second.QueryRemoteService<Func<double, double, double>>();
            AreDoubleEqual(31.0, d(10.5, 20.5), 1e-10);
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

        [Fact]
        public void AsyncIfaceOnServerSide()
        {
            var service = new Class1Async();
            _first.RegisterLocalService(service);
            var i1 = _second.QueryRemoteService<IIface1Async>();
            AssertIIface1Async(i1);
            Assert.True(service.Meth4Called);
        }

        [Fact]
        public void MultipleServicesAndIdentity()
        {
            _first.RegisterLocalService(new Adder());
            _first.RegisterLocalService(new Class1Async());
            var adder = _second.QueryRemoteService<IAdder>();
            Assert.Same(adder, _second.QueryRemoteService<IAdder>());
            var i1 = _second.QueryRemoteService<IIface1>();
            Assert.Same(i1, _second.QueryRemoteService<IIface1>());
        }

        [Fact(Skip = "Does not correctly work on Linux or Debug")]
        public void ClientServiceDeallocedWhenNotneeded()
        {
            _first.RegisterLocalService(new Adder());
            var weakAdder = new WeakReference(_second.QueryRemoteService<IAdder>());
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            Assert.False(weakAdder.IsAlive);
            var adder = _second.QueryRemoteService<IAdder>();
            Assert.Equal(2, adder.Add(1, 1));
        }

        [Fact]
        public void IfaceToDelegateConversion()
        {
            _first.RegisterLocalService(new Adder());
            var adder = _second.QueryRemoteService<Func<int, int, int>>();
            Assert.Equal(5, adder(2, 3));
        }

        [Fact]
        public void DelegateToIfaceConversion()
        {
            _first.RegisterLocalService((Func<int, int, int>)((a, b) => a + b));
            Assert.Equal(30, _second.QueryRemoteService<IAdder>().Add(10, 20));
        }

        public interface IIface2
        {
            int Invoke(string param);
            int Invoke(int param);
        }

        public class Class2 : IIface2
        {
            public int Invoke(string param)
            {
                return param.Length;
            }

            public int Invoke(int param)
            {
                return param * param;
            }
        }

        [Fact]
        public void BasicMethodOverloading()
        {
            _first.RegisterLocalService(new Class2());
            var i2 = _second.QueryRemoteService<IIface2>();
            Assert.Equal(9, i2.Invoke(3));
            Assert.Equal(2, i2.Invoke("Hi"));
        }

        public interface IIface3A
        {
            int Invoke(string param, string param2);
            int Invoke(int param2, int param);
        }

        public interface IIface3B
        {
            int Invoke(string param2, string param);
            int Invoke(int param, int param2);
        }

        public class Class3 : IIface3A
        {
            public int Invoke(string param, string param2)
            {
                return param.Length + 2 * (param2 ?? "").Length;
            }

            public int Invoke(int param2, int param)
            {
                return 2 * param2 + param;
            }
        }

        [Fact]
        public void ChangingNumberAndOrderOfParameters()
        {
            _first.RegisterLocalService(new Class3());
            var i2 = _second.QueryRemoteService<IIface2>();
            Assert.Equal(3, i2.Invoke(3));
            Assert.Equal(2, i2.Invoke("Hi"));
            var i3 = _second.QueryRemoteService<IIface3A>();
            Assert.Equal(10, i3.Invoke(3, 4));
            Assert.Equal(8, i3.Invoke("Hi", "Dev"));
            var i3O = _second.QueryRemoteService<IIface3B>();
            Assert.Equal(10, i3O.Invoke(4, 3));
            Assert.Equal(7, i3O.Invoke("Hi", "Dev"));
        }

        [Fact]
        public void ByteArraySupport()
        {
            _first.RegisterLocalService((Func<byte[], byte[]>)(p => p.AsEnumerable().Reverse().ToArray()));
            var d = _second.QueryRemoteService<Func<byte[], byte[]>>();
            Assert.Equal(new byte[] { 255, 3, 2, 1, 0 }, d(new byte[] { 0, 1, 2, 3, 255 }));
        }

        [Fact]
        public void ByteBufferSupport()
        {
            _first.RegisterLocalService((Func<ByteBuffer, ByteBuffer>)(p => ByteBuffer.NewAsync(p.ToByteArray().AsEnumerable().Reverse().ToArray())));
            var d = _second.QueryRemoteService<Func<byte[], byte[]>>();
            Assert.Equal(new byte[] { 255, 3, 2, 1, 0 }, d(new byte[] { 0, 1, 2, 3, 255 }));
        }

        [Fact]
        public void ByteBufferSupport2()
        {
            _first.RegisterLocalService((Func<byte[], byte[]>)(p => p.AsEnumerable().Reverse().ToArray()));
            var d = _second.QueryRemoteService<Func<ByteBuffer, ByteBuffer>>();
            Assert.Equal(new byte[] { 255, 3, 2, 1, 0 }, d(ByteBuffer.NewAsync(new byte[] { 0, 1, 2, 3, 255 })).ToByteArray());
        }

        [Fact]
        public void SimpleConversion()
        {
            _first.RegisterLocalService((Func<int, int>)(p => p * p));
            var d = _second.QueryRemoteService<Func<short, string>>();
            Assert.Equal("81", d(9));
        }

        [Fact]
        public void PassingException()
        {
            _first.RegisterLocalService((Func<int>)(() => { throw new ArgumentException("msg", "te" + "st"); }));
            var d = _second.QueryRemoteService<Func<int>>();
            var e = Assert.Throws<AggregateException>(() => d());
            Assert.Single(e.InnerExceptions);
            var inner = e.InnerExceptions[0];
            Assert.IsType<ArgumentException>(inner);
            Assert.StartsWith("msg", ((ArgumentException)inner).Message);
            Assert.Equal("test", ((ArgumentException)inner).ParamName);
        }

        [Fact]
        public void DisconnectionShouldCancelRunningMethods()
        {
            var e = new AutoResetEvent(false);
            _first.RegisterLocalService((Func<Task>)(() => Task.Factory.StartNew(() => e.WaitOne(1000))));
            var d = _second.QueryRemoteService<Func<Task>>();
            var task = d();
            task = task.ContinueWith(t => Assert.True(t.IsCanceled));
            Disconnect();
            task.Wait(1000);
            e.Set();
        }

        [Fact]
        public void SupportIListLongAsParameters()
        {
            _first.RegisterLocalService((Func<IList<long>, IList<long>>)(p => p.Reverse().ToList()));
            var d = _second.QueryRemoteService<Func<IList<long>, IList<long>>>();
            Assert.Equal(new List<long> { 3, 2, 1 }, d(new List<long> { 1, 2, 3 }));
        }

        [Fact]
        public void SupportIListIntAsParameters()
        {
            _first.RegisterLocalService((Func<IList<int>, IList<int>>)(p => p.Reverse().ToList()));
            var d = _second.QueryRemoteService<Func<IList<int>, IList<int>>>();
            Assert.Equal(new List<int> { 3, 2, 1 }, d(new List<int> { 1, 2, 3 }));
        }

        [Fact]
        public void SupportIDictionaryIntAsParameters()
        {
            _first.RegisterLocalService((Func<IDictionary<int, int>, IDictionary<int, int>>)(p => p.ToDictionary(kv => kv.Value, kv => kv.Key)));
            var d = _second.QueryRemoteService<Func<IDictionary<int, int>, IDictionary<int, int>>>();
            Assert.Equal(new Dictionary<int, int> { { 1, 5 }, { 2, 6 }, { 3, 7 } }, d(new Dictionary<int, int> { { 5, 1 }, { 6, 2 }, { 7, 3 } }));
        }

        public class SimpleDTO
        {
            public string Name { get; set; }
            public double Number { get; set; }
        }

        [Fact]
        public void SupportSimpleDTOAsParameter()
        {
            SimpleDTO received = null;
            _first.RegisterLocalService((Action<SimpleDTO>)(a => received = a));
            var d = _second.QueryRemoteService<Action<SimpleDTO>>();
            d(new SimpleDTO { Name = "Text", Number = 3.14 });
            Assert.NotNull(received);
            Assert.Equal("Text", received.Name);
            AreDoubleEqual(3.14, received.Number, 1e-14);
        }

        [Fact]
        public void SupportSimpleDTOAsResult()
        {
            _first.RegisterLocalService((Func<SimpleDTO>)(() => new SimpleDTO { Name = "Text", Number = 3.14 }));
            var d = _second.QueryRemoteService<Func<SimpleDTO>>();
            var received = d();
            Assert.NotNull(received);
            Assert.Equal("Text", received.Name);
            AreDoubleEqual(3.14, received.Number, 1e-14);
        }

        [Fact]
        public void CanReturnAndRegisterOtherTypes()
        {
            _first.RegisterLocalService((Func<object>)(() => new SimpleDTO { Name = "Text", Number = 3.14 }));
            var d = _second.QueryRemoteService<Func<object>>();
            _second.RegisterRemoteType(typeof(SimpleDTO));
            var received = (SimpleDTO)d();
            Assert.NotNull(received);
            Assert.Equal("Text", received.Name);
            AreDoubleEqual(3.14, received.Number, 1e-14);
        }

        [Fact]
        public void CanSendAndRegisterOtherTypes()
        {
            SimpleDTO received = null;
            _first.RegisterLocalService((Action<object>)(r => received = (SimpleDTO)r));
            _first.RegisterLocalType(typeof(SimpleDTO));
            var d = _second.QueryRemoteService<Action<object>>();
            d(new SimpleDTO { Name = "Text", Number = 3.14 });
            Assert.NotNull(received);
            Assert.Equal("Text", received.Name);
            AreDoubleEqual(3.14, received.Number, 1e-14);
        }

        [Fact]
        public void CanReturnComplexObjectsAsTask()
        {
            _first.RegisterLocalService((Func<List<int>, Task<List<int>>>)(p => Task.Factory.StartNew(() => p.AsEnumerable().Reverse().ToList())));
            var d = _second.QueryRemoteService<Func<List<int>, Task<List<int>>>>();
            Assert.Equal(new List<int> { 3, 2, 1 }, d(new List<int> { 1, 2, 3 }).Result);
        }

        void AreDoubleEqual(double expected, double value, double precision)
        {
            var diff = Math.Abs(expected - value);
            Assert.InRange(diff, - precision/2,  precision/2);
        }
    }
}