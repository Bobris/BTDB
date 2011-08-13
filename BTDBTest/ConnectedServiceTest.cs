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
    }
}