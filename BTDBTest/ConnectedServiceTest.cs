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

        public void Runner(object obj, AbstractBufferedReader reader, IServiceInternalServer server)
        {
            var resultId = reader.ReadVUInt32();
            try
            {
                var param1 = reader.ReadVInt32();
                var param2 = reader.ReadVInt32();
                var result = ((IAdder)obj).Add(param1, param2);
                var writer = server.StartResultMarshaling(resultId);
                writer.WriteVInt32(result);
                server.FinishResultMarshaling(writer);
            }
            catch (Exception ex)
            {
                server.ExceptionMarshaling(resultId, ex);
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