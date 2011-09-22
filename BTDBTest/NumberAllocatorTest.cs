using BTDB.Service;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class NumberAllocatorTest
    {
        [Test]
        public void BasicTest()
        {
            var a=new NumberAllocator(5);
            Assert.AreEqual(5, a.Allocate());
            Assert.AreEqual(6, a.Allocate());
            a.Deallocate(5);
            Assert.AreEqual(5, a.Allocate());
            Assert.AreEqual(7, a.Allocate());
        }
    }
}