using BTDB.Service;
using Xunit;

namespace BTDBTest
{
    public class NumberAllocatorTest
    {
        [Fact]
        public void BasicTest()
        {
            var a=new NumberAllocator(5);
            Assert.Equal(5u, a.Allocate());
            Assert.Equal(6u, a.Allocate());
            a.Deallocate(5);
            Assert.Equal(5u, a.Allocate());
            Assert.Equal(7u, a.Allocate());
        }
    }
}