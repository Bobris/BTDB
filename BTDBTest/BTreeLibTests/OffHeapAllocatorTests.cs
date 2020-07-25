using System;
using BTDB.Allocators;
using Xunit;

namespace BTDBTest.BTreeLibTests
{
    public class OffHeapAllocatorTests
    {
        [Fact]
        public void HGlobalAllocatorReturnsPointerCanWriteInto()
        {
            var allocator = new HGlobalAllocator();
            var ptr = allocator.Allocate((IntPtr)4);
            unsafe
            {
                *(int*)ptr = 0x12345678;
                Assert.Equal(0x12345678, *(int*)ptr);
            }
            allocator.Deallocate(ptr);
        }

        [Fact(Skip = "Using HGlobalAllocator instead")]
        public void MallocAllocatorReturnsPointerCanWriteInto()
        {
            var allocator = new MallocAllocator();
            var ptr = allocator.Allocate((IntPtr)4);
            unsafe
            {
                *(int*)ptr = 0x12345678;
                Assert.Equal(0x12345678, *(int*)ptr);
            }
            allocator.Deallocate(ptr);
        }

        [Fact]
        public void LeakDetectorWorks()
        {
            var allocator = new LeakDetectorWrapperAllocator(new HGlobalAllocator());
            var ptr1 = allocator.Allocate((IntPtr)4);
            var ptr2 = allocator.Allocate((IntPtr)8);
            var ptr3 = allocator.Allocate((IntPtr)16);
            allocator.Deallocate(ptr2);
            var leaks = allocator.QueryAllocations();
            Assert.Equal(2u, leaks.Count);
            Assert.Equal(20ul, leaks.Size);
            allocator.Deallocate(ptr1);
            allocator.Deallocate(ptr3);
            Assert.Throws<InvalidOperationException>(() => allocator.Deallocate(ptr1));
        }

        [Fact]
        public void LeakDetectorDisposesLeaks()
        {
            var mainAllocator = new LeakDetectorWrapperAllocator(new HGlobalAllocator());
            var allocator = new LeakDetectorWrapperAllocator(mainAllocator);
            allocator.Allocate((IntPtr)4);
            allocator.Dispose();
            Assert.Equal(0u, mainAllocator.QueryAllocations().Count);
        }
    }
}
