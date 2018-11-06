using BTDB.ARTLib;
using System;
using Xunit;

namespace ARTLibTest
{
    public class RootNodeTests: IDisposable
    {
        LeakDetectorWrapperAllocator _allocator;
        ARTImpl _impl;

        public RootNodeTests()
        {
            _allocator = new LeakDetectorWrapperAllocator(new HGlobalAllocator());
            _impl = new ARTImpl(_allocator, false);
        }

        public void Dispose()
        {
            var leaks = _allocator.QueryAllocations();
            Assert.Equal(0ul, leaks.Count);
        }

        [Fact]
        public void CouldBeCreated()
        {
            using (var root = new RootNode(_impl))
            {
            }
        }

        [Fact]
        public void CanCreateSnapshot()
        {
            using (var root = new RootNode(_impl))
            {
                using (var snapshot = root.Snapshot())
                {
                }
            }
        }

        [Fact]
        public void CanRevertToSnapshot()
        {
            using (var root = new RootNode(_impl))
            {
                using (var snapshot = root.Snapshot())
                {
                    root.RevertTo(snapshot);
                }
            }
        }

        [Fact]
        public void ItIsForbiddenToRevertSnapshot()
        {
            using (var root = new RootNode(_impl))
            {
                using (var snapshot = root.Snapshot())
                {
                    Assert.Throws<InvalidOperationException>(() => snapshot.RevertTo(snapshot));
                }
            }
        }

    }
}
