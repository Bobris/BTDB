using System;
using BTDB.Allocators;
using BTDB.BTreeLib;
using Xunit;

namespace BTDBTest.BTreeLibTests;

public class RootNode12Tests : IDisposable
{
    LeakDetectorWrapperAllocator _allocator;
    BTreeImpl12 _impl;

    public RootNode12Tests()
    {
        _allocator = new(new MallocAllocator());
        _impl = new(_allocator);
    }

    public void Dispose()
    {
        var leaks = _allocator.QueryAllocations();
        Assert.Equal(0ul, leaks.Count);
    }

    [Fact]
    public void CouldBeCreated()
    {
        using (var root = new RootNode12(_impl))
        {
        }
    }

    [Fact]
    public void CanCreateSnapshot()
    {
        using (var root = new RootNode12(_impl))
        {
            using (var snapshot = root.Snapshot())
            {
            }
        }
    }

    [Fact]
    public void CanRevertToSnapshot()
    {
        using (var root = new RootNode12(_impl))
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
        using (var root = new RootNode12(_impl))
        {
            using (var snapshot = root.Snapshot())
            {
                Assert.Throws<InvalidOperationException>(() => snapshot.RevertTo(snapshot));
            }
        }
    }
}
