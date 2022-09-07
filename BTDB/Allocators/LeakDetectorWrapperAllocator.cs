using System;
using System.Collections.Concurrent;

namespace BTDB.Allocators;

class LeakDetectorWrapperAllocator : IOffHeapAllocator, IDisposable
{
    IOffHeapAllocator _wrapped;
    ConcurrentDictionary<IntPtr, IntPtr> _ptr2SizeMap = new();

    public LeakDetectorWrapperAllocator(IOffHeapAllocator wrap)
    {
        _wrapped = wrap;
    }

    public (uint Count, ulong Size) QueryAllocations()
    {
        (uint Count, ulong Size) res = (0, 0);
        foreach (var i in _ptr2SizeMap)
        {
            res = (res.Count + 1, res.Size + (ulong)i.Value.ToInt64());
        }
        return res;
    }

    public IntPtr Allocate(IntPtr size)
    {
        var res = _wrapped.Allocate(size + 32);
        unsafe
        {
            new Span<byte>(res.ToPointer(), 16).Fill(0xBB);
            new Span<byte>((res + 16).ToPointer(), size.ToInt32()).Fill(255);
            new Span<byte>((res + size.ToInt32() + 16).ToPointer(), 16).Fill(0xEE);
        }
        res += 16;
        _ptr2SizeMap.TryAdd(res, size);
        return res;
    }

    public void Deallocate(IntPtr ptr, IntPtr size)
    {
        if (!_ptr2SizeMap.TryRemove(ptr, out var osize))
            throw new InvalidOperationException($"Trying to free memory which is not allocated {ptr.ToInt64()}");
        if (size != osize)
            throw new InvalidOperationException(
                $"Deallocate size is different from allocated {size.ToInt64()}!={osize.ToInt64()}");
        ptr -= 16;
        unsafe
        {
            var span = new Span<byte>(ptr.ToPointer(), 16);
            for (var i = 0; i < 16; i++)
            {
                if (span[i] != 0xBB)
                    throw new InvalidOperationException("Overwrite of block at begging " + i);
            }
            span = new((ptr + osize.ToInt32() + 16).ToPointer(), 16);
            for (var i = 0; i < 16; i++)
            {
                if (span[i] != 0xEE)
                    throw new InvalidOperationException("Overwrite of block at end " + i);
            }
            new Span<byte>(ptr.ToPointer(), (int)osize + 32).Fill(0xDD);
        }
        _wrapped.Deallocate(ptr, size + 32);
    }

    public (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetStats()
    {
        return _wrapped.GetStats();
    }

    public void Dispose()
    {
        foreach (var i in _ptr2SizeMap)
        {
            Deallocate(i.Key, i.Value);
        }
    }
}
