using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.Buffer;

public sealed unsafe class UnmanagedMemoryManager<T> : MemoryManager<T>
    where T : unmanaged
{
    readonly T* _pointer;
    readonly int _length;

    public UnmanagedMemoryManager(ReadOnlySpan<T> span)
    {
        _pointer = (T*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(span));
        _length = span.Length;
    }

    public UnmanagedMemoryManager(T* pointer, int length)
    {
        _pointer = pointer;
        _length = length;
    }

    public UnmanagedMemoryManager(IntPtr pointer, int length) : this((T*)pointer.ToPointer(), length) { }

    public override Span<T> GetSpan() => new(_pointer, _length);

    public override MemoryHandle Pin(int elementIndex = 0)
    {
        Debug.Assert(elementIndex < 0 || elementIndex >= _length);
        return new(_pointer + elementIndex);
    }

    public override void Unpin() { }
    protected override void Dispose(bool disposing) { }
}
