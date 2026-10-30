using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;

namespace BTDB.StreamLayer;

/// Passes all bytes written to the given writer and computes their XxHash64 on the way. Positions count from 0.
sealed class HashingMemWriter : IMemWriter
{
    MemWriter _memWriter;
    XxHash64 _hash = new();
    readonly byte[] _buffer = GC.AllocateUninitializedArray<byte>(64 * 1024, pinned: true);
    long _totalPos;

    public HashingMemWriter(in MemWriter memWriter)
    {
        _memWriter = memWriter;
    }

    void Pass(ReadOnlySpan<byte> data)
    {
        _hash.Update(data);
        _memWriter.WriteBlock(data);
        _totalPos += data.Length;
    }

    void PassBuffer(in MemWriter memWriter) => Pass(_buffer.AsSpan(0, (int)(memWriter.Current - memWriter.Start)));

    /// Passes what is left and returns the given writer, which goes on after all bytes passed, and their hash.
    public MemWriter Finish(in MemWriter memWriter, out ulong hash)
    {
        PassBuffer(memWriter);
        hash = _hash.Digest();
        return _memWriter;
    }

    public unsafe void Init(ref MemWriter memWriter)
    {
        memWriter.Start = (nint)Unsafe.AsPointer(ref _buffer[0]);
        memWriter.Current = memWriter.Start;
        memWriter.End = memWriter.Start + _buffer.Length;
    }

    public void Flush(ref MemWriter memWriter, uint spaceNeeded)
    {
        PassBuffer(memWriter);
        Init(ref memWriter);
    }

    public long GetCurrentPosition(in MemWriter memWriter) => _totalPos + memWriter.Current - memWriter.Start;

    public void WriteBlock(ref MemWriter memWriter, ref byte buffer, nuint length)
    {
        PassBuffer(memWriter);
        Pass(MemoryMarshal.CreateReadOnlySpan(ref buffer, (int)length));
        Init(ref memWriter);
    }

    public void SetCurrentPosition(ref MemWriter memWriter, long position) => throw new NotSupportedException();
}
