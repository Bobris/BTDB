using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.StreamLayer;

public class PositionLessStreamWriter : IMemWriter, IDisposable
{
    readonly IPositionLessStream _stream;
    readonly Action _onDispose;
    ulong _ofs;
    int _pos;
    const int BufLength = 8 * 1024;
    readonly byte[] _buf;

    public PositionLessStreamWriter(IPositionLessStream stream, bool atEnd = false)
        : this(stream, null, atEnd)
    {
    }

    public PositionLessStreamWriter(IPositionLessStream stream, Action? onDispose, bool atEnd = false)
    {
        _stream = stream;
        _onDispose = onDispose ?? DisposeStream;
        _buf = GC.AllocateUninitializedArray<byte>(BufLength, pinned: true);
        _pos = 0;
        if (atEnd)
        {
            _ofs = _stream.GetSize();
        }
        else
        {
            _ofs = 0;
            _stream.SetSize(0);
        }
    }

    void DisposeStream()
    {
        _stream.Dispose();
    }

    void FlushBuffer()
    {
        _stream.Write(_buf.AsSpan(0, _pos), _ofs);
        _ofs += (ulong)_pos;
        _pos = 0;
    }

    public void Dispose()
    {
        if (_pos != 0) FlushBuffer();
        _onDispose();
    }

    public unsafe void Init(ref MemWriter memWriter)
    {
        memWriter.Start = (nint)Unsafe.AsPointer(ref _buf[0]) + _pos;
        memWriter.Current = memWriter.Start;
        memWriter.End = memWriter.Start + BufLength - _pos;
    }

    public unsafe void Flush(ref MemWriter memWriter, uint spaceNeeded)
    {
        _pos = BufLength - (int)(memWriter.End - memWriter.Current);
        if (spaceNeeded != 0 && _pos + 1024 < BufLength) return;
        FlushBuffer();
        memWriter.Start = (nint)Unsafe.AsPointer(ref _buf[0]);
        memWriter.Current = memWriter.Start;
        memWriter.End = memWriter.Start + BufLength;
    }

    public long GetCurrentPosition(in MemWriter memWriter)
    {
        return (long)_ofs + BufLength - (int)(memWriter.End - memWriter.Current);
    }

    public void WriteBlock(ref MemWriter memWriter, ref byte buffer, nuint length)
    {
        Flush(ref memWriter, 0);
        _stream.Write(MemoryMarshal.CreateReadOnlySpan(ref buffer, (int)length), _ofs);
        _ofs += length;
    }

    public void SetCurrentPosition(ref MemWriter memWriter, long position)
    {
        Flush(ref memWriter, 0);
        if (_pos != 0) FlushBuffer();
        _ofs = (ulong)position;
        Init(ref memWriter);
    }

    public long GetCurrentPositionWithoutWriter()
    {
        return (long)_ofs + _pos;
    }
}
