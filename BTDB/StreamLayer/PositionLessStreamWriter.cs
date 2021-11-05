using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.StreamLayer;

public class PositionLessStreamWriter : ISpanWriter, IDisposable
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
        _buf = new byte[BufLength];
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

    public void Init(ref SpanWriter spanWriter)
    {
        spanWriter.Buf = _buf.AsSpan(_pos);
        spanWriter.HeapBuffer = _buf;
    }

    public void Sync(ref SpanWriter spanWriter)
    {
        _pos = BufLength - spanWriter.Buf.Length;
    }

    public bool Flush(ref SpanWriter spanWriter)
    {
        _pos = BufLength - spanWriter.Buf.Length;
        FlushBuffer();
        spanWriter.Buf = _buf;
        return true;
    }

    public long GetCurrentPosition(in SpanWriter spanWriter)
    {
        return (long)_ofs + BufLength - spanWriter.Buf.Length;
    }

    public long GetCurrentPositionWithoutWriter()
    {
        return (long)_ofs + _pos;
    }

    public void WriteBlock(ref SpanWriter spanWriter, ref byte buffer, uint length)
    {
        _stream.Write(MemoryMarshal.CreateReadOnlySpan(ref buffer, (int)length), _ofs);
        _ofs += length;
    }

    public void WriteBlockWithoutWriter(ref byte buffer, uint length)
    {
        if (length <= (uint)(BufLength - _pos))
        {
            Unsafe.CopyBlockUnaligned(ref MemoryMarshal.GetReference(_buf.AsSpan(_pos, (int)length)), ref buffer,
                length);
            _pos += (int)length;
        }
        else
        {
            var writer = new SpanWriter(this);
            writer.WriteBlock(ref buffer, length);
            writer.Sync();
        }
    }

    public void SetCurrentPosition(ref SpanWriter spanWriter, long position)
    {
        if (_pos != 0) FlushBuffer();
        _ofs = (ulong)position;
        spanWriter.Buf = _buf;
    }
}
