using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;

namespace BTDB.StreamLayer;

public class PositionLessStreamReader : ISpanReader
{
    readonly IPositionLessStream _stream;
    readonly ulong _valueSize;
    ulong _ofs;
    readonly byte[] _buf;
    uint _usedOfs;
    uint _usedLen;

    public PositionLessStreamReader(IPositionLessStream stream, int bufferSize = 8192)
    {
        if (bufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(bufferSize));

        _stream = stream;
        _valueSize = _stream.GetSize();
        _ofs = 0;
        _buf = new byte[bufferSize];
        _usedOfs = 0;
        _usedLen = 0;
    }

    public void Init(ref SpanReader spanReader)
    {
        if (_usedLen == 0)
        {
            var read = _stream.Read(_buf, _ofs);
            spanReader.Buf = _buf.AsSpan(0, read);
            _usedOfs = 0;
            _usedLen = (uint)read;
            _ofs += (uint)read;
            return;
        }
        spanReader.Buf = _buf.AsSpan((int)_usedOfs, (int)_usedLen);
    }

    public bool FillBufAndCheckForEof(ref SpanReader spanReader)
    {
        if (spanReader.Buf.Length != 0) return false;
        var read = _stream.Read(_buf, _ofs);
        spanReader.Buf = _buf.AsSpan(0, read);
        _usedOfs = 0;
        _usedLen = (uint)read;
        _ofs += (uint)read;
        return spanReader.Buf.Length == 0;
    }

    public long GetCurrentPosition(in SpanReader spanReader)
    {
        return (long)_ofs - spanReader.Buf.Length;
    }

    public bool ReadBlock(ref SpanReader spanReader, ref byte buffer, uint length)
    {
        if (length < _buf.Length)
        {
            if (FillBufAndCheckForEof(ref spanReader) || (uint)spanReader.Buf.Length < length) return true;
            Unsafe.CopyBlockUnaligned(ref buffer,
                ref PackUnpack.UnsafeGetAndAdvance(ref spanReader.Buf, (int)length), length);
            return false;
        }

        var read = _stream.Read(MemoryMarshal.CreateSpan(ref buffer, (int)length), _ofs);
        _ofs += (uint)read;
        return read < length;
    }

    public bool SkipBlock(ref SpanReader spanReader, uint length)
    {
        _ofs += length;
        if (_ofs <= _valueSize) return false;
        _ofs = _valueSize;
        return true;
    }

    public void SetCurrentPosition(ref SpanReader spanReader, long position)
    {
        spanReader.Buf = new ReadOnlySpan<byte>();
        _usedOfs = 0;
        _usedLen = 0;
        _ofs = (ulong)position;
    }

    public void Sync(ref SpanReader spanReader)
    {
        var curLen = (uint)spanReader.Buf.Length;
        _usedOfs += _usedLen - curLen;
        _usedLen = curLen;
    }
}
