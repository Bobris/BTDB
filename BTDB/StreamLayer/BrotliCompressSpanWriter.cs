using System;
using System.Buffers;
using System.IO.Compression;
using System.Runtime.InteropServices;
using BTDB.Buffer;

namespace BTDB.StreamLayer;

public class BrotliCompressSpanWriter : ISpanWriter, ICompressedSize, IDisposable
{
    readonly ISpanWriter _target;
    BrotliEncoder _encoder;
    byte[] _buffer;
    int _ofs;
    long _totalPos;
    long _compressedSize;

    public BrotliCompressSpanWriter(ISpanWriter target, int quality = 5, int windowBits = 22)
    {
        _target = target;
        if (quality is < 0 or > 11)
        {
            quality = 11;
        }

        if (windowBits < 10) windowBits = 10;
        if (windowBits > 24) windowBits = 24;

        _encoder = new(quality, windowBits);
        _buffer = new byte[256 * 1024];
        _ofs = 0;
        _totalPos = 0;
        _compressedSize = 0;
    }

    ~BrotliCompressSpanWriter()
    {
        _encoder.Dispose();
        if (_ofs != -1) throw new("BrotliCompressSpanWriter not disposed");
    }

    public long GetCompressedSize()
    {
        return _compressedSize;
    }

    public void Init(ref SpanWriter spanWriter)
    {
        spanWriter.Buf = _buffer.AsSpan(_ofs);
    }

    public void Sync(ref SpanWriter spanWriter)
    {
        _ofs = _buffer.Length - spanWriter.Buf.Length;
    }

    public bool Flush(ref SpanWriter spanWriter)
    {
        Sync(ref spanWriter);
        WriteCore(_buffer.AsSpan(0, _ofs), false);
        _totalPos += _ofs;
        _ofs = 0;
        Init(ref spanWriter);
        return true;
    }

    void WriteCore(ReadOnlySpan<byte> buffer, bool isFinalBlock)
    {
        var lastResult = OperationStatus.DestinationTooSmall;
        var targetWriter = new SpanWriter(_target);
        while (lastResult == OperationStatus.DestinationTooSmall)
        {
            if (targetWriter.Buf.Length < 1024) _target.Flush(ref targetWriter);
            lastResult = _encoder.Compress(buffer, targetWriter.Buf, out var bytesConsumed, out var bytesWritten,
                isFinalBlock);
            if (lastResult == OperationStatus.InvalidData)
                throw new InvalidOperationException();
            if (bytesWritten > 0)
            {
                PackUnpack.UnsafeAdvance(ref targetWriter.Buf, bytesWritten);
                _compressedSize += bytesWritten;
            }

            if (bytesConsumed > 0)
                PackUnpack.UnsafeAdvance(ref buffer, bytesConsumed);
        }

        targetWriter.Sync();
    }

    public long GetCurrentPosition(in SpanWriter spanWriter)
    {
        return _totalPos + _buffer.Length - spanWriter.Buf.Length;
    }

    public long GetCurrentPositionWithoutWriter()
    {
        return _totalPos + _ofs;
    }

    public void WriteBlock(ref SpanWriter spanWriter, ref byte buffer, uint length)
    {
        Sync(ref spanWriter);
        WriteBlockWithoutWriter(ref buffer, length);
        Init(ref spanWriter);
    }

    public void WriteBlockWithoutWriter(ref byte buffer, uint length)
    {
        WriteCore(_buffer.AsSpan(0, _ofs), false);
        _totalPos += _ofs;
        _ofs = 0;
        WriteCore(MemoryMarshal.CreateSpan(ref buffer, (int)length), false);
        _totalPos += length;
    }

    public void SetCurrentPosition(ref SpanWriter spanWriter, long position)
    {
        throw new NotSupportedException();
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        WriteCore(_buffer.AsSpan(0, _ofs), true);
        _ofs = -1;
        _encoder.Dispose();
    }
}
