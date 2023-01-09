using System;
using System.Runtime.CompilerServices;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.KVDBLayer;

namespace BTDB.StreamLayer;

public sealed class ContinuousMemoryBlockWriter : ISpanWriter
{
    byte[] _bytes;
    int _ofs;

    public ContinuousMemoryBlockWriter()
    {
        _bytes = Array.Empty<byte>();
        _ofs = 0;
    }

    public ContinuousMemoryBlockWriter(in StructList<byte> buffer)
    {
        _bytes = buffer.UnsafeBackingArray ?? Array.Empty<byte>();
        _ofs = (int)buffer.Count;
    }

    public StructList<byte> GetStructList()
    {
        return new(_bytes, (uint)_ofs);
    }

    public Span<byte> GetSpan()
    {
        return _bytes.AsSpan(0, _ofs);
    }

    public ByteBuffer GetByteBuffer()
    {
        return ByteBuffer.NewSync(_bytes, 0, _ofs);
    }

    public void Reset()
    {
        _ofs = 0;
    }

    public void ResetAndFreeMemory()
    {
        _bytes = Array.Empty<byte>();
        _ofs = 0;
    }

    void ISpanWriter.Init(ref SpanWriter spanWriter)
    {
        spanWriter.Buf = _bytes.AsSpan(_ofs);
    }

    void ISpanWriter.Sync(ref SpanWriter spanWriter)
    {
        _ofs = _bytes.Length - spanWriter.Buf.Length;
    }

    bool ISpanWriter.Flush(ref SpanWriter spanWriter)
    {
        ((ISpanWriter)this).Sync(ref spanWriter);
        var newLength = Math.Min(Math.Max(_bytes.Length * 2, 32), int.MaxValue);
        if (_bytes.Length == newLength)
            throw new BTDBException("ContinuousMemoryBlockWriter reached maximum size of " + int.MaxValue);
        Array.Resize(ref _bytes, newLength);
        ((ISpanWriter)this).Init(ref spanWriter);
        return true;
    }

    long ISpanWriter.GetCurrentPosition(in SpanWriter spanWriter)
    {
        return _bytes.Length - spanWriter.Buf.Length;
    }

    public long GetCurrentPositionWithoutWriter()
    {
        return _ofs;
    }

    void ISpanWriter.WriteBlock(ref SpanWriter spanWriter, ref byte buffer, uint length)
    {
        ((ISpanWriter)this).Sync(ref spanWriter);
        var newLength = (int)Math.Min(Math.Max(_bytes.Length * 2, _ofs + length), int.MaxValue);
        if (_bytes.Length == newLength)
            throw new BTDBException("ContinuousMemoryBlockWriter reached maximum size of " + int.MaxValue);
        Array.Resize(ref _bytes, newLength);
        ((ISpanWriter)this).Init(ref spanWriter);
        Unsafe.CopyBlockUnaligned(ref PackUnpack.UnsafeGetAndAdvance(ref spanWriter.Buf, (int)length),
            ref buffer, length);
    }

    public void WriteBlockWithoutWriter(ref byte buffer, uint length)
    {
        var writer = new SpanWriter(this);
        writer.WriteBlock(ref buffer, length);
        writer.Sync();
    }

    void ISpanWriter.SetCurrentPosition(ref SpanWriter spanWriter, long position)
    {
        _ofs = (int)position;
        spanWriter.Buf = _bytes.AsSpan(_ofs);
    }
}
