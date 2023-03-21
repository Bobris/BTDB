using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using BTDB.Buffer;

namespace BTDB.StreamLayer;

public class ReadOnlySequenceSpanReader : ISpanReader
{
    ReadOnlySequence<byte> _sequence;
    long _pos;

    public ReadOnlySequenceSpanReader(ReadOnlySequence<byte> sequence)
    {
        _sequence = sequence;
        _pos = 0;
    }

    public void Init(ref SpanReader spanReader)
    {
        spanReader.Buf = _sequence.FirstSpan;
    }

    public bool FillBufAndCheckForEof(ref SpanReader spanReader)
    {
        if (spanReader.Buf.Length != 0) return false;
        var read = _sequence.FirstSpan.Length;
        _pos += read;
        _sequence = _sequence.Slice(read);
        if (_sequence.IsEmpty) return true;
        spanReader.Buf = _sequence.FirstSpan;
        return false;
    }

    public long GetCurrentPosition(in SpanReader spanReader)
    {
        return _pos + _sequence.FirstSpan.Length - spanReader.Buf.Length;
    }

    public bool ReadBlock(ref SpanReader spanReader, ref byte buffer, uint length)
    {
        while (length > 0)
        {
            if (FillBufAndCheckForEof(ref spanReader)) return true;
            var canSkip = (int)Math.Min(spanReader.Buf.Length, length);
            Unsafe.CopyBlockUnaligned(ref buffer,
                ref PackUnpack.UnsafeGetAndAdvance(ref spanReader.Buf, canSkip), (uint)canSkip);
            buffer = Unsafe.Add(ref buffer, canSkip);
            length -= (uint)canSkip;
        }

        return false;
    }

    public bool SkipBlock(ref SpanReader spanReader, uint length)
    {
        while (length > 0)
        {
            if (FillBufAndCheckForEof(ref spanReader)) return true;
            var canSkip = (int)Math.Min(spanReader.Buf.Length, length);
            PackUnpack.UnsafeAdvance(ref spanReader.Buf, canSkip);
            length -= (uint)canSkip;
        }

        return false;
    }

    public void SetCurrentPosition(ref SpanReader spanReader, long position)
    {
        throw new NotSupportedException();
    }

    public void Sync(ref SpanReader spanReader)
    {
        var read = _sequence.FirstSpan.Length - spanReader.Buf.Length;
        _pos += read;
        _sequence = _sequence.Slice(read);
    }

    bool TryReadBlockAsMemory(ref SpanReader spanReader, uint length, out ReadOnlyMemory<byte> result)
    {
        Sync(ref spanReader);
        var pos = _sequence.Start;
        if (!_sequence.TryGet(ref pos, out result, false)) return false;
        if (result.Length < length) return false;
        result = result[..(int)length];
        _pos += length;
        _sequence = _sequence.Slice(length);
        Init(ref spanReader);
        return true;
    }
}
