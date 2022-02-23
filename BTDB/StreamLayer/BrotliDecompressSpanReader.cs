using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;

namespace BTDB.StreamLayer;

public class BrotliDecompressSpanReader : ISpanReader, IDisposable
{
    BrotliDecoder _decoder;
    ISpanReader _source;
    ulong _ofs;
    readonly byte[] _buf;
    uint _usedOfs;
    uint _usedLen;

    public BrotliDecompressSpanReader(ISpanReader source)
    {
        _source = source;
        _decoder = new();
        _ofs = 0;
        _buf = new byte[256 * 1024];
        _usedOfs = 0;
        _usedLen = 0;
    }

    ~BrotliDecompressSpanReader()
    {
        _decoder.Dispose();
    }

    public void Init(ref SpanReader spanReader)
    {
        if (_usedLen == 0)
        {
            var read = ReadCore(_buf);
            spanReader.Buf = _buf.AsSpan(0, read);
            _usedOfs = 0;
            _usedLen = (uint)read;
            _ofs += (uint)read;
            return;
        }

        spanReader.Buf = _buf.AsSpan((int)_usedOfs, (int)_usedLen);
    }

    int ReadCore(Span<byte> buf)
    {
        var reader = new SpanReader(_source);
        var read = 0;
        while (true)
        {
            if (reader.Eof)
            {
                throw new InvalidDataException("Brotli premature eof");
            }

            var status = _decoder.Decompress(reader.Buf, buf, out var bytesConsumed, out var bytesWritten);
            if (status == OperationStatus.InvalidData)
            {
                throw new InvalidDataException("Brotli data corrupted");
            }

            PackUnpack.UnsafeAdvance(ref buf, bytesWritten);
            read += bytesWritten;
            PackUnpack.UnsafeAdvance(ref reader.Buf, bytesConsumed);
            if (status == OperationStatus.NeedMoreData) continue;
            reader.Sync();
            return read;
        }
    }

    public bool FillBufAndCheckForEof(ref SpanReader spanReader)
    {
        if (spanReader.Buf.Length != 0) return false;
        var read = ReadCore(_buf);
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

        var read = ReadCore(MemoryMarshal.CreateSpan(ref buffer, (int)length));
        _ofs += (uint)read;
        return read < length;
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
        var curLen = (uint)spanReader.Buf.Length;
        _usedOfs += _usedLen - curLen;
        _usedLen = curLen;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        _decoder.Dispose();
    }
}
