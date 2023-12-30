using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using BTDB.Buffer;

namespace BTDB.StreamLayer;

public class BrotliDecompressMemReader : IMemReader
{
    BrotliDecoder _decoder;
    MemReader _source;
    ulong _ofs;
    readonly byte[] _buf;
    uint _usedOfs;
    uint _usedLen;

    public BrotliDecompressMemReader(in MemReader memReader)
    {
        _source = memReader;
        _decoder = new();
        _ofs = 0;
        _buf = GC.AllocateUninitializedArray<byte>(64 * 1024, pinned: true);
        _usedOfs = 0;
        _usedLen = 0;
    }

    ~BrotliDecompressMemReader()
    {
        _decoder.Dispose();
    }

    public MemReader Finish(in MemReader decompressor)
    {
        var memReader = decompressor;
        while (!memReader.Eof)
        {
            memReader.Current = memReader.End;
        }

        GC.SuppressFinalize(this);
        _decoder.Dispose();
        return _source;
    }

    unsafe int ReadCore(Span<byte> buf)
    {
        var read = 0;
        while (true)
        {
            if (_source.Eof)
            {
                throw new InvalidDataException("Brotli premature eof");
            }

            var status =
                _decoder.Decompress(
                    new((void*)_source.Current, (int)Math.Min(_source.End - _source.Current, int.MaxValue)), buf,
                    out var bytesConsumed, out var bytesWritten);
            if (status == OperationStatus.InvalidData)
            {
                throw new InvalidDataException("Brotli data corrupted");
            }

            PackUnpack.UnsafeAdvance(ref buf, bytesWritten);
            read += bytesWritten;
            _source.Current += bytesConsumed;
            if (status == OperationStatus.NeedMoreData) continue;
            return read;
        }
    }

    public unsafe void Init(ref MemReader reader)
    {
        if (_usedLen == 0)
        {
            var read = ReadCore(_buf);
            reader.Start = (nint)Unsafe.AsPointer(ref _buf[0]);
            reader.Current = reader.Start;
            reader.End = reader.Start + read;
            _usedOfs = 0;
            _usedLen = (uint)read;
            _ofs += (uint)read;
            return;
        }

        reader.Start = (nint)Unsafe.AsPointer(ref _buf[0]);
        reader.Current = reader.Start + (int)_usedOfs;
        reader.End = reader.Current + (int)_usedLen;
    }

    public unsafe void FillBuf(ref MemReader memReader, nuint advisePrefetchLength)
    {
        if (memReader.End - memReader.Current >= 1024) return;
        _usedOfs = (uint)(memReader.Current - memReader.Start);
        _usedLen = (uint)(memReader.End - memReader.Current);
        if (_usedOfs != 0)
        {
            _buf.AsSpan((int)_usedOfs, (int)_usedLen).CopyTo(_buf);
            _usedOfs = 0;
        }

        var read = ReadCore(_buf.AsSpan((int)_usedLen));
        _usedLen += (uint)read;
        memReader.Start = (nint)Unsafe.AsPointer(ref _buf[0]);
        memReader.Current = memReader.Start;
        memReader.End = memReader.Start + (int)_usedLen;
        _ofs += (uint)read;
    }

    public long GetCurrentPosition(in MemReader memReader)
    {
        _usedOfs = (uint)(memReader.Current - memReader.Start);
        _usedLen = (uint)(memReader.End - memReader.Current);
        return (long)_ofs - _usedLen + _usedOfs;
    }

    public void ReadBlock(ref MemReader memReader, ref byte buffer, nuint length)
    {
        throw new NotImplementedException();
    }

    public void SkipBlock(ref MemReader memReader, nuint length)
    {
        while (length > 0)
        {
            FillBuf(ref memReader, 0);
            var canSkip = (int)Math.Min(memReader.End - memReader.Current, (nint)length);
            if (canSkip == 0) PackUnpack.ThrowEndOfStreamException();
            memReader.Current += canSkip;
            length -= (uint)canSkip;
        }
    }

    public void SetCurrentPosition(ref MemReader memReader, long position)
    {
        throw new NotSupportedException();
    }

    public bool Eof(ref MemReader memReader)
    {
        if (memReader.Current < memReader.End) return false;
        FillBuf(ref memReader, 0);
        return memReader.Current >= memReader.End;
    }
}
