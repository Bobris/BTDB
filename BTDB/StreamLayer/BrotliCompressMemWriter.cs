using System;
using System.Buffers;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;

namespace BTDB.StreamLayer;

public class BrotliCompressMemWriter : IMemWriter
{
    MemWriter _memWriter;
    BrotliEncoder _encoder;
    readonly byte[] _buffer;
    int _ofs;
    long _totalPos;

    public BrotliCompressMemWriter(in MemWriter memWriter, int quality = 5, int windowBits = 22)
    {
        _memWriter = memWriter;
        if (quality is < 0 or > 11)
        {
            quality = 11;
        }

        if (windowBits < 10) windowBits = 10;
        if (windowBits > 24) windowBits = 24;

        _encoder = new(quality, windowBits);
        _buffer = GC.AllocateUninitializedArray<byte>(64 * 1024, pinned: true);
        _ofs = 0;
        _totalPos = 0;
    }

    ~BrotliCompressMemWriter()
    {
        _encoder.Dispose();
        if (_ofs != -1) throw new("BrotliCompressMemWriter not finished");
    }

    unsafe void WriteCore(ReadOnlySpan<byte> buffer, bool isFinalBlock)
    {
        var lastResult = OperationStatus.DestinationTooSmall;
        while (lastResult == OperationStatus.DestinationTooSmall)
        {
            _memWriter.Resize(1);
            lastResult = _encoder.Compress(buffer,
                new((void*)_memWriter.Current, (int)Math.Min(_memWriter.End - _memWriter.Current, int.MaxValue)),
                out var bytesConsumed, out var bytesWritten, isFinalBlock);
            if (lastResult == OperationStatus.InvalidData)
                throw new InvalidOperationException();
            if (bytesWritten > 0)
            {
                _memWriter.Current += bytesWritten;
            }

            if (bytesConsumed > 0)
                PackUnpack.UnsafeAdvance(ref buffer, bytesConsumed);
        }
    }

    public MemWriter Finish(in MemWriter memWriter)
    {
        _ofs = (int)(memWriter.Current - memWriter.Start);
        WriteCore(_buffer.AsSpan(0, _ofs), true);
        _totalPos += _ofs;
        _ofs = -1;
        _encoder.Dispose();
        GC.SuppressFinalize(this);
        return _memWriter;
    }

    public unsafe void Init(ref MemWriter memWriter)
    {
        memWriter.Start = (nint)Unsafe.AsPointer(ref _buffer[0]);
        memWriter.Current = memWriter.Start + _ofs;
        memWriter.End = memWriter.Start + _buffer.Length;
    }

    public void Flush(ref MemWriter memWriter, uint spaceNeeded)
    {
        _ofs = (int)(memWriter.Current - memWriter.Start);
        WriteCore(_buffer.AsSpan(0, _ofs), false);
        _ofs = 0;
        Init(ref memWriter);
    }

    public long GetCurrentPosition(in MemWriter memWriter)
    {
        return _totalPos + memWriter.Current - memWriter.Start;
    }

    public void WriteBlock(ref MemWriter memWriter, ref byte buffer, nuint length)
    {
        _ofs = (int)(memWriter.Current - memWriter.Start);
        WriteCore(_buffer.AsSpan(0, _ofs), false);
        _totalPos += _ofs;
        _ofs = 0;
        WriteCore(MemoryMarshal.CreateSpan(ref buffer, (int)length), false);
        _totalPos += (long)length;
        Init(ref memWriter);
    }

    public void SetCurrentPosition(ref MemWriter memWriter, long position)
    {
        throw new NotSupportedException();
    }
}
