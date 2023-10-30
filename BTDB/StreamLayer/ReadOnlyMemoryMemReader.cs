using System;
using System.Buffers;
using BTDB.Buffer;

namespace BTDB.StreamLayer;

public class ReadOnlyMemoryMemReader : IMemReader, IDisposable
{
    ReadOnlyMemory<byte> _memory;
    MemoryHandle _pin;

    public ReadOnlyMemoryMemReader(ReadOnlyMemory<byte> memory)
    {
        _memory = memory;
    }

    ~ReadOnlyMemoryMemReader()
    {
        _pin.Dispose();
    }

    public unsafe void Init(ref MemReader memReader)
    {
        _pin = _memory.Pin();
        memReader.Start = (nint)_pin.Pointer;
        memReader.Current = memReader.Start;
        memReader.End = memReader.Start + _memory.Length;
    }

    public void FillBuf(ref MemReader memReader, nuint advisePrefetchLength)
    {
        PackUnpack.ThrowEndOfStreamException();
    }

    public long GetCurrentPosition(in MemReader memReader)
    {
        return memReader.Current - memReader.Start;
    }

    public void ReadBlock(ref MemReader memReader, ref byte buffer, nuint length)
    {
        PackUnpack.ThrowEndOfStreamException();
    }

    public void SkipBlock(ref MemReader memReader, nuint length)
    {
        PackUnpack.ThrowEndOfStreamException();
    }

    public void SetCurrentPosition(ref MemReader memReader, long position)
    {
        memReader.Current = memReader.Start + (nint)position;
    }

    public bool Eof(ref MemReader memReader)
    {
        return memReader.Current == memReader.End;
    }

    public bool ThrowIfNotSimpleReader()
    {
        return true;
    }

    public bool TryReadBlockAsMemory(ref MemReader memReader, uint length, out ReadOnlyMemory<byte> result)
    {
        if (memReader.Current + length > memReader.End)
        {
            PackUnpack.ThrowEndOfStreamException();
        }

        result = _memory.Slice((int)(memReader.Current - memReader.Start), (int)length);
        return true;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        _pin.Dispose();
        _memory = default;
    }
}
