using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;

namespace BTDB.StreamLayer;

public class PositionLessStreamReader : IMemReader
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
        _buf = GC.AllocateUninitializedArray<byte>(bufferSize, pinned: true);
        _usedOfs = 0;
        _usedLen = 0;
    }

    public unsafe void Init(ref MemReader reader)
    {
        if (_usedLen == 0)
        {
            var read = _stream.Read(_buf, _ofs);
            reader.Start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buf));
            reader.Current = reader.Start;
            reader.End = reader.Start + read;
            _usedOfs = 0;
            _usedLen = (uint)read;
            _ofs += (uint)read;
            return;
        }

        reader.Start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buf));
        reader.Current = reader.Start + (int)_usedOfs;
        reader.End = reader.Current + (int)_usedLen;
    }

    public unsafe void FillBuf(ref MemReader memReader, nuint advisePrefetchLength)
    {
        var curLen = (uint)(memReader.End - memReader.Current);
        _usedOfs += _usedLen - curLen;
        _usedLen = curLen;
        if (curLen != 0 || advisePrefetchLength == 0) return;
        var read = _stream.Read(_buf, _ofs);
        memReader.Start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buf));
        memReader.Current = memReader.Start;
        memReader.End = memReader.Start + read;
        _usedOfs = 0;
        _usedLen = (uint)read;
        _ofs += (uint)read;
        if (read == 0) PackUnpack.ThrowEndOfStreamException();
    }

    public long GetCurrentPosition(in MemReader spanReader)
    {
        return (long)_ofs - (spanReader.End - spanReader.Current);
    }

    public unsafe void ReadBlock(ref MemReader memReader, ref byte buffer, nuint length)
    {
        if (length < (nuint)_buf.Length)
        {
            FillBuf(ref memReader, length);
            if (memReader.End - memReader.Current < (nint)length) PackUnpack.ThrowEndOfStreamException();
            Unsafe.CopyBlockUnaligned(ref buffer, ref Unsafe.AsRef<byte>((void*)memReader.Current), (uint)length);
            memReader.Current += (int)length;
            return;
        }

        var read = _stream.Read(MemoryMarshal.CreateSpan(ref buffer, (int)length), _ofs);
        _ofs += (uint)read;
        if (read < (int)length) PackUnpack.ThrowEndOfStreamException();
    }

    public void SkipBlock(ref MemReader memReader, nuint length)
    {
        _ofs += length;
        if (_ofs <= _valueSize)
        {
            memReader.Start = 0;
            memReader.Current = 0;
            memReader.End = 0;
            _usedOfs = 0;
            _usedLen = 0;
            return;
        }

        _ofs = _valueSize;
        PackUnpack.ThrowEndOfStreamException();
    }

    public void SetCurrentPosition(ref MemReader memReader, long position)
    {
        memReader.Start = 0;
        memReader.Current = 0;
        memReader.End = 0;
        _usedOfs = 0;
        _usedLen = 0;
        _ofs = (ulong)position;
    }

    public bool Eof(ref MemReader memReader)
    {
        return GetCurrentPosition(memReader) == (long)_valueSize;
    }
}
