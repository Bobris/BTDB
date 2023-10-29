using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;

namespace BTDB.StreamLayer;

public class ReadOnlySequenceMemReader : IMemReader, IDisposable
{
    ReadOnlySequence<byte> _sequence;
    long _pos;
    MemoryHandle _pin;
    byte[]? _buffer;
    const int BufferSize = 4096;

    public ReadOnlySequenceMemReader(ReadOnlySequence<byte> sequence)
    {
        _sequence = sequence;
        _pos = 0;
    }

    ~ReadOnlySequenceMemReader()
    {
        _pin.Dispose();
    }

    public unsafe void Init(ref MemReader memReader)
    {
        var pos = _sequence.Start;
        if (_sequence.TryGet(ref pos, out var memory, false))
        {
            _pin = memory.Pin();
            memReader.Start = (nint)_pin.Pointer;
            memReader.Current = memReader.Start;
            memReader.End = memReader.Start + memory.Length;
            return;
        }

        _buffer = GC.AllocateUninitializedArray<byte>(BufferSize, pinned: true);
        memReader.Start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buffer));
        memReader.Current = memReader.Start;
        var len = Math.Min(_sequence.FirstSpan.Length, BufferSize);
        memReader.End = memReader.Start + len;
        _sequence.FirstSpan[..len].CopyTo(_buffer);
    }

    public unsafe void FillBuf(ref MemReader memReader, nuint advisePrefetchLength)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(advisePrefetchLength, (nuint)BufferSize);
        var alreadyRead = memReader.Current - memReader.Start;
        _pos += alreadyRead;
        _sequence = _sequence.Slice(alreadyRead);
        if (_sequence.IsEmpty) PackUnpack.ThrowEndOfStreamException();
        var memory = _sequence.First;
        _pin.Dispose();
        if ((nuint)memory.Length >= advisePrefetchLength)
        {
            _pin = memory.Pin();
            memReader.Start = (nint)_pin.Pointer;
            memReader.Current = memReader.Start;
            memReader.End = memReader.Start + memory.Length;
            return;
        }

        if (_buffer == null) _buffer = GC.AllocateUninitializedArray<byte>(BufferSize, pinned: true);
        memReader.Start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buffer));
        memReader.Current = memReader.Start;
        var len = Math.Min(_sequence.FirstSpan.Length, BufferSize);
        memReader.End = memReader.Start + len;
        _sequence.FirstSpan[..len].CopyTo(_buffer);
        var read = len;
        var seq = _sequence;
        while ((nuint)read < advisePrefetchLength)
        {
            seq = seq.Slice(len);
            if (seq.IsEmpty) PackUnpack.ThrowEndOfStreamException();
            len = Math.Min(seq.FirstSpan.Length, BufferSize - read);
            memReader.End += len;
            _sequence.FirstSpan[..len].CopyTo(_buffer[read..]);
            read += len;
        }
    }

    public long GetCurrentPosition(in MemReader memReader)
    {
        return _pos + memReader.Current - memReader.Start;
    }

    public unsafe void ReadBlock(ref MemReader memReader, ref byte buffer, nuint length)
    {
        while (length > 0)
        {
            FillBuf(ref memReader, 1);
            var canSkip = (int)Math.Min((nuint)(memReader.End - memReader.Current), length);
            Unsafe.CopyBlockUnaligned(ref buffer,
                ref Unsafe.AsRef<byte>((void*)memReader.Current), (uint)canSkip);
            memReader.Current += canSkip;
            length -= (uint)canSkip;
        }
    }

    public void SkipBlock(ref MemReader memReader, nuint length)
    {
        var alreadyRead = memReader.Current - memReader.Start;
        _pos += alreadyRead;
        _sequence = _sequence.Slice(alreadyRead);
        _pin.Dispose();
        memReader.Start = 0;
        memReader.Current = 0;
        memReader.End = 0;
        while (length > 0)
        {
            if (_sequence.IsEmpty) PackUnpack.ThrowEndOfStreamException();
            var canSkip = (int)Math.Min((nuint)_sequence.FirstSpan.Length, length);
            _pos += canSkip;
            _sequence = _sequence.Slice(canSkip);
            length -= (uint)canSkip;
        }
    }

    public void SetCurrentPosition(ref MemReader memReader, long position)
    {
        throw new NotSupportedException();
    }

    public bool Eof(ref MemReader memReader)
    {
        var alreadyRead = memReader.Current - memReader.Start;
        memReader.Start = memReader.Current;
        _pos += alreadyRead;
        _sequence = _sequence.Slice(alreadyRead);
        return _sequence.IsEmpty;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        _pin.Dispose();
        _sequence = default;
    }
}
