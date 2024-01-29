using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using BTDB.Buffer;

namespace BTDB.StreamLayer;

public class ReadOnlySequenceMemReader : IMemReader, IDisposable
{
    ReadOnlySequence<byte> _sequence;
    long _pos;
    MemoryHandle _pin;

    public ReadOnlySequenceMemReader(ReadOnlySequence<byte> sequence)
    {
        _sequence = sequence;
        _pos = 0;
    }

    ~ReadOnlySequenceMemReader()
    {
        _pin.Dispose();
    }

    public unsafe void Init(ref MemReader reader)
    {
        var pos = _sequence.Start;
        if (_sequence.TryGet(ref pos, out var memory, false))
        {
            _pin = memory.Pin();
            reader.Start = (nint)_pin.Pointer;
            reader.Current = reader.Start;
            reader.End = reader.Start + memory.Length;
            return;
        }

        reader.Start = 0;
        reader.Current = 0;
        reader.End = 0;
    }

    public unsafe void FillBuf(ref MemReader memReader, nuint advisePrefetchLength)
    {
        var alreadyRead = memReader.Current - memReader.Start;
        _pos += alreadyRead;
        _sequence = _sequence.Slice(alreadyRead);
        if (_sequence.IsEmpty)
        {
            if (advisePrefetchLength > 0)
                PackUnpack.ThrowEndOfStreamException();
            memReader.Start = 0;
            memReader.Current = 0;
            memReader.End = 0;
            return;
        }
        var memory = _sequence.First;
        _pin.Dispose();
        _pin = memory.Pin();
        memReader.Start = (nint)_pin.Pointer;
        memReader.Current = memReader.Start;
        memReader.End = memReader.Start + memory.Length;
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
