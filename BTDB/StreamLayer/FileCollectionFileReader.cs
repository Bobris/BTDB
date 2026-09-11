using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace BTDB.StreamLayer;

/// <summary>
/// Buffered reader over RandomRead, with an initial absolute offset and an exclusive end offset captured at restart.
/// Does not own the file. The caller must retain it and keep the selected byte prefix immutable while reading.
/// </summary>
public sealed class FileCollectionFileReader : IMemReader
{
    IFileCollectionFile _file = null!;
    readonly byte[] _buffer;
    ulong _size;
    ulong _offset;

    public FileCollectionFileReader(IFileCollectionFile file, ulong startOffset = 0,
        ulong endOffset = ulong.MaxValue, int bufferSize = 8192)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(bufferSize);
        _buffer = GC.AllocateUninitializedArray<byte>(bufferSize, pinned: true);
        Restart(file, startOffset, endOffset);
    }

    /// <summary>
    /// Reuse the buffer for another file or range. Discard the previous MemReader before calling this method,
    /// then construct a new MemReader with this controller. The end is capped at the file's current size.
    /// </summary>
    public void Restart(IFileCollectionFile file, ulong startOffset = 0, ulong endOffset = ulong.MaxValue)
    {
        ArgumentNullException.ThrowIfNull(file);
        var size = Math.Min(file.GetSize(), endOffset);
        if (startOffset > size) throw new ArgumentOutOfRangeException(nameof(startOffset));
        _file = file;
        _size = size;
        _offset = startOffset;
    }

    public unsafe void Init(ref MemReader reader)
    {
        var length = (int)Math.Min((ulong)_buffer.Length, _size - _offset);
        if (length != 0) _file.RandomRead(_buffer.AsSpan(0, length), _offset, false);
        reader.Start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buffer));
        reader.Current = reader.Start;
        reader.End = reader.Start + length;
        _offset += (uint)length;
    }

    public void FillBuf(ref MemReader reader, nuint advisePrefetchLength)
    {
        if (reader.Current < reader.End || advisePrefetchLength == 0) return;
        Init(ref reader);
        if (reader.Current == reader.End) PackUnpack.ThrowEndOfStreamException();
    }

    public long GetCurrentPosition(in MemReader reader) => (long)_offset - (reader.End - reader.Current);

    public unsafe void ReadBlock(ref MemReader reader, ref byte buffer, nuint length)
    {
        // MemReader has already consumed any bytes remaining in its buffer.
        while (length != 0)
        {
            if (length < (nuint)_buffer.Length)
            {
                FillBuf(ref reader, length);
                var count = (uint)Math.Min(length, (nuint)(reader.End - reader.Current));
                Unsafe.CopyBlockUnaligned(ref buffer, ref Unsafe.AsRef<byte>((void*)reader.Current), count);
                reader.Current += (nint)count;
                buffer = ref Unsafe.Add(ref buffer, (nint)count);
                length -= count;
            }
            else
            {
                var count = (int)Math.Min(Math.Min((ulong)length, _size - _offset), int.MaxValue);
                if (count == 0) PackUnpack.ThrowEndOfStreamException();
                _file.RandomRead(MemoryMarshal.CreateSpan(ref buffer, count), _offset, false);
                _offset += (uint)count;
                buffer = ref Unsafe.Add(ref buffer, count);
                length -= (uint)count;
                ClearBuffer(ref reader);
            }
        }
    }

    public void SkipBlock(ref MemReader reader, nuint length)
    {
        ClearBuffer(ref reader);
        if ((ulong)length > _size - _offset)
        {
            _offset = _size;
            PackUnpack.ThrowEndOfStreamException();
        }
        _offset += (ulong)length;
    }

    public void SetCurrentPosition(ref MemReader reader, long position)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(position);
        if ((ulong)position > _size) PackUnpack.ThrowEndOfStreamException();
        _offset = (ulong)position;
        ClearBuffer(ref reader);
    }

    static void ClearBuffer(ref MemReader reader)
    {
        reader.Start = reader.Current = reader.End = 0;
    }

    public bool Eof(ref MemReader reader) => (ulong)GetCurrentPosition(reader) == _size;
}
