using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace BTDB.StreamLayer;

public class FileMemWriter : IMemWriter, IDisposable
{
    const int BufLength = 8 * 1024;
    readonly Memory<byte> _memOwner;
    long _currentPosition;
    readonly SafeFileHandle _streamHandle;

    public FileMemWriter(SafeFileHandle streamHandle)
    {
        _memOwner = MemoryMarshal.CreateFromPinnedArray(GC.AllocateUninitializedArray<byte>(BufLength, pinned: true), 0,
            BufLength);
        _streamHandle = streamHandle;
        _currentPosition = 0;
    }

    public FileMemWriter(string fileName) : this(File.OpenHandle(fileName, FileMode.Create, FileAccess.ReadWrite))
    {
    }

    ~FileMemWriter()
    {
        Dispose();
    }

    public unsafe void Init(ref MemWriter memWriter)
    {
        memWriter.Start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(_memOwner.Span));
        memWriter.End = memWriter.Start + _memOwner.Length;
        memWriter.Current = memWriter.Start;
    }

    public unsafe void Flush(ref MemWriter memWriter, uint spaceNeeded)
    {
        var buffer = new ReadOnlySpan<byte>((void*)memWriter.Start, (int)(memWriter.Current - memWriter.Start));
        if (buffer.IsEmpty) return;
        RandomAccess.Write(_streamHandle, buffer, _currentPosition);
        _currentPosition += buffer.Length;
        memWriter.Current = memWriter.Start;
    }

    public long GetCurrentPosition(in MemWriter memWriter)
    {
        return _currentPosition + (memWriter.Current - memWriter.Start);
    }

    public void WriteBlock(ref MemWriter memWriter, ref byte buffer, nuint length)
    {
        Flush(ref memWriter, 0);
        while (length > 0)
        {
            var len = (int)Math.Min(1024 * 1024, length);
            RandomAccess.Write(_streamHandle, MemoryMarshal.CreateReadOnlySpan(ref buffer, len), _currentPosition);
            _currentPosition += len;
            buffer = ref Unsafe.Add(ref buffer, len);
            length -= (nuint)len;
        }
    }

    public void SetCurrentPosition(ref MemWriter memWriter, long position)
    {
        Flush(ref memWriter, 0);
        _currentPosition = position;
    }

    public long GetCurrentPositionWithoutWriter()
    {
        return _currentPosition;
    }

    public void Dispose()
    {
        _streamHandle.Dispose();
        GC.SuppressFinalize(this);
    }
}
