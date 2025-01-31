using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using BTDB.Buffer;
using Microsoft.Win32.SafeHandles;

namespace BTDB.StreamLayer;

public class MemoryMappedMemReader : IMemReader, IDisposable
{
    readonly SafeFileHandle _fileHandle;
    readonly MemoryMappedFile _memoryMappedFile;
    readonly MemoryMappedViewAccessor _viewAccessor;
    readonly unsafe byte* _ptr = null;

    public unsafe MemoryMappedMemReader(string fileName)
    {
        _fileHandle = File.OpenHandle(fileName);
        _memoryMappedFile = MemoryMappedFile.CreateFromFile(_fileHandle, null, 0, MemoryMappedFileAccess.Read,
            HandleInheritability.None,
            false);
        _viewAccessor =
            _memoryMappedFile.CreateViewAccessor(0, RandomAccess.GetLength(_fileHandle), MemoryMappedFileAccess.Read);
        _viewAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);
    }

    ~MemoryMappedMemReader()
    {
        Dispose();
    }

    public unsafe void Init(ref MemReader reader)
    {
        reader.Start = (nint)_ptr;
        reader.End = reader.Start + (nint)_viewAccessor.Capacity;
        reader.Current = reader.Start;
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
        if (length > 0) PackUnpack.ThrowEndOfStreamException();
    }

    public void SkipBlock(ref MemReader memReader, nuint length)
    {
        if (length > 0) PackUnpack.ThrowEndOfStreamException();
    }

    public void SetCurrentPosition(ref MemReader memReader, long position)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(position);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(position, memReader.End - memReader.Start);
        memReader.Current = memReader.Start + (nint)position;
    }

    public bool TryReadBlockAsMemory(ref MemReader memReader, uint length, out ReadOnlyMemory<byte> result)
    {
        if (memReader.Current + length > memReader.End)
        {
            PackUnpack.ThrowEndOfStreamException();
        }

        result = new UnmanagedMemoryManager<byte>(memReader.Current, (int)length).Memory;
        memReader.Current += (nint)length;
        return true;
    }

    public bool Eof(ref MemReader memReader)
    {
        return memReader.Current == memReader.End;
    }

    public void Dispose()
    {
        _viewAccessor?.SafeMemoryMappedViewHandle.ReleasePointer();
        _memoryMappedFile?.Dispose();
        _fileHandle?.Dispose();
        GC.SuppressFinalize(this);
    }

    public bool ThrowIfNotSimpleReader()
    {
        return true;
    }
}
