using System;
using System.IO;
using Microsoft.Win32.SafeHandles;

namespace BTDB.StreamLayer;

public class PositionLessFileStreamProxy : IPositionLessStream
{
    readonly FileStream _stream;
    readonly SafeFileHandle _handle;
    readonly bool _dispose;

    public PositionLessFileStreamProxy(string fileName)
    {
        _stream = new(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1,
            FileOptions.None);
        _handle = _stream.SafeFileHandle!;
        _dispose = true;
    }

    public PositionLessFileStreamProxy(FileStream stream, bool dispose)
    {
        _stream = stream;
        _handle = _stream.SafeFileHandle!;
        _dispose = dispose;
    }

    public int Read(Span<byte> data, ulong pos)
    {
        return RandomAccess.Read(_handle, data, (long)pos);
    }

    public void Write(ReadOnlySpan<byte> data, ulong pos)
    {
        RandomAccess.Write(_handle, data, (long)pos);
    }

    public void Flush()
    {
        _stream.Flush();
    }

    public void HardFlush()
    {
        _stream.Flush(true);
    }

    public ulong GetSize()
    {
        return (ulong)_stream.Length;
    }

    public void SetSize(ulong size)
    {
        _stream.SetLength((long)size);
    }

    public void Dispose()
    {
        if (_dispose)
        {
            _handle.Dispose();
            _stream.Dispose();
        }
    }
}
