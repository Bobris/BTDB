using System;
using System.IO;
using Microsoft.Win32.SafeHandles;

namespace BTDB.StreamLayer
{
    public class PositionLessFileStreamProxy : IPositionLessStream
    {
        readonly FileStream _stream;
        readonly SafeFileHandle _handle;
        readonly bool _dispose;

        public PositionLessFileStreamProxy(string fileName)
        {
            if (string.IsNullOrEmpty(fileName)) throw new ArgumentOutOfRangeException(nameof(fileName));
            _stream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1, FileOptions.None);
            _handle = _stream.SafeFileHandle;
            _dispose = true;
        }

        public PositionLessFileStreamProxy(FileStream stream, bool dispose)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            _stream = stream;
            _dispose = dispose;
        }

        public int Read(byte[] data, int offset, int size, ulong pos)
        {
            return (int)PlatformMethods.Instance.PRead(_handle, data.AsSpan(offset, size), pos);
        }

        public void Write(byte[] data, int offset, int size, ulong pos)
        {
            PlatformMethods.Instance.PWrite(_handle, data.AsSpan(offset, size), pos);
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
}
