using System;
using System.Collections.Generic;

namespace BTDB
{
    public class ManagedMemoryStream : IStream
    {
        ulong _size;
        readonly List<byte[]> _data = new List<byte[]>();
        readonly object _lock = new object();
        const int OneBufSize = 128 * 1024;

        public void Dispose()
        {
        }

        public int Read(byte[] data, int offset, int size, ulong position)
        {
            lock (_lock)
            {
                int read = 0;
                while (size > 0 && position < _size)
                {
                    var buf = _data[(int)(position / OneBufSize)];
                    var startOfs = (int)(position % OneBufSize);
                    var rest = buf.Length - startOfs;
                    if (size < rest) rest = size;
                    Array.Copy(buf, startOfs, data, offset, rest);
                    position += (ulong)rest;
                    read += rest;
                    offset += rest;
                    size -= rest;
                }
                return read;
            }
        }

        public void Write(byte[] data, int offset, int size, ulong position)
        {
            lock (_lock)
            {
                if (position + (ulong)size > _size) SetSizeInternal(position + (ulong)size);
                while (size > 0)
                {
                    var buf = _data[(int)(position / OneBufSize)];
                    var startOfs = (int)(position % OneBufSize);
                    var rest = buf.Length - startOfs;
                    if (size < rest) rest = size;
                    Array.Copy(data, offset, buf, startOfs, rest);
                    position += (ulong)rest;
                    offset += rest;
                    size -= rest;
                }
            }
        }

        public void Flush()
        {
        }

        public void HardFlush()
        {
        }

        public ulong GetSize()
        {
            lock (_lock)
            {
                return _size;
            }
        }

        public void SetSize(ulong newSize)
        {
            lock (_lock)
            {
                SetSizeInternal(newSize);
            }
        }

        void SetSizeInternal(ulong newSize)
        {
            while (_data.Count < (int)((newSize + OneBufSize - 1) / OneBufSize))
            {
                _data.Add(new byte[OneBufSize]);
            }
            _size = newSize;
        }
    }
}