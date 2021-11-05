using System;
using System.Threading;
using BTDB.Collections;
using BTDB.KVDBLayer;

namespace BTDB.StreamLayer;

public class MemoryPositionLessStream : IPositionLessStream
{
    long _size;
    StructList<byte[]> _data;
    readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
    const uint OneBufSize = 128 * 1024;

    public void Dispose()
    {
    }

    public int Read(Span<byte> data, ulong position)
    {
        using (_lock.ReadLock())
        {
            int read = 0;
            while (data.Length > 0 && position < (ulong)_size)
            {
                var buf = _data[(int)(position / OneBufSize)];
                var startOfs = (int)(position % OneBufSize);
                var rest = buf.Length - startOfs;
                if ((ulong)_size - position < (ulong)rest) rest = (int)((ulong)_size - position);
                if (data.Length < rest) rest = data.Length;
                buf.AsSpan(startOfs, rest).CopyTo(data);
                position += (ulong)rest;
                read += rest;
                data = data.Slice(rest);
            }

            return read;
        }
    }

    public void Write(ReadOnlySpan<byte> data, ulong position)
    {
        if (data.Length == 0) return;
        using (_lock.WriteLock())
        {
            if (position + (ulong)data.Length > (ulong)_size) SetSizeInternal(position + (ulong)data.Length);
        }

        while (data.Length > 0)
        {
            byte[] buf;
            using (_lock.ReadLock())
            {
                buf = _data[(int)(position / OneBufSize)];
            }
            var startOfs = (int)(position % OneBufSize);
            var rest = buf.Length - startOfs;
            if (data.Length < rest) rest = data.Length;
            data.Slice(0, rest).CopyTo(buf.AsSpan(startOfs, rest));
            position += (ulong)rest;
            data = data.Slice(rest);
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
        return (ulong)Interlocked.Read(ref _size);
    }

    public void SetSize(ulong newSize)
    {
        using (_lock.WriteLock())
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

        Interlocked.Exchange(ref _size, (long)newSize);
    }
}
