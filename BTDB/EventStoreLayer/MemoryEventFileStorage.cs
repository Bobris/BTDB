using System;
using BTDB.Buffer;
using BTDB.Collections;

namespace BTDB.EventStoreLayer;

public class MemoryEventFileStorage : IEventFileStorage
{
    public MemoryEventFileStorage(uint maxBlockSize = 4096, ulong maxFileSize = int.MaxValue)
    {
        MaxBlockSize = maxBlockSize;
        MaxFileSize = maxFileSize;
    }

    const uint BlockSize = 4096;
    StructList<byte[]> _blocks;

    public uint MaxBlockSize { get; }

    public ulong MaxFileSize { get; }

    public uint Read(ByteBuffer buf, ulong position)
    {
        CheckBufLength(buf);
        var read = 0u;
        var size = (uint)buf.Length;
        var offset = 0u;
        while (size > 0)
        {
            var index = (int)(position / BlockSize);
            if (index >= _blocks.Count) break;
            var blk = _blocks[index];
            var startOfs = (uint)(position % BlockSize);
            var rest = BlockSize - startOfs;
            if (size < rest) rest = size;
            Array.Copy(blk, startOfs, buf.Buffer, buf.Offset + offset, rest);
            position += rest;
            read += rest;
            offset += rest;
            size -= rest;
        }
        return read;
    }

    void CheckBufLength(ByteBuffer buf)
    {
        if (buf.Length > MaxBlockSize) throw new ArgumentOutOfRangeException(nameof(buf), "buf length is over MaxBlockSize");
    }

    public void Write(ByteBuffer buf, ulong position)
    {
        CheckBufLength(buf);
        var newBlockCount = (int)(((long)position + buf.Length + BlockSize - 1) / BlockSize);
        while (_blocks.Count < newBlockCount)
        {
            _blocks.Add(new byte[BlockSize]);
        }
        var size = (uint)buf.Length;
        var offset = 0u;
        while (size > 0)
        {
            var blk = _blocks[(int)(position / BlockSize)];
            var startOfs = (uint)(position % BlockSize);
            var rest = (uint)blk.Length - startOfs;
            if (size < rest) rest = size;
            Array.Copy(buf.Buffer, buf.Offset + offset, blk, startOfs, rest);
            position += rest;
            offset += rest;
            size -= rest;
        }
    }

    public IEventFileStorage CreateNew(IEventFileStorage file)
    {
        return new MemoryEventFileStorage(MaxBlockSize, MaxFileSize);
    }
}
