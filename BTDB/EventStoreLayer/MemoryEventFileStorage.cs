using System;
using System.Collections.Generic;
using BTDB.Buffer;

namespace BTDB.EventStoreLayer
{
    public class MemoryEventFileStorage : IEventFileStorage
    {
        public MemoryEventFileStorage(uint maxBlockSize = 4096)
        {
            MaxBlockSize = maxBlockSize;
        }

        const uint BlockSize = 4096;
        readonly List<byte[]> _blocks = new List<byte[]>();
        ulong _writePos;

        public uint MaxBlockSize { get; private set; }

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
            if (buf.Length > MaxBlockSize) throw new ArgumentOutOfRangeException("buf", "buf length is over MaxBlockSize");
        }

        public void SetWritePosition(ulong position)
        {
            _writePos = position;
        }

        public void Write(ByteBuffer buf)
        {
            CheckBufLength(buf);
            var newBlockCount = (int)(((long)_writePos + buf.Length + BlockSize - 1) / BlockSize);
            while (_blocks.Count < newBlockCount)
            {
                _blocks.Add(new byte[BlockSize]);
            }
            var size = (uint)buf.Length;
            var offset = 0u;
            while (size > 0)
            {
                var blk = _blocks[(int)(_writePos / BlockSize)];
                var startOfs = (uint)(_writePos % BlockSize);
                var rest = (uint)blk.Length - startOfs;
                if (size < rest) rest = size;
                Array.Copy(buf.Buffer, buf.Offset + offset, blk, startOfs, rest);
                _writePos += rest;
                offset += rest;
                size -= rest;
            }
        }
    }
}