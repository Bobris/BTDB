using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BTDB.Buffer;

namespace BTDB.EventStoreLayer
{
    public class MemoryEventFileStorage : IEventFileStorage
    {
        public MemoryEventFileStorage(uint maxBlockSize = 4096)
        {
            MaxBlockSize = maxBlockSize;
        }

        const int BlockSize = 4096;
        readonly List<byte[]> _blocks = new List<byte[]>();
        ulong _writePos;

        public uint MaxBlockSize { get; private set; }

        public Task<uint> Read(ByteBuffer buf, ulong position)
        {
            CheckBufLength(buf);
            var read = 0;
            var size = buf.Length;
            var offset = 0;
            while (size > 0)
            {
                var index = (int)(position / BlockSize);
                if (index >= _blocks.Count) break;
                var blk = _blocks[index];
                var startOfs = (int)(position % BlockSize);
                var rest = BlockSize - startOfs;
                if (size < rest) rest = size;
                Array.Copy(blk, startOfs, buf.Buffer, buf.Offset + offset, rest);
                position += (ulong)rest;
                read += rest;
                offset += rest;
                size -= rest;
            }
            return Task.FromResult((uint)read);
        }

        void CheckBufLength(ByteBuffer buf)
        {
            if (buf.Length > MaxBlockSize) throw new ArgumentOutOfRangeException("buf", "buf length is over MaxBlockSize");
        }

        public void SetWritePosition(ulong position)
        {
            _writePos = position;
        }

        public Task Write(ByteBuffer buf)
        {
            CheckBufLength(buf);
            var newBlockCount = (int)(((long)_writePos + buf.Length + BlockSize - 1) / BlockSize);
            while (_blocks.Count < newBlockCount)
            {
                _blocks.Add(new byte[BlockSize]);
            }
            var size = buf.Length;
            var offset = 0;
            while (size > 0)
            {
                var blk = _blocks[(int)(_writePos / BlockSize)];
                var startOfs = (int)(_writePos % BlockSize);
                var rest = blk.Length - startOfs;
                if (size < rest) rest = size;
                Array.Copy(buf.Buffer, buf.Offset+offset, blk, startOfs, rest);
                _writePos += (ulong)rest;
                offset += rest;
                size -= rest;
            }
            return Task.FromResult(true);
        }
    }
}