using System;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    [Flags]
    enum BlockType : byte
    {
        FirstBlock = 1,
        MiddleBlock = 2,
        LastBlock = 4,
        HasTypeDeclaration = 8,
        HasMetadata = 16,
        HasOneEvent = 32,
        HasMoreEvents = 64,
    }

    internal class ReadOnlyEventStore : IReadEventStore
    {
        readonly EventStoreManager _eventStoreManager;
        readonly IEventFileStorage _file;
        ulong _nextReadPosition;
        const int FirstReadAhead = 4096;
        const byte StartBlockMagic = 248;


        public ReadOnlyEventStore(EventStoreManager eventStoreManager, IEventFileStorage file)
        {
            _eventStoreManager = eventStoreManager;
            _file = file;
            if (_file.MaxBlockSize < FirstReadAhead) throw new ArgumentException("file.MaxBlockSize is less than FirstReadAhead");
        }

        public Task ReadFromStartToEnd(IEventStoreObserver observer)
        {
            _nextReadPosition = 0;
            return ReadToEnd(observer);
        }


        public async Task ReadToEnd(IEventStoreObserver observer)
        {
            var bufferBlock = new byte[FirstReadAhead + _file.MaxBlockSize];
            var bufferStartPosition = _nextReadPosition & (~511UL);
            var bufferFullLength = 0;
            var bufferReadOffset = (int)(_nextReadPosition - bufferStartPosition);
            var buf = ByteBuffer.NewSync(bufferBlock, bufferFullLength, FirstReadAhead);
            var bufReadLength = (int)await _file.Read(buf, bufferStartPosition);
            bufferFullLength += bufReadLength;
            while (true)
            {
                if (bufferReadOffset >= bufferFullLength)
                {
                    return;
                }
                var b = bufferBlock[bufferReadOffset];
                if (b == 0)
                {
                    return;
                }
                if (b != StartBlockMagic)
                {
                    SetCorrupted();
                    return;
                }
                bufferReadOffset++;
                if (bufferReadOffset + 6 > bufferFullLength)
                {
                    SetCorrupted();
                    return;
                }
                var blockCheckSum = PackUnpack.UnpackUInt32LE(bufferBlock, bufferReadOffset);
                bufferReadOffset += 4;
                var blockLenLen = PackUnpack.LengthVUInt(bufferBlock, bufferReadOffset);
                if (bufferReadOffset + blockLenLen > bufferFullLength)
                {
                    SetCorrupted();
                    return;
                }
                var blockLen = PackUnpack.UnpackVUInt(bufferBlock, ref bufferReadOffset);
                if (blockLen == 0 || blockLen > _file.MaxBlockSize)
                {
                    SetCorrupted(); // Maybe allow this in future
                    return;
                }
                var bufferLenToFill = ((uint)(bufferReadOffset + (int)blockLen + FirstReadAhead)) & (~511u);
                if (bufferLenToFill > bufferBlock.Length) bufferLenToFill = (uint)bufferBlock.Length;
                buf = ByteBuffer.NewSync(bufferBlock, bufferFullLength, (int)(bufferLenToFill - bufferFullLength));
                bufReadLength = (int)await _file.Read(buf, bufferStartPosition + (ulong)bufferFullLength);
                bufferFullLength += bufReadLength;
                if (bufferReadOffset + (int)blockLen > bufferFullLength)
                {
                    SetCorrupted();
                    return;
                }
                if (Checksum.CalcFletcher32(bufferBlock, (uint)bufferReadOffset, (uint)blockLen) != blockCheckSum)
                {
                    SetCorrupted();
                    return;
                }
                var blockType = (BlockType)bufferBlock[bufferReadOffset];
                bufferReadOffset++;
                blockLen--;
                if ((blockType & (BlockType.FirstBlock | BlockType.MiddleBlock | BlockType.LastBlock)) == (BlockType.FirstBlock | BlockType.LastBlock))
                {
                    Process(blockType, ByteBuffer.NewSync(bufferBlock, bufferReadOffset, (int)blockLen), observer);
                }
                else
                {
                    throw new NotImplementedException();
                }
                _nextReadPosition = bufferStartPosition + (ulong)bufferReadOffset + blockLen;
                bufferReadOffset += (int)blockLen;
                var nextBufferStartPosition = _nextReadPosition & (~511UL);
                var bufferMoveDistance = (int)(bufferStartPosition - nextBufferStartPosition);
                if (bufferMoveDistance <= 0) continue;
                Array.Copy(bufferBlock, bufferReadOffset, bufferBlock, bufferReadOffset - bufferMoveDistance, bufferFullLength - bufferReadOffset);
                bufferStartPosition = nextBufferStartPosition;
                bufferFullLength -= bufferMoveDistance;
                bufferReadOffset -= bufferMoveDistance;
            }
        }

        void Process(BlockType blockType, ByteBuffer block, IEventStoreObserver observer)
        {
            var reader = new ByteBufferReader(block);
            if (blockType.HasFlag(BlockType.HasTypeDeclaration))
            {
                
            }
            throw new NotImplementedException();
        }

        void SetCorrupted()
        {
            // File if corrupted and cannot be continued to append new events
        }
    }
}