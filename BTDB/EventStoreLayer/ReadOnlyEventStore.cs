using System;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public class ReadOnlyEventStore : IReadEventStore
    {
        protected readonly IEventFileStorage _file;
        protected readonly ITypeSerializersMapping _mapping;
        protected ulong _nextReadPosition;
        const int FirstReadAhead = 4096;
        public const byte StartBlockMagic = 248;

        public ReadOnlyEventStore(IEventFileStorage file, ITypeSerializersMapping mapping)
        {
            _file = file;
            _mapping = mapping;
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
                _mapping.LoadTypeDescriptors(reader);
            }
            var metadata = blockType.HasFlag(BlockType.HasMetadata) ? _mapping.LoadObject(reader) : null;
            var readEvents = observer.ObservedMetadata(metadata);
            if (!readEvents) return;
            if (blockType.HasFlag(BlockType.HasOneEvent))
            {
                observer.ObservedEvents(new[] { _mapping.LoadObject(reader) });
            }
            else if (blockType.HasFlag(BlockType.HasMoreEvents))
            {
                var eventCount = reader.ReadVUInt32();
                var events = new object[eventCount];
                for (var i = 0; i < eventCount; i++)
                {
                    events[i] = _mapping.LoadObject(reader);
                }
                observer.ObservedEvents(events);
            }
            else
            {
                observer.ObservedEvents(new object[0]);
            }
        }

        void SetCorrupted()
        {
            // File if corrupted and cannot be continued to append new events
        }
    }
}