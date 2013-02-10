using System;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public class ReadOnlyEventStore : IReadEventStore
    {
        protected readonly IEventFileStorage File;
        protected readonly ITypeSerializersMapping Mapping;
        protected ulong NextReadPosition;
        const int FirstReadAhead = 4096;
        protected const uint SectorSize = 512;
        protected const ulong SectorMask = ~(ulong)(SectorSize - 1);
        protected const uint SectorMaskUInt = (uint)(SectorMask & uint.MaxValue);
        protected ulong EndBufferPosition;
        protected readonly byte[] EndBuffer = new byte[SectorSize];
        protected uint EndBufferLen;
        protected readonly uint MaxBlockSize;

        public ReadOnlyEventStore(IEventFileStorage file, ITypeSerializersMapping mapping)
        {
            File = file;
            Mapping = mapping;
            EndBufferPosition = ulong.MaxValue;
            MaxBlockSize = Math.Min(File.MaxBlockSize, 0x1000000); // For Length there is only 3 bytes so maximum could be less
            if (MaxBlockSize < FirstReadAhead) throw new ArgumentException("file.MaxBlockSize is less than FirstReadAhead");
        }

        public Task ReadFromStartToEnd(IEventStoreObserver observer)
        {
            NextReadPosition = 0;
            return ReadToEnd(observer);
        }

        public async Task ReadToEnd(IEventStoreObserver observer)
        {
            var bufferBlock = new byte[FirstReadAhead + MaxBlockSize];
            var bufferStartPosition = NextReadPosition & SectorMask;
            var bufferFullLength = 0;
            var bufferReadOffset = (int)(NextReadPosition - bufferStartPosition);
            var buf = ByteBuffer.NewSync(bufferBlock, bufferFullLength, FirstReadAhead);
            var bufReadLength = (int)await File.Read(buf, bufferStartPosition);
            bufferFullLength += bufReadLength;
            while (true)
            {
                if (bufferReadOffset >= bufferFullLength)
                {
                    break;
                }
                if (bufferReadOffset + 8 > bufferFullLength)
                {
                    SetCorrupted();
                    return;
                }
                var blockCheckSum = PackUnpack.UnpackUInt32LE(bufferBlock, bufferReadOffset);
                bufferReadOffset += 4;
                var blockLen = PackUnpack.UnpackUInt32LE(bufferBlock, bufferReadOffset);
                if (blockCheckSum == 0 && blockLen == 0)
                    break;
                var blockType = (BlockType)(blockLen & 0xff);
                blockLen >>= 8;
                bufferReadOffset += 4;
                if (blockLen == 0 || blockLen + 8 > MaxBlockSize)
                {
                    SetCorrupted();
                    return;
                }
                var bufferLenToFill = ((uint)(bufferReadOffset + (int)blockLen + FirstReadAhead)) & SectorMaskUInt;
                if (bufferLenToFill > bufferBlock.Length) bufferLenToFill = (uint)bufferBlock.Length;
                buf = ByteBuffer.NewSync(bufferBlock, bufferFullLength, (int)(bufferLenToFill - bufferFullLength));
                bufReadLength = (int)await File.Read(buf, bufferStartPosition + (ulong)bufferFullLength);
                bufferFullLength += bufReadLength;
                if (bufferReadOffset + (int)blockLen > bufferFullLength)
                {
                    SetCorrupted();
                    return;
                }
                if (Checksum.CalcFletcher32(bufferBlock, (uint)bufferReadOffset-4, blockLen+4) != blockCheckSum)
                {
                    SetCorrupted();
                    return;
                }
                if ((blockType & (BlockType.FirstBlock | BlockType.MiddleBlock | BlockType.LastBlock)) == (BlockType.FirstBlock | BlockType.LastBlock))
                {
                    Process(blockType, ByteBuffer.NewSync(bufferBlock, bufferReadOffset, (int)blockLen), observer);
                }
                else
                {
                    throw new NotImplementedException();
                }
                NextReadPosition = bufferStartPosition + (ulong)bufferReadOffset + blockLen;
                bufferReadOffset += (int)blockLen;
                var nextBufferStartPosition = NextReadPosition & SectorMask;
                var bufferMoveDistance = (int)(bufferStartPosition - nextBufferStartPosition);
                if (bufferMoveDistance <= 0) continue;
                Array.Copy(bufferBlock, bufferReadOffset, bufferBlock, bufferReadOffset - bufferMoveDistance, bufferFullLength - bufferReadOffset);
                bufferStartPosition = nextBufferStartPosition;
                bufferFullLength -= bufferMoveDistance;
                bufferReadOffset -= bufferMoveDistance;
            }
            EndBufferLen = (uint)(bufferFullLength - bufferFullLength & SectorMaskUInt);
            EndBufferPosition = bufferStartPosition + (ulong)bufferFullLength - EndBufferLen;
            Array.Copy(bufferBlock, bufferFullLength - EndBufferLen, EndBuffer, 0, EndBufferLen);
        }

        void Process(BlockType blockType, ByteBuffer block, IEventStoreObserver observer)
        {
            var reader = new ByteBufferReader(block);
            if (blockType.HasFlag(BlockType.HasTypeDeclaration))
            {
                Mapping.LoadTypeDescriptors(reader);
            }
            var metadata = blockType.HasFlag(BlockType.HasMetadata) ? Mapping.LoadObject(reader) : null;
            uint eventCount;
            if (blockType.HasFlag(BlockType.HasOneEvent))
            {
                eventCount = 1;
            }
            else if (blockType.HasFlag(BlockType.HasMoreEvents))
            {
                eventCount = reader.ReadVUInt32();
            }
            else
            {
                eventCount = 0;
            }
            var readEvents = observer.ObservedMetadata(metadata, eventCount);
            if (!readEvents) return;
            var events = new object[eventCount];
            for (var i = 0; i < eventCount; i++)
            {
                events[i] = Mapping.LoadObject(reader);
            }
            observer.ObservedEvents(events);
        }

        void SetCorrupted()
        {
            // File if corrupted and cannot be continued to append new events
        }
    }
}