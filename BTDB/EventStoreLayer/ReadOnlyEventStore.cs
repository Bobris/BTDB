using System;
using System.IO;
using System.Runtime.CompilerServices;
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
        protected const int HeaderSize = 8;
        protected const uint SectorSize = 512;
        protected const ulong SectorMask = ~(ulong)(SectorSize - 1);
        protected const uint SectorMaskUInt = (uint)(SectorMask & uint.MaxValue);
        protected ulong EndBufferPosition;
        protected readonly byte[] EndBuffer = new byte[SectorSize];
        protected uint EndBufferLen;
        protected readonly uint MaxBlockSize;
        bool _knownAsCorrupted;
        internal bool KnownAsFinished;

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
            var overflowWriter = default(ByteBufferWriter);
            var bufferBlock = new byte[FirstReadAhead + MaxBlockSize];
            var bufferStartPosition = NextReadPosition & SectorMask;
            var bufferFullLength = 0;
            var bufferReadOffset = (int)(NextReadPosition - bufferStartPosition);
            var buf = ByteBuffer.NewSync(bufferBlock, bufferFullLength, FirstReadAhead);
            var bufReadLength = (int)await File.Read(buf, bufferStartPosition);
            bufferFullLength += bufReadLength;
            while (true)
            {
                if (bufferReadOffset == bufferFullLength)
                {
                    break;
                }
                if (bufferReadOffset + HeaderSize > bufferFullLength)
                {
                    SetCorrupted();
                    return;
                }
                var blockCheckSum = PackUnpack.UnpackUInt32LE(bufferBlock, bufferReadOffset);
                bufferReadOffset += 4;
                var blockLen = PackUnpack.UnpackUInt32LE(bufferBlock, bufferReadOffset);
                if (blockCheckSum == 0 && blockLen == 0)
                {
                    bufferReadOffset -= 4;
                    break;
                }
                var blockType = (BlockType)(blockLen & 0xff);
                blockLen >>= 8;
                if (blockType == BlockType.LastBlock && blockLen == 0)
                {
                    if (Checksum.CalcFletcher32(bufferBlock, (uint)bufferReadOffset, 4) != blockCheckSum)
                    {
                        SetCorrupted();
                        return;
                    }
                    KnownAsFinished = true;
                    return;
                }
                if (blockLen == 0)
                {
                    SetCorrupted();
                    return;
                }
                if (blockLen + HeaderSize > MaxBlockSize)
                {
                    SetCorrupted();
                    return;
                }
                bufferReadOffset += 4;
                var bufferLenToFill = ((uint)(bufferReadOffset + (int)blockLen + FirstReadAhead)) & SectorMaskUInt;
                if (bufferLenToFill > bufferBlock.Length) bufferLenToFill = (uint)bufferBlock.Length;
                buf = ByteBuffer.NewSync(bufferBlock, bufferFullLength, (int)(bufferLenToFill - bufferFullLength));
                if (buf.Length != 0)
                {
                    bufReadLength = (int) await File.Read(buf, bufferStartPosition + (ulong) bufferFullLength);
                    bufferFullLength += bufReadLength;
                }
                if (bufferReadOffset + (int)blockLen > bufferFullLength)
                {
                    SetCorrupted();
                    return;
                }
                if (Checksum.CalcFletcher32(bufferBlock, (uint)bufferReadOffset - 4, blockLen + 4) != blockCheckSum)
                {
                    SetCorrupted();
                    return;
                }
                var blockTypeBlock = blockType & (BlockType.FirstBlock | BlockType.MiddleBlock | BlockType.LastBlock);
                if (blockTypeBlock == (BlockType.FirstBlock | BlockType.LastBlock))
                {
                    Process(blockType, ByteBuffer.NewSync(bufferBlock, bufferReadOffset, (int)blockLen), observer);
                }
                else
                {
                    if (blockTypeBlock == BlockType.FirstBlock)
                    {
                        overflowWriter = new ByteBufferWriter();
                    }
                    else if (blockTypeBlock == BlockType.MiddleBlock || blockTypeBlock == BlockType.LastBlock)
                    {
                        if (overflowWriter == null)
                        {
                            SetCorrupted();
                            return;
                        }
                    }
                    else
                    {
                        SetCorrupted();
                        return;
                    }
                    overflowWriter.WriteBlock(ByteBuffer.NewSync(bufferBlock, bufferReadOffset, (int)blockLen));
                    if (blockTypeBlock == BlockType.LastBlock)
                    {
                        Process(blockType, overflowWriter.Data, observer);
                        overflowWriter = null;
                    }
                }
                NextReadPosition = bufferStartPosition + (ulong)bufferReadOffset + blockLen;
                bufferReadOffset += (int)blockLen;
                var nextBufferStartPosition = NextReadPosition & SectorMask;
                var bufferMoveDistance = (int)(nextBufferStartPosition - bufferStartPosition);
                if (bufferMoveDistance <= 0) continue;
                Array.Copy(bufferBlock, bufferReadOffset, bufferBlock, bufferReadOffset - bufferMoveDistance, bufferFullLength - bufferReadOffset);
                bufferStartPosition = nextBufferStartPosition;
                bufferFullLength -= bufferMoveDistance;
                bufferReadOffset -= bufferMoveDistance;
            }
            if (overflowWriter != null)
            {
                SetCorrupted();
                return;
            }
            EndBufferLen = (uint)(bufferReadOffset - (bufferReadOffset & SectorMaskUInt));
            EndBufferPosition = bufferStartPosition + (ulong)bufferReadOffset - EndBufferLen;
            Array.Copy(bufferBlock, bufferReadOffset - EndBufferLen, EndBuffer, 0, EndBufferLen);
        }

        public bool IsKnownAsCorrupted()
        {
            return _knownAsCorrupted;
        }

        public bool IsKnownAsFinished()
        {
            return KnownAsFinished;
        }

        public bool IsKnownAsAppendable()
        {
            return EndBufferPosition != ulong.MaxValue;
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

        void SetCorrupted([CallerLineNumber] int sourceLineNumber = 0)
        {
            _knownAsCorrupted = true;
            EndBufferPosition = ulong.MaxValue;
            throw new InvalidDataException(string.Format("EventStore is corrupted (detailed line number {0})", sourceLineNumber));
        }
    }
}