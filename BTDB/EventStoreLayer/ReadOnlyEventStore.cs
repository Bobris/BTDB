using System;
using System.IO;
using System.Runtime.CompilerServices;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public class ReadOnlyEventStore : IReadEventStore
    {
        protected IEventFileStorage File;
        protected readonly ITypeSerializersMapping Mapping;
        protected readonly ICompressionStrategy CompressionStrategy;
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

        public ReadOnlyEventStore(IEventFileStorage file, ITypeSerializersMapping mapping, ICompressionStrategy compressionStrategy)
        {
            File = file;
            Mapping = mapping;
            CompressionStrategy = compressionStrategy;
            EndBufferPosition = ulong.MaxValue;
            MaxBlockSize = Math.Min(File.MaxBlockSize, 0x1000000); // For Length there is only 3 bytes so maximum could be less
            if (MaxBlockSize < FirstReadAhead) throw new ArgumentException("file.MaxBlockSize is less than FirstReadAhead");
        }

        public void ReadFromStartToEnd(IEventStoreObserver observer)
        {
            NextReadPosition = 0;
            ReadToEnd(observer);
        }

        public void ReadToEnd(IEventStoreObserver observer)
        {
            var overflowWriter = default(ByteBufferWriter);
            var bufferBlock = new byte[FirstReadAhead + MaxBlockSize];
            var bufferStartPosition = NextReadPosition & SectorMask;
            var bufferFullLength = 0;
            var bufferReadOffset = (int)(NextReadPosition - bufferStartPosition);
            var buf = ByteBuffer.NewSync(bufferBlock, bufferFullLength, FirstReadAhead);
            var bufReadLength = (int)File.Read(buf, bufferStartPosition);
            bufferFullLength += bufReadLength;
            while (true)
            {
                if (bufferStartPosition + (ulong)bufferReadOffset + HeaderSize > File.MaxFileSize)
                {
                    KnownAsFinished = true;
                    return;
                }
                if (bufferReadOffset == bufferFullLength)
                {
                    break;
                }
                if (bufferReadOffset + HeaderSize > bufferFullLength)
                {
                    for (var i = bufferReadOffset; i < bufferFullLength; i++)
                    {
                        if (bufferBlock[i] != 0)
                        {
                            SetCorrupted();
                            return;
                        }
                    }
                    break;
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
                if (blockLen == 0 && blockType != (BlockType.FirstBlock | BlockType.LastBlock))
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
                    bufReadLength = (int)File.Read(buf, bufferStartPosition + (ulong)bufferFullLength);
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
                var stopReadingRequested = false;
                if (blockTypeBlock == (BlockType.FirstBlock | BlockType.LastBlock))
                {
                    stopReadingRequested = Process(blockType, ByteBuffer.NewSync(bufferBlock, bufferReadOffset, (int)blockLen), observer);
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
                        stopReadingRequested = Process(blockType, overflowWriter.Data, observer);
                        overflowWriter = null;
                    }
                }
                bufferReadOffset += (int)blockLen;
                if (overflowWriter == null)
                    NextReadPosition = bufferStartPosition + (ulong)bufferReadOffset;
                if (stopReadingRequested)
                {
                    return;
                }
                var nextBufferStartPosition = (bufferStartPosition + (ulong)bufferReadOffset) & SectorMask;
                var bufferMoveDistance = (int)(nextBufferStartPosition - bufferStartPosition);
                if (bufferMoveDistance <= 0) continue;
                Array.Copy(bufferBlock, bufferMoveDistance, bufferBlock, 0, bufferFullLength - bufferMoveDistance);
                bufferStartPosition = nextBufferStartPosition;
                bufferFullLength -= bufferMoveDistance;
                bufferReadOffset -= bufferMoveDistance;
            }
            if (overflowWriter != null)
            {
                // It is not corrupted here just unfinished, but definitely not appendable
                EndBufferPosition = ulong.MaxValue;
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

        bool Process(BlockType blockType, ByteBuffer block, IEventStoreObserver observer)
        {
            if (blockType.HasFlag(BlockType.Compressed))
            {
                CompressionStrategy.Decompress(ref block);
            }
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
            if (!readEvents) return observer.ShouldStopReadingNextEvents();
            var events = new object[eventCount];
            for (var i = 0; i < eventCount; i++)
            {
                events[i] = Mapping.LoadObject(reader);
            }
            observer.ObservedEvents(events);
            return observer.ShouldStopReadingNextEvents();
        }

        void SetCorrupted([CallerLineNumber] int sourceLineNumber = 0)
        {
            _knownAsCorrupted = true;
            EndBufferPosition = ulong.MaxValue;
            throw new InvalidDataException($"EventStore is corrupted (detailed line number {sourceLineNumber})");
        }
    }
}