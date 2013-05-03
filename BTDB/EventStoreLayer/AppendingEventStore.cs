using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public class AppendingEventStore : ReadOnlyEventStore, IWriteEventStore
    {
        readonly byte[] _zeroes = new byte[SectorSize + HeaderSize];

        public AppendingEventStore(IEventFileStorage file, ITypeSerializersMapping mapping, ICompressionStrategy compressionStrategy)
            : base(file, mapping, compressionStrategy)
        {
        }

        public void Store(object metadata, object[] events)
        {
            Store(metadata, new List<object>(events));
        }

        public void Store(object metadata, IReadOnlyList<object> events)
        {
            if (!IsKnownAsAppendable())
            {
                ReadToEnd(new SkippingEventObserver());
            }
            if (IsKnownAsFinished()) throw new InvalidOperationException("Cannot append to already finished EventStore");
            var writer = new ByteBufferWriter();
            var startOffset = (int)EndBufferLen + HeaderSize;
            writer.WriteBlock(_zeroes, 0, startOffset);
            IDescriptorSerializerContext serializerContext = Mapping;
            if (metadata != null)
                serializerContext = serializerContext.StoreNewDescriptors(writer, metadata);
            if (events != null)
            {
                foreach (var o in events)
                {
                    serializerContext = serializerContext.StoreNewDescriptors(writer, o);
                }
                if (events.Count == 0) events = null;
            }
            serializerContext.FinishNewDescriptors(writer);
            var blockType = BlockType.FirstBlock;
            if (serializerContext.SomeTypeStored)
                blockType |= BlockType.HasTypeDeclaration;
            if (metadata != null)
            {
                serializerContext.StoreObject(writer, metadata);
                blockType |= BlockType.HasMetadata;
            }
            if (events != null)
            {
                if (events.Count == 1)
                {
                    serializerContext.StoreObject(writer, events[0]);
                    blockType |= BlockType.HasOneEvent;
                }
                else
                {
                    writer.WriteVUInt32((uint)events.Count);
                    foreach (var o in events)
                    {
                        serializerContext.StoreObject(writer, o);
                    }
                    blockType |= BlockType.HasMoreEvents;
                }
            }
            var lenWithoutEndPadding = (int)writer.GetCurrentPosition();
            writer.WriteBlock(_zeroes, 0, (int)(SectorSize - 1));
            var block = writer.Data;
            if (CompressionStrategy.ShouldTryToCompress(lenWithoutEndPadding - startOffset))
            {
                var compressedBlock = ByteBuffer.NewSync(block.Buffer, startOffset, lenWithoutEndPadding - startOffset);
                if (CompressionStrategy.Compress(ref compressedBlock))
                {
                    blockType |= BlockType.Compressed;
                    Array.Copy(compressedBlock.Buffer, compressedBlock.Offset, block.Buffer, startOffset, compressedBlock.Length);
                    lenWithoutEndPadding = startOffset + compressedBlock.Length;
                    Array.Copy(_zeroes, 0, block.Buffer, lenWithoutEndPadding, (int)SectorSize - 1);
                }
            }
            do
            {
                var blockLen = MaxBlockSize - (EndBufferLen + HeaderSize);
                if (blockLen >= lenWithoutEndPadding - startOffset)
                {
                    blockLen = (uint)(lenWithoutEndPadding - startOffset);
                    blockType &= ~BlockType.MiddleBlock;
                    blockType |= BlockType.LastBlock;
                }
                WriteOneBlock(ByteBuffer.NewSync(block.Buffer, block.Offset + startOffset, (int)blockLen), blockType);
                startOffset += (int)blockLen;
                blockType &= ~BlockType.FirstBlock;
                blockType |= BlockType.MiddleBlock;
            } while (lenWithoutEndPadding > startOffset);
            serializerContext.CommitNewDescriptors();
        }

        public void FinalizeStore()
        {
            if (IsKnownAsFinished()) return;
            if (!IsKnownAsAppendable())
            {
                ReadToEnd(new SkippingEventObserver());
            }
            if (IsKnownAsFinished()) return;
            var startOffset = (int)EndBufferLen + HeaderSize;
            WriteOneBlock(ByteBuffer.NewSync(_zeroes, startOffset, 0), BlockType.LastBlock);
            EndBufferPosition = ulong.MaxValue;
            KnownAsFinished = true;
        }

        public ulong KnownAppendablePosition()
        {
            if (!IsKnownAsAppendable()) throw new InvalidOperationException("IsKnownAsAppendable needs to return true before calling this method. Use ReadToEnd(new SkippingEventObserver()).Wait() to initialize.");
            return EndBufferPosition + EndBufferLen;
        }

        void WriteOneBlock(ByteBuffer block, BlockType blockType)
        {
            var blockLen = (uint)block.Length;
            var o = block.Offset - 4;
            PackUnpack.PackUInt32LE(block.Buffer, o, (blockLen << 8) + (uint)blockType);
            var checksum = Checksum.CalcFletcher32(block.Buffer, (uint)o, blockLen + 4);
            o -= 4;
            PackUnpack.PackUInt32LE(block.Buffer, o, checksum);
            o -= (int)EndBufferLen;
            Array.Copy(EndBuffer, 0, block.Buffer, o, EndBufferLen);
            var lenWithoutEndPadding = (int)(EndBufferLen + HeaderSize + blockLen);
            block = ByteBuffer.NewAsync(block.Buffer, o, (int)((uint)(lenWithoutEndPadding + SectorSize - 1) & SectorMaskUInt));
            File.Write(block, EndBufferPosition);
            NextReadPosition = EndBufferPosition + (ulong)lenWithoutEndPadding;
            EndBufferPosition = NextReadPosition & SectorMask;
            EndBufferLen = (uint)(NextReadPosition - EndBufferPosition);
            Array.Copy(block.Buffer, block.Offset + lenWithoutEndPadding - EndBufferLen, EndBuffer, 0, EndBufferLen);
        }
    }
}