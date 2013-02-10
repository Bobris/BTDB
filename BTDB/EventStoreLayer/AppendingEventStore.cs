using System;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public class AppendingEventStore : ReadOnlyEventStore, IWriteEventStore
    {
        readonly byte[] _zeroes = new byte[SectorSize + 12];

        public AppendingEventStore(IEventFileStorage file, ITypeSerializersMapping mapping)
            : base(file, mapping)
        {
        }

        public async Task Store(object metadata, object[] events)
        {
            if (EndBufferPosition == ulong.MaxValue)
            {
                await ReadToEnd(new SkippingEventObserver());
            }
            var writer = new ByteBufferWriter();
            var startOffset = (int)EndBufferLen + 8;
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
                if (events.Length == 0) events = null;
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
                if (events.Length == 1)
                {
                    serializerContext.StoreObject(writer, events[0]);
                    blockType |= BlockType.HasOneEvent;
                }
                else
                {
                    writer.WriteVUInt32((uint)events.Length);
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
            if (lenWithoutEndPadding <= MaxBlockSize)
            {
                blockType |= BlockType.LastBlock;
                var blockLen = (uint)(lenWithoutEndPadding - startOffset);
                await WriteOneBlock(ByteBuffer.NewSync(block.Buffer, block.Offset + startOffset, (int)blockLen), blockType);
            }
            else
            {
                throw new NotImplementedException();
            }
            serializerContext.CommitNewDescriptors();
        }

        async Task WriteOneBlock(ByteBuffer block, BlockType blockType)
        {
            var blockLen = (uint)block.Length;
            var o = block.Offset - 4;
            PackUnpack.PackUInt32LE(block.Buffer, o, (blockLen << 8) + (uint)blockType);
            var checksum = Checksum.CalcFletcher32(block.Buffer, (uint)o, blockLen + 4);
            o -= 4;
            PackUnpack.PackUInt32LE(block.Buffer, o, checksum);
            o -= (int)EndBufferLen;
            Array.Copy(EndBuffer, 0, block.Buffer, o, EndBufferLen);
            var lenWithoutEndPadding = (int)(EndBufferLen + 8 + blockLen);
            block = ByteBuffer.NewAsync(block.Buffer, o, (int)((uint)(lenWithoutEndPadding + SectorSize - 1) & SectorMaskUInt));
            File.SetWritePosition(EndBufferPosition);
            await File.Write(block);
            NextReadPosition = EndBufferPosition + (ulong)lenWithoutEndPadding;
            EndBufferPosition = NextReadPosition & SectorMask;
            EndBufferLen = (uint)(NextReadPosition - EndBufferPosition);
            Array.Copy(block.Buffer, block.Offset + lenWithoutEndPadding - EndBufferLen, EndBuffer, 0, EndBufferLen);
        }
    }
}