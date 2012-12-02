using System;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public class AppendingEventStore : ReadOnlyEventStore, IWriteEventStore
    {
        public AppendingEventStore(IEventFileStorage file, ITypeSerializersMapping mapping)
            : base(file, mapping)
        {
        }

        public async Task Store(object metadata, object[] events)
        {
            var writer = new ByteBufferWriter();
            writer.WriteInt64(0);
            writer.WriteInt32(0);
            IDescriptorSerializerContext serializerContext = _mapping;
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
            var block = writer.Data;
            if (block.Length <= _file.MaxBlockSize)
            {
                blockType |= BlockType.LastBlock;
                block.Buffer[block.Offset + 11] = (byte)blockType;
                var blockLen = (uint)block.Length - 11;
                var checksum = Checksum.CalcFletcher32(block.Buffer, (uint)(block.Offset + 11), blockLen);
                var blockLenLen = PackUnpack.LengthVUInt(blockLen);
                var o = block.Offset + 11 - blockLenLen;
                PackUnpack.PackVUInt(block.Buffer, ref o, blockLen);
                o -= blockLenLen + 4;
                PackUnpack.PackUInt32LE(block.Buffer, o, checksum);
                o--;
                block.Buffer[o] = StartBlockMagic;
                block = ByteBuffer.NewAsync(block.Buffer, o, (int) (1 + 4 + blockLenLen + 1 + blockLen));
                _file.SetWritePosition(_nextReadPosition);
                await _file.Write(block);
                _nextReadPosition += (ulong)block.Length;
            }
            else
            {
                throw new NotImplementedException();
            }
            serializerContext.CommitNewDescriptors();
        }
    }
}