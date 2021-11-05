using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public class AppendingEventStore : ReadOnlyEventStore, IWriteEventStore
{
    readonly byte[] _zeroes = new byte[SectorSize + HeaderSize];

    public AppendingEventStore(IEventFileStorage file, ITypeSerializersMapping mapping, ICompressionStrategy compressionStrategy)
        : base(file, mapping, compressionStrategy)
    {
    }

    public void Store(object metadata, IReadOnlyList<object> events)
    {
        if (!IsKnownAsAppendable())
        {
            ReadToEnd(new SkippingEventObserver());
        }
        if (IsKnownAsFinished()) throw new InvalidOperationException("Cannot append to already finished EventStore");
        SerializeIntoBuffer(metadata, events, out var startOffset, out var serializerContext, out var blockType, out var lenWithoutEndPadding, out var block);
        if (SpaceNeeded(startOffset, lenWithoutEndPadding) + EndBufferPosition + EndBufferLen > File.MaxFileSize)
        {
            FinalizeStore();
            File = File.CreateNew(File);
            NextReadPosition = 0;
            EndBufferPosition = 0;
            EndBufferLen = 0;
            KnownAsFinished = false;
            Mapping.Reset();
            SerializeIntoBuffer(metadata, events, out startOffset, out serializerContext, out blockType, out lenWithoutEndPadding, out block);
            if ((ulong)lenWithoutEndPadding > File.MaxFileSize)
            {
                throw new ArgumentOutOfRangeException(
                    $"Size of events are bigger than MaxFileSize {lenWithoutEndPadding}>{File.MaxFileSize}");
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
        } while (startOffset < lenWithoutEndPadding);
        serializerContext.CommitNewDescriptors();
    }

    void SerializeIntoBuffer(object? metadata, IReadOnlyList<object>? events, out int startOffset,
                             out IDescriptorSerializerContext serializerContext, out BlockType blockType,
                             out int lenWithoutEndPadding, out ByteBuffer block)
    {
        startOffset = (int)EndBufferLen + HeaderSize;
        var writer = new SpanWriter();
        writer.WriteBlock(_zeroes.AsSpan(0, startOffset));
        serializerContext = Mapping;
        if (metadata != null)
            serializerContext = serializerContext.StoreNewDescriptors(metadata);
        if (events != null)
        {
            foreach (var o in events)
            {
                serializerContext = serializerContext.StoreNewDescriptors(o);
            }
            if (events.Count == 0) events = null;
        }
        serializerContext.FinishNewDescriptors(ref writer);
        blockType = BlockType.FirstBlock;
        if (serializerContext.SomeTypeStored)
            blockType |= BlockType.HasTypeDeclaration;
        if (metadata != null)
        {
            serializerContext.StoreObject(ref writer, metadata);
            blockType |= BlockType.HasMetadata;
        }
        if (events != null)
        {
            if (events.Count == 1)
            {
                serializerContext.StoreObject(ref writer, events[0]);
                blockType |= BlockType.HasOneEvent;
            }
            else
            {
                writer.WriteVUInt32((uint)events.Count);
                foreach (var o in events)
                {
                    serializerContext.StoreObject(ref writer, o);
                }
                blockType |= BlockType.HasMoreEvents;
            }
        }
        lenWithoutEndPadding = (int)writer.GetCurrentPosition();
        writer.WriteBlock(_zeroes.AsSpan(0, (int)(SectorSize - 1)));
        block = writer.GetByteBufferAndReset();
        if (CompressionStrategy.ShouldTryToCompress(lenWithoutEndPadding - startOffset))
        {
            var compressedBlock = new ReadOnlySpan<byte>(block.Buffer, startOffset, lenWithoutEndPadding - startOffset);
            if (CompressionStrategy.Compress(ref compressedBlock))
            {
                blockType |= BlockType.Compressed;
                compressedBlock.CopyTo(new Span<byte>(block.Buffer, startOffset, compressedBlock.Length));
                lenWithoutEndPadding = startOffset + compressedBlock.Length;
                new Span<byte>(block.Buffer, lenWithoutEndPadding, (int)SectorSize - 1).Clear();
            }
        }
    }

    ulong SpaceNeeded(int startOffset, int lenWithoutEndPadding)
    {
        var res = 0u;
        do
        {
            var blockLen = MaxBlockSize - (EndBufferLen + HeaderSize);
            if (blockLen >= lenWithoutEndPadding - startOffset)
            {
                blockLen = (uint)(lenWithoutEndPadding - startOffset);
            }
            res += HeaderSize + blockLen;
            startOffset += (int)blockLen;
        } while (startOffset < lenWithoutEndPadding);
        return res;
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
        if (EndBufferPosition + (ulong)startOffset <= File.MaxFileSize)
        {
            WriteOneBlock(ByteBuffer.NewSync(new byte[SectorSize * 2], startOffset, 0), BlockType.LastBlock);
        }
        EndBufferPosition = ulong.MaxValue;
        KnownAsFinished = true;
    }

    public ulong KnownAppendablePosition()
    {
        if (!IsKnownAsAppendable()) throw new InvalidOperationException("IsKnownAsAppendable needs to return true before calling this method. Use ReadToEnd(new SkippingEventObserver()).Wait() to initialize.");
        return EndBufferPosition + EndBufferLen;
    }

    public IEventFileStorage CurrentFileStorage => File;

    void WriteOneBlock(ByteBuffer block, BlockType blockType)
    {
        var blockLen = (uint)block.Length;
        var o = block.Offset - 4;
        PackUnpack.PackUInt32LE(block.Buffer!, o, (blockLen << 8) + (uint)blockType);
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
        Array.Copy(block.Buffer!, block.Offset + lenWithoutEndPadding - EndBufferLen, EndBuffer, 0, EndBufferLen);
    }
}
