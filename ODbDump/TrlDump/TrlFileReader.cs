using System;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace ODbDump.TrlDump;

public class TrlFileReader
{
    readonly ISpanReader _readerController;
    readonly ICompressionStrategy _compression;
    const int MaxValueSizeInlineInMemory = 7;
    static readonly byte[] MagicStartOfTransaction = "tR"u8.ToArray();

    ulong _commitUlong;

    public TrlFileReader(IFileCollectionFile file)
    {
        _readerController = file.GetExclusiveReader();
        _compression = new SnappyCompressionStrategy();
    }

    public void Iterate(ITrlVisitor visitor)
    {
        bool afterTemporaryEnd = false;
        Span<byte> trueValue = stackalloc byte[12];

        var reader = new SpanReader(_readerController);
        SkipUntilNextTransaction(ref reader);
        while (!reader.Eof)
        {
            var command = (KVCommandType) reader.ReadUInt8();
            if (command == 0 && afterTemporaryEnd)
            {
                return;
            }

            afterTemporaryEnd = false;
            visitor.StartOperation(command & KVCommandType.CommandMask);

            switch (command & KVCommandType.CommandMask)
            {
                case KVCommandType.CreateOrUpdateDeprecated:
                case KVCommandType.CreateOrUpdate:
                {
                    var keyLen = reader.ReadVInt32();
                    var valueLen = reader.ReadVInt32();
                    var key = new byte[keyLen];
                    reader.ReadBlock(key);
                    var keyBuf = ByteBuffer.NewAsync(key);
                    if ((command & KVCommandType.FirstParamCompressed) != 0)
                    {
                        _compression.DecompressKey(ref keyBuf);
                    }

                    trueValue.Clear();
                    if (valueLen <= MaxValueSizeInlineInMemory &&
                        (command & KVCommandType.SecondParamCompressed) == 0)
                    {
                        reader.ReadBlock(ref MemoryMarshal.GetReference(trueValue), (uint) valueLen);
                    }
                    else
                    {
                        var partLen = Math.Min(12, valueLen);
                        reader.ReadBlock(ref MemoryMarshal.GetReference(trueValue), (uint) partLen);
                        reader.SkipBlock(valueLen - partLen);
                    }

                    ExplainCreateOrUpdate(visitor, keyBuf.AsSyncReadOnlySpan(), valueLen, trueValue);
                }
                    break;
                case KVCommandType.UpdateKeySuffix:
                {
                    var keyPrefix = reader.ReadVUInt32();
                    var keyLen = reader.ReadVUInt32();
                    var key = new byte[keyLen];
                    reader.ReadBlock(key);
                    ExplainUpdateKeySuffix(visitor, key.AsSpan(), keyPrefix);
                }
                    break;
                case KVCommandType.EraseOne:
                {
                    var keyLen = reader.ReadVInt32();
                    var key = new byte[keyLen];
                    reader.ReadBlock(key);
                    var keyBuf = ByteBuffer.NewAsync(key);
                    if ((command & KVCommandType.FirstParamCompressed) != 0)
                    {
                        _compression.DecompressKey(ref keyBuf);
                    }
                    ExplainEraseOne(visitor, keyBuf.AsSyncReadOnlySpan());
                }
                    break;
                case KVCommandType.EraseRange:
                {
                    var keyLen1 = reader.ReadVInt32();
                    var keyLen2 = reader.ReadVInt32();
                    reader.SkipBlock(keyLen1);
                    reader.SkipBlock(keyLen2);
                }
                    break;
                case KVCommandType.DeltaUlongs:
                {
                    reader.SkipVUInt32();
                    reader.SkipVUInt64();
                }
                    break;
                case KVCommandType.TransactionStart:
                    if (!reader.CheckMagic(MagicStartOfTransaction))
                        throw new Exception("Invalid transaction magic");
                    visitor.OperationDetail($"file pos: {_readerController.GetCurrentPosition(reader)}");
                    break;
                case KVCommandType.CommitWithDeltaUlong:
                    var delta = reader.ReadVUInt64();
                    _commitUlong+=delta;
                    visitor.OperationDetail($"delta: {delta} {_commitUlong}");
                    break;
                case KVCommandType.EndOfFile:
                    visitor.OperationDetail($"position: {reader.Controller!.GetCurrentPosition(reader)}");
                    return;
                case KVCommandType.TemporaryEndOfFile:
                    afterTemporaryEnd = true;
                    break;
            }
            visitor.EndOperation();
        }
    }

    void SkipUntilNextTransaction(ref SpanReader reader)
    {
        int depth = 0;
        while (!reader.Eof)
        {
            var b = reader.ReadUInt8();
            switch (depth)
            {
                case 0:
                    if (b == 3)
                        depth = 1;
                    break;
                case 1:
                    depth = b == (byte) 't' ? 2 : 0;
                    break;
                case 2:
                    if (b == (byte) 'R')
                        return;
                    else
                        depth = 0;
                    break;
            }
        }
    }

    void ExplainCreateOrUpdate(ITrlVisitor visitor, ReadOnlySpan<byte> key, int valueLength,
        ReadOnlySpan<byte> firstBytes)
    {
        switch (key[0])
        {
            case 0:
                visitor.OperationDetail("metadata");
                break;
            case 1:
            {
                var oid = SkipByteAndReadVUInt64(key);
                var valueReader = new SpanReader(firstBytes);
                var tableId = valueReader.ReadVUInt32();

                visitor.UpsertObject(oid, tableId, key.Length, valueLength);
            }
                break;
            case 2:
            {
                var oid = SkipByteAndReadVUInt64(key);
                visitor.UpsertODBDictionary(oid, key.Length, valueLength);
            }
                break;
            case 3:
            {
                var relationIdx = SkipByteAndReadVUInt64(key);
                visitor.UpsertRelationValue(relationIdx, key.Length, valueLength);
            }
                break;
            case 4:
            {
                var reader = new SpanReader(key);
                reader.SkipBlock(1);
                var relationIdx = reader.ReadVUInt64();
                var skIdx = reader.ReadUInt8();
                visitor.UpsertRelationSecondaryKey(relationIdx, skIdx, key.Length, valueLength);
            }
                break;
        }
    }

    void ExplainUpdateKeySuffix(ITrlVisitor visitor, ReadOnlySpan<byte> key, uint keyPrefix)
    {
        if (key[0] == 3)
        {
            var relationIdx = SkipByteAndReadVUInt64(key);
            visitor.ModifyRelationInKeyValue(relationIdx, key.Length, (int)keyPrefix);
        }
        else
        {
            visitor.OperationDetail("Strange key suffix update key[0]=" + key[0]);
        }
    }

    void ExplainEraseOne(ITrlVisitor visitor, ReadOnlySpan<byte> key)
    {
        switch (key[0])
        {
            case 0:
                visitor.OperationDetail("metadata");
                break;
            case 1:
            {
                var oid = SkipByteAndReadVUInt64(key);
                visitor.EraseObject(oid);
            }
                break;
            case 2:
            {
                var oid = SkipByteAndReadVUInt64(key);
                visitor.EraseODBDictionary(oid, key.Length);
            }
                break;
            case 3:
            {
                var relationIdx = SkipByteAndReadVUInt64(key);
                visitor.EraseRelationValue(relationIdx, key.Length);
            }
                break;
            case 4:
            {
                var reader = new SpanReader(key);
                reader.SkipBlock(1);
                var relationIdx = reader.ReadVUInt64();
                var skIdx = reader.ReadUInt8();
                //visitor.EraseRelationSecondaryKey(relationIdx, skIdx, key.Length);
            }
                break;
        }
    }

    ulong SkipByteAndReadVUInt64(ReadOnlySpan<byte> key)
    {
        var reader = new SpanReader(key);
        reader.SkipBlock(1);
        return reader.ReadVUInt64();
    }
}
