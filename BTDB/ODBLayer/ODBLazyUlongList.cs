using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

interface IInternalODBLazyUlongList
{
    ulong Id { get; }
}

public class ODBLazyUlongList : ILazyUlongList, IInternalODBLazyUlongList, IAmLazyDBObject
{
    readonly IInternalObjectDBTransaction _tr;
    readonly IKeyValueDBTransaction _keyValueTr;
    readonly ulong _id;
    readonly byte[] _prefix;
    ulong? _count;
    ulong _currentRecordIndex;
    byte[]? _currentRecord;
    int _currentRecordLength;
    int _currentRecordCount;
    ulong _currentRecordLastValue;
    bool _currentRecordDirty;

    public ODBLazyUlongList(IInternalObjectDBTransaction tr, ulong id)
    {
        _tr = tr;
        _keyValueTr = tr.KeyValueDBTransaction;
        _id = id;
        var length = PackUnpack.LengthVUInt(id);
        _prefix = new byte[ObjectDB.AllDictionariesPrefixLen + length];
        MemoryMarshal.GetReference(_prefix.AsSpan()) = ObjectDB.AllDictionariesPrefixByte;
        PackUnpack.UnsafePackVUInt(
            ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(_prefix.AsSpan()),
                ObjectDB.AllDictionariesPrefixLen), id, length);
    }

    public ODBLazyUlongList(IInternalObjectDBTransaction tr) : this(tr, tr.AllocateDictionaryId())
    {
    }

    public ulong Id => _id;

    public void Add(ulong value)
    {
        var count = Count;
        var recordIndex = count / LazyUlongList.ValuesPerRecord;
        var countInRecord = (int)(count % LazyUlongList.ValuesPerRecord);
        LoadRecordForAppend(recordIndex, countInRecord);
        if (_currentRecordCount == 0)
            LazyUlongList.WriteFirstValue(_currentRecord!, ref _currentRecordLength, value);
        else
            LazyUlongList.WriteNextValue(_currentRecord!, ref _currentRecordLength, _currentRecordLastValue, value);
        _currentRecordLastValue = value;
        _currentRecordCount++;
        _currentRecordDirty = true;
        _count = count + 1;
        if (_currentRecordCount != LazyUlongList.ValuesPerRecord)
            return;
        using var cursor = _keyValueTr.CreateCursor();
        StoreCurrentDirtyRecord(cursor, stackalloc byte[32]);
        StoreCount(cursor);
        _currentRecordLength = 0;
        _currentRecordCount = 0;
    }

    public void Clear()
    {
        using var cursor = _keyValueTr.CreateCursor();
        cursor.EraseAll(_prefix);
        _count = 0;
        _currentRecord = null;
        _currentRecordLength = 0;
        _currentRecordCount = 0;
        _currentRecordDirty = false;
    }

    public ulong Count
    {
        get
        {
            if (_count.HasValue)
                return _count.Value;
            using var cursor = _keyValueTr.CreateCursor();
            switch (cursor.Find(_prefix, (uint)_prefix.Length))
            {
                case FindResult.Exact:
                {
                    Span<byte> buffer = stackalloc byte[16];
                    _count = PackUnpack.UnpackVUInt(cursor.GetValueSpan(ref buffer));
                    break;
                }
                case FindResult.NotFound:
                    _count = 0;
                    break;
                default:
                    throw new BTDBException("Lazy ulong list is incomplete.");
            }

            return _count.Value;
        }
    }

    public bool IsComplete()
    {
        using var cursor = _keyValueTr.CreateCursor();
        return cursor.Find(_prefix, (uint)_prefix.Length) is FindResult.Exact or FindResult.NotFound;
    }

    public void Flush()
    {
        using var cursor = _keyValueTr.CreateCursor();
        StoreCurrentDirtyRecord(cursor, stackalloc byte[32]);
        StoreCount(cursor);
    }

    void LoadRecordForAppend(ulong recordIndex, int expectedCount)
    {
        if (_currentRecord != null && _currentRecordIndex == recordIndex)
            return;
        using var cursor = _keyValueTr.CreateCursor();
        Span<byte> keyBuffer = stackalloc byte[32];
        if (StoreCurrentDirtyRecord(cursor, keyBuffer))
            StoreCount(cursor);

        _currentRecordIndex = recordIndex;
        _currentRecord ??= new byte[LazyUlongList.MaxRecordPayloadLength];
        _currentRecordLength = 0;
        _currentRecordCount = 0;
        _currentRecordDirty = false;
        if (expectedCount == 0)
            return;
        var key = RecordKey(recordIndex, keyBuffer);
        if (!cursor.FindExactKey(key))
            throw new BTDBException("Lazy ulong list is missing its last record.");
        Memory<byte> valueBuffer = _currentRecord;
        var value = cursor.GetValueMemory(ref valueBuffer, copy: true);
        _currentRecordLength = value.Length;
        _currentRecordCount = LazyUlongList.InspectRecord(valueBuffer.Span[..value.Length],
            out _currentRecordLastValue);
        if (_currentRecordCount != expectedCount)
            throw new BTDBException("Lazy ulong list count does not match its last record.");
    }

    bool StoreCurrentDirtyRecord(IKeyValueDBCursor cursor, Span<byte> keyBuffer)
    {
        if (!_currentRecordDirty || _currentRecord == null)
            return false;
        cursor.CreateOrUpdateKeyValue(RecordKey(_currentRecordIndex, keyBuffer),
            _currentRecord.AsSpan(0, _currentRecordLength));
        _currentRecordDirty = false;
        return true;
    }

    void StoreCount(IKeyValueDBCursor cursor)
    {
        if (!_count.HasValue)
            return;
        if (_count.Value == 0)
        {
            if (cursor.FindExactKey(_prefix))
                cursor.EraseCurrent();
            return;
        }

        Span<byte> value = stackalloc byte[9];
        var length = PackUnpack.LengthVUInt(_count.Value);
        PackUnpack.UnsafePackVUInt(ref MemoryMarshal.GetReference(value), _count.Value, length);
        cursor.CreateOrUpdateKeyValue(_prefix, value[..(int)length]);
    }

    ReadOnlySpan<byte> RecordKey(ulong recordIndex, Span<byte> keyBuffer)
    {
        var recordLength = PackUnpack.LengthVUInt(recordIndex);
        var key = keyBuffer[..(_prefix.Length + (int)recordLength)];
        _prefix.CopyTo(key);
        PackUnpack.UnsafePackVUInt(ref key[_prefix.Length], recordIndex, recordLength);
        return key;
    }

    public void ApplyCommands(ReadOnlyMemory<byte> commands)
    {
        if (_currentRecordDirty)
            throw new InvalidOperationException(
                "Cannot apply lazy ulong list commands while the list has unflushed changes.");
        _currentRecord = null;
        _currentRecordLength = 0;
        _currentRecordCount = 0;

        using var cursor = _keyValueTr.CreateCursor();
        var span = commands.Span;
        var offset = 0;
        Span<byte> keyBuffer = stackalloc byte[32];
        while (offset < span.Length)
        {
            var command = span[offset++];
            switch (command)
            {
                case LazyUlongList.CommandClear:
                    cursor.EraseAll(_prefix);
                    _count = 0;
                    break;
                case LazyUlongList.CommandRecord:
                {
                    var recordIndex = PackUnpack.UnpackVUInt(span, ref offset);
                    var valueLength = BinaryPrimitives.ReadUInt16LittleEndian(span[offset..]);
                    offset += sizeof(ushort);
                    var value = span.Slice(offset, valueLength);
                    offset += valueLength;
                    cursor.CreateOrUpdateKeyValue(RecordKey(recordIndex, keyBuffer), value);
                    _count = null;
                    break;
                }
                case LazyUlongList.CommandCount:
                    _count = PackUnpack.UnpackVUInt(span, ref offset);
                    StoreCount(cursor);
                    break;
                default:
                    throw new ArgumentException("Invalid ILazyUlongList command.");
            }
        }
    }

    public static void DoSave(ref MemWriter writer, IWriterCtx ctx, ILazyUlongList? list)
    {
        if (list is IInternalODBLazyUlongList goodList)
        {
            writer.WriteVUInt64(goodList.Id);
            return;
        }

        if (list != null)
            throw new BTDBException("Only BTDB ILazyUlongList instances can be saved.");
        var tr = ((IDBWriterCtx)ctx).GetTransaction();
        writer.WriteVUInt64(tr.AllocateDictionaryId());
    }

    public IEnumerator<ulong> GetEnumerator()
    {
        return EnumerateFromIndex(0).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public IEnumerable<ulong> EnumerateFromIndex(ulong index)
    {
        if (_currentRecordDirty)
            Flush();
        var recordIndex = index / LazyUlongList.ValuesPerRecord;
        return EnumerateFromRecord(recordIndex, (int)(index % LazyUlongList.ValuesPerRecord));
    }

    bool FindRecord(IKeyValueDBCursor cursor, ulong recordIndex)
    {
        Span<byte> keyBuffer = stackalloc byte[32];
        return cursor.Find(RecordKey(recordIndex, keyBuffer), (uint)_prefix.Length) == FindResult.Exact;
    }

    IEnumerable<ulong> EnumerateFromRecord(ulong recordIndex, int skip)
    {
        using var cursor = _keyValueTr.CreateCursor();
        if (!FindRecord(cursor, recordIndex))
            yield break;
        Memory<byte> valueBuffer = new byte[LazyUlongList.MaxRecordPayloadLength];
        do
        {
            foreach (var value in LazyUlongList.DecodeRecord(cursor.GetValueMemory(ref valueBuffer, copy: true)))
            {
                if (skip != 0)
                {
                    skip--;
                    continue;
                }

                yield return value;
            }
        } while (cursor.FindNextKey(_prefix));
    }
}
