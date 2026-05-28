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

interface IInternalODBRoaringBitmap
{
    ulong Id { get; }
    IInternalODBRoaringBitmapPageReader GetPageReader();
}

interface IInternalODBRoaringBitmapPageReader : IDisposable
{
    ulong? ReadNext();
    Memory<byte> ReadValue(Memory<byte> content);
}

public class ODBRoaringBitmap : IRoaringBitmap, IInternalODBRoaringBitmap, IAmLazyDBObject
{
    readonly IInternalObjectDBTransaction _tr;
    readonly IKeyValueDBTransaction _keyValueTr;
    readonly ulong _id;
    readonly byte[] _prefix;
    ulong? _count;
    ulong _currentPageIndex;
    byte[]? _currentPage;
    byte[]? _currentEncodedPage;
    bool _currentPageDirty;
    const byte CommandClear = 0;
    const byte CommandPage = 1;
    const byte CommandCount = 2;

    public ODBRoaringBitmap(IInternalObjectDBTransaction tr, ulong id)
    {
        _tr = tr;
        _keyValueTr = _tr.KeyValueDBTransaction;
        _id = id;
        var len = PackUnpack.LengthVUInt(id);
        _prefix = new byte[ObjectDB.AllDictionariesPrefixLen + len];
        MemoryMarshal.GetReference(_prefix.AsSpan()) = ObjectDB.AllDictionariesPrefixByte;
        PackUnpack.UnsafePackVUInt(
            ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(_prefix.AsSpan()),
                ObjectDB.AllDictionariesPrefixLen), id, len);
    }

    public ODBRoaringBitmap(IInternalObjectDBTransaction tr) : this(tr, tr.AllocateDictionaryId())
    {
    }

    public ulong Id => _id;

    public bool Get(ulong value)
    {
        var pageIndex = value >> 16;
        if (_currentPage != null && _currentPageIndex == pageIndex)
            return RoaringBitmaps.GetBit(_currentPage, (ushort)value);
        return RoaringBitmaps.Contains(LoadEncodedPage(pageIndex), (ushort)value);
    }

    public void Set(ulong value, bool enabled)
    {
        var pageIndex = value >> 16;
        LoadPage(pageIndex);
        var offset = (ushort)value;
        var old = RoaringBitmaps.GetBit(_currentPage!, offset);
        if (old == enabled)
            return;
        if (enabled)
        {
            RoaringBitmaps.SetBit(_currentPage!, offset);
            _count = Count + 1;
        }
        else
        {
            RoaringBitmaps.UnsetBit(_currentPage!, offset);
            _count = Count - 1;
        }

        _currentPageDirty = true;
    }

    public void Clear()
    {
        using var cursor = _keyValueTr.CreateCursor();
        cursor.EraseAll(_prefix);
        _count = 0;
        _currentPage = null;
        _currentEncodedPage = null;
        _currentPageDirty = false;
    }

    public ulong Count
    {
        get
        {
            if (_count.HasValue)
                return _count.Value;
            using var cursor = _keyValueTr.CreateCursor();
            Span<byte> buffer = stackalloc byte[16];
            if (cursor.FindExactKey(_prefix))
            {
                _count = PackUnpack.UnpackVUInt(cursor.GetValueSpan(ref buffer));
            }
            else
            {
                _count = 0;
            }

            return _count.Value;
        }
    }

    public bool IsComplete()
    {
        using var cursor = _keyValueTr.CreateCursor();
        if (cursor.FindExactKey(_prefix))
            return true;
        return !cursor.FindNextKey(_prefix);
    }

    public void Flush()
    {
        using var cursor = _keyValueTr.CreateCursor();
        StoreCurrentDirtyPage(cursor, stackalloc byte[16]);
        if (!_count.HasValue && !IsComplete())
            _count = CalculateCount();
        StoreCount(cursor);
    }

    bool StoreCurrentDirtyPage(IKeyValueDBCursor cursor, Span<byte> keyBuffer)
    {
        if (!_currentPageDirty || _currentPage == null)
            return false;
        Span<byte> compressed = stackalloc byte[RoaringBitmaps.BitmapSize];
        var key = PageKey(_currentPageIndex, keyBuffer);
        var len = RoaringBitmaps.Compress(_currentPage, compressed);
        if (len == 0)
        {
            if (cursor.FindExactKey(key))
                cursor.EraseCurrent();
        }
        else
        {
            cursor.CreateOrUpdateKeyValue(key, compressed[..len]);
        }

        _currentPageDirty = false;
        _currentEncodedPage = null;
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
        }
        else
        {
            Span<byte> value = stackalloc byte[16];
            var len = PackUnpack.LengthVUInt(_count.Value);
            PackUnpack.UnsafePackVUInt(ref MemoryMarshal.GetReference(value), _count.Value, len);
            cursor.CreateOrUpdateKeyValue(_prefix, value[..(int)len]);
        }
    }

    void LoadPage(ulong pageIndex)
    {
        if (_currentPage != null && _currentPageIndex == pageIndex)
            return;
        var currentEncodedPage = _currentEncodedPage != null && _currentPageIndex == pageIndex
            ? _currentEncodedPage
            : null;
        using var cursor = _keyValueTr.CreateCursor();
        Span<byte> keyBuffer = stackalloc byte[16];
        if (StoreCurrentDirtyPage(cursor, keyBuffer))
            StoreCount(cursor);

        _currentPageIndex = pageIndex;
        _currentPage = new byte[RoaringBitmaps.BitmapSize];
        _currentEncodedPage = null;
        if (currentEncodedPage != null)
        {
            RoaringBitmaps.ToBitmap(currentEncodedPage, _currentPage);
            return;
        }

        var key = PageKey(pageIndex, keyBuffer);
        if (!cursor.FindExactKey(key))
            return;
        Span<byte> buffer = stackalloc byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.ToBitmap(cursor.GetValueSpan(ref buffer), _currentPage);
    }

    ReadOnlySpan<byte> LoadEncodedPage(ulong pageIndex)
    {
        if (_currentEncodedPage != null && _currentPageIndex == pageIndex)
            return _currentEncodedPage;
        using var cursor = _keyValueTr.CreateCursor();
        Span<byte> keyBuffer = stackalloc byte[16];
        if (StoreCurrentDirtyPage(cursor, keyBuffer))
            StoreCount(cursor);

        _currentPageIndex = pageIndex;
        _currentPage = null;
        var key = PageKey(pageIndex, keyBuffer);
        if (!cursor.FindExactKey(key))
        {
            _currentEncodedPage = [];
            return _currentEncodedPage;
        }

        Span<byte> buffer = stackalloc byte[RoaringBitmaps.BitmapSize];
        _currentEncodedPage = cursor.GetValueSpan(ref buffer).ToArray();
        return _currentEncodedPage;
    }

    ReadOnlySpan<byte> PageKey(ulong pageIndex, Span<byte> keyBuffer)
    {
        var pageLen = PackUnpack.LengthVUInt(pageIndex);
        var key = keyBuffer[..(_prefix.Length + (int)pageLen)];
        _prefix.CopyTo(key);
        PackUnpack.UnsafePackVUInt(ref key[_prefix.Length], pageIndex, pageLen);
        return key;
    }

    public void ApplyCommands(ReadOnlyMemory<byte> commands)
    {
        if (_currentPageDirty)
            throw new InvalidOperationException(
                "Cannot apply roaring bitmap commands while bitmap has unflushed changes.");
        _currentPage = null;
        _currentEncodedPage = null;

        using var cursor = _keyValueTr.CreateCursor();
        var span = commands.Span;
        var offset = 0;
        Span<byte> keyBuffer = stackalloc byte[16];
        while (offset < span.Length)
        {
            var command = span[offset++];
            switch (command)
            {
                case CommandClear:
                    cursor.EraseAll(_prefix);
                    _count = 0;
                    break;
                case CommandPage:
                {
                    var pageIndex = PackUnpack.UnpackVUInt(span, ref offset);
                    offset = AlignToUshort(offset);
                    var valueLength = BinaryPrimitives.ReadUInt16LittleEndian(span[offset..]);
                    offset += sizeof(ushort);
                    var value = span.Slice(offset, valueLength);
                    offset += valueLength;
                    var key = PageKey(pageIndex, keyBuffer);
                    if (valueLength == 0)
                    {
                        if (cursor.FindExactKey(key))
                            cursor.EraseCurrent();
                    }
                    else
                    {
                        cursor.CreateOrUpdateKeyValue(key, value);
                    }

                    break;
                }
                case CommandCount:
                {
                    _count = PackUnpack.UnpackVUInt(span, ref offset);
                    StoreCount(cursor);
                    break;
                }
                default:
                    throw new ArgumentException("Invalid IRoaringBitmap command.");
            }
        }
    }

    static int AlignToUshort(int offset)
    {
        return (offset + 1) & ~1;
    }

    public static void DoSave(ref MemWriter writer, IWriterCtx ctx, IRoaringBitmap? bitmap)
    {
        if (bitmap is IInternalODBRoaringBitmap goodBitmap)
        {
            writer.WriteVUInt64(goodBitmap.Id);
            return;
        }

        if (bitmap != null)
            throw new BTDBException("Only BTDB IRoaringBitmap instances can be saved.");
        var tr = ((IDBWriterCtx)ctx).GetTransaction();
        writer.WriteVUInt64(tr.AllocateDictionaryId());
    }

    IInternalODBRoaringBitmapPageReader IInternalODBRoaringBitmap.GetPageReader()
    {
        if (_currentPageDirty)
            Flush();
        return new PageReader(_keyValueTr.CreateCursor(), _prefix);
    }

    sealed class PageReader : IInternalODBRoaringBitmapPageReader
    {
        readonly IKeyValueDBCursor _cursor;
        readonly byte[] _prefix;
        Memory<byte> _keyBuffer = new byte[16];

        public PageReader(IKeyValueDBCursor cursor, byte[] prefix)
        {
            _cursor = cursor;
            _prefix = prefix;
        }

        public ulong? ReadNext()
        {
            while (_cursor.FindNextKey(_prefix))
            {
                var keySpan = _cursor.GetKeyMemory(ref _keyBuffer).Span;
                if (keySpan.Length == _prefix.Length)
                    continue;
                return PackUnpack.UnpackVUInt(keySpan[_prefix.Length..]);
            }

            return null;
        }

        public Memory<byte> ReadValue(Memory<byte> content)
        {
            var value = _cursor.GetValueMemory(ref content, copy: true);
            return content[..value.Length];
        }

        public void Dispose()
        {
            _cursor.Dispose();
        }
    }

    ulong CalculateCount()
    {
        var result = 0ul;
        using var cursor = _keyValueTr.CreateCursor();
        Memory<byte> valueBuffer = new byte[RoaringBitmaps.BitmapSize];
        Span<byte> keyBuffer = stackalloc byte[16];
        while (cursor.FindNextKey(_prefix))
        {
            if (cursor.GetKeySpan(ref keyBuffer).Length == _prefix.Length)
                continue;
            foreach (var _ in RoaringBitmaps.Enumerate(cursor.GetValueMemory(ref valueBuffer), 0))
                result++;
        }

        return result;
    }

    public RoaringBitmapEnumerator GetEnumerator()
    {
        if (_currentPageDirty)
            Flush();
        return new(_keyValueTr.CreateCursor(), _prefix);
    }

    IEnumerator<ulong> IEnumerable<ulong>.GetEnumerator()
    {
        return GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public sealed class RoaringBitmapEnumerator : IEnumerator<ulong>
    {
        readonly IKeyValueDBCursor _cursor;
        readonly byte[] _prefix;
        readonly byte[] _valueBuffer;
        RoaringBitmaps.Enumerator _pageEnumerator;

        internal RoaringBitmapEnumerator(IKeyValueDBCursor cursor, byte[] prefix)
        {
            _cursor = cursor;
            _prefix = prefix;
            _valueBuffer = new byte[RoaringBitmaps.BitmapSize];
        }

        public bool MoveNext()
        {
            Span<byte> keyBuffer = stackalloc byte[16];
            while (!_pageEnumerator.MoveNext())
            {
                if (!_cursor.FindNextKey(_prefix))
                    return false;
                var keySpan = _cursor.GetKeySpan(ref keyBuffer);
                if (keySpan.Length == _prefix.Length)
                    continue;
                var pageIndex = PackUnpack.UnpackVUInt(keySpan[_prefix.Length..]);
                Memory<byte> valueBuffer = _valueBuffer;
                _pageEnumerator = RoaringBitmaps.Enumerate(_cursor.GetValueMemory(ref valueBuffer),
                    pageIndex << 16).GetEnumerator();
            }

            Current = _pageEnumerator.Current;
            return true;
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        public ulong Current { get; private set; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            _cursor.Dispose();
        }
    }
}
