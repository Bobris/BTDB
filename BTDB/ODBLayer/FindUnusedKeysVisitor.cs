using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer;

public class FindUnusedKeysVisitor : IDisposable
{
    IFileCollection? _fileCollection;
    IKeyValueDB? _keyValueDb;
    IKeyValueDBTransaction? _kvtr;
    readonly byte[] _tempBytes = new byte[32];

    public struct UnseenKey
    {
        public byte[] Key { get; set; }
        public uint ValueSize { get; set; }
    }

    public FindUnusedKeysVisitor(string? onDiskFileCollectionDictionary = null)
    {
        if (onDiskFileCollectionDictionary != null)
            _fileCollection = new OnDiskFileCollection(onDiskFileCollectionDictionary);
        else
            _fileCollection = new InMemoryFileCollection();
        _keyValueDb = new BTreeKeyValueDB(_fileCollection);
    }

    IEnumerable<byte[]> SupportedKeySpaces()
    {
        yield return ObjectDB.AllObjectsPrefix;
        yield return ObjectDB.AllDictionariesPrefix;
    }

    public void ImportAllKeys(IObjectDBTransaction sourceDbTr)
    {
        using var sourceCursor = sourceDbTr.KeyValueDBTransaction.CreateCursor();
        foreach (var prefix in SupportedKeySpaces())
            ImportKeysWithPrefix(prefix, sourceCursor);
    }

    void ImportKeysWithPrefix(byte[] prefix, IKeyValueDBCursor sourceCursor)
    {
        if (!sourceCursor.FindFirstKey(prefix))
            return;
        var buf = Span<byte>.Empty;
        using (var kvtr = _keyValueDb!.StartWritingTransaction().Result)
        {
            using var cursor = kvtr.CreateCursor();
            do
            {
                //create all keys, instead of value store only byte length of value
                cursor.CreateOrUpdateKeyValue(sourceCursor.GetKeySpan(ref buf),
                    Vuint2ByteBuffer(sourceCursor.GetStorageSizeOfCurrentKey().Value));
            } while (sourceCursor.FindNextKey(prefix));

            kvtr.Commit();
        }
    }

    public ODBIterator Iterate(IObjectDBTransaction tr)
    {
        var iterator = new ODBIterator(tr, new VisitorForFindUnused(this));
        using (_kvtr = _keyValueDb!.StartWritingTransaction().Result)
        {
            iterator.Iterate();
            _kvtr.Commit();
        }

        _kvtr = null;
        return iterator;
    }

    public IEnumerable<UnseenKey> UnseenKeys()
    {
        using var trkv = _keyValueDb!.StartReadOnlyTransaction();
        using var cursor = trkv.CreateCursor();
        var buf = Memory<byte>.Empty;
        foreach (var prefix in SupportedKeySpaces())
        {
            if (!cursor.FindFirstKey(prefix))
                continue;
            do
            {
                yield return new UnseenKey
                {
                    Key = cursor.SlowGetKey(),
                    ValueSize = (uint)PackUnpack.UnpackVUInt(cursor.GetValueMemory(ref buf).Span)
                };
            } while (cursor.FindNextKey(prefix));
        }
    }

    public void DeleteUnused(IObjectDBTransaction tr)
    {
        using var cursor = tr.KeyValueDBTransaction.CreateCursor();
        foreach (var unseen in UnseenKeys())
        {
            cursor.EraseAll(unseen.Key);
        }
    }

    public void Dispose()
    {
        _keyValueDb?.Dispose();
        _keyValueDb = null;
        _fileCollection?.Dispose();
        _fileCollection = null;
    }

    ReadOnlySpan<byte> Vuint2ByteBuffer(uint v)
    {
        var ofs = 0;
        PackUnpack.PackVUInt(_tempBytes, ref ofs, v);
        return _tempBytes.AsSpan(0, ofs);
    }

    [SkipLocalsInit]
    void MarkKeyAsUsed(IKeyValueDBCursor cursor)
    {
        Span<byte> buf = stackalloc byte[4096];
        using var cursorTarget = _kvtr!.CreateCursor();
        if (cursorTarget.FindExactKey(cursor.GetKeySpan(ref buf)))
        {
            cursorTarget.EraseCurrent();
        }
    }

    class VisitorForFindUnused : IODBFastVisitor
    {
        readonly FindUnusedKeysVisitor _finder;

        public VisitorForFindUnused(FindUnusedKeysVisitor finder)
        {
            _finder = finder;
        }

        public void MarkCurrentKeyAsUsed(IKeyValueDBCursor cursor)
        {
            _finder.MarkKeyAsUsed(cursor);
        }
    }
}
