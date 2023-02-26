using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer;

public class FindUnusedKeysVisitor : IDisposable
{
    IFileCollection _fileCollection;
    IKeyValueDB _keyValueDb;
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
        _keyValueDb = new KeyValueDB(_fileCollection);
    }

    IEnumerable<byte[]> SupportedKeySpaces()
    {
        yield return ObjectDB.AllObjectsPrefix;
        yield return ObjectDB.AllDictionariesPrefix;
    }

    public void ImportAllKeys(IObjectDBTransaction sourceDbTr)
    {
        var itr = (IInternalObjectDBTransaction)sourceDbTr;
        var sourceKvTr = itr.KeyValueDBTransaction;
        foreach (var prefix in SupportedKeySpaces())
            ImportKeysWithPrefix(prefix, sourceKvTr);
    }

    void ImportKeysWithPrefix(byte[] prefix, IKeyValueDBTransaction sourceKvTr)
    {
        if (!sourceKvTr.FindFirstKey(prefix))
            return;
        using (var kvtr = _keyValueDb.StartWritingTransaction().Result)
        {
            do
            {
                //create all keys, instead of value store only byte length of value
                kvtr.CreateOrUpdateKeyValue(sourceKvTr.GetKey(), Vuint2ByteBuffer(sourceKvTr.GetStorageSizeOfCurrentKey().Value));
            } while (sourceKvTr.FindNextKey(prefix));
            kvtr.Commit();
        }
    }

    public ODBIterator Iterate(IObjectDBTransaction tr)
    {
        var iterator = new ODBIterator(tr, new VisitorForFindUnused(this));
        using (_kvtr = _keyValueDb.StartWritingTransaction().Result)
        {
            iterator.Iterate();
            _kvtr.Commit();
        }
        _kvtr = null;
        return iterator;
    }

    public IEnumerable<UnseenKey> UnseenKeys()
    {
        using var trkv = _keyValueDb.StartReadOnlyTransaction();
        foreach (var prefix in SupportedKeySpaces())
        {
            if (!trkv.FindFirstKey(prefix))
                continue;
            do
            {
                yield return new UnseenKey
                {
                    Key = trkv.GetKeyToArray(),
                    ValueSize = (uint)PackUnpack.UnpackVUInt(trkv.GetValue())
                };
            } while (trkv.FindNextKey(prefix));
        }
    }

    public void DeleteUnused(IObjectDBTransaction tr)
    {
        var itr = (IInternalObjectDBTransaction)tr;
        var kvtr = itr.KeyValueDBTransaction;
        foreach (var unseen in UnseenKeys())
        {
            kvtr.EraseAll(unseen.Key);
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

    void MarkKeyAsUsed(IKeyValueDBTransaction tr)
    {
        _kvtr!.EraseCurrent(tr.GetKey());
    }

    class VisitorForFindUnused : IODBFastVisitor
    {
        readonly FindUnusedKeysVisitor _finder;

        public VisitorForFindUnused(FindUnusedKeysVisitor finder)
        {
            _finder = finder;
        }

        public void MarkCurrentKeyAsUsed(IKeyValueDBTransaction tr)
        {
            _finder.MarkKeyAsUsed(tr);
        }
    }
}
