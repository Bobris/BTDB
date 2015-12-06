using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public class FindUnusedKeysVisitor : IODBVisitor, IDisposable
    {
        IFileCollection _memoryFileCollection;
        IKeyValueDB _keyValueDb;
        IKeyValueDBTransaction _kvtr;
        readonly byte[] _tempBytes = new byte[32];

        public struct UnseenKey
        {
            public byte[] Key { get; set; }
            public uint ValueSize { get; set; }
        }

        public FindUnusedKeysVisitor()
        {
            _memoryFileCollection = new InMemoryFileCollection();
            _keyValueDb = new KeyValueDB(_memoryFileCollection);
        }

        IEnumerable<byte[]> SupportedKeySpaces()
        {
            yield return ObjectDB.TableNamesPrefix;
            yield return ObjectDB.TableVersionsPrefix;
            yield return ObjectDB.TableSingletonsPrefix;
            yield return ObjectDB.LastDictIdKey;
            yield return ObjectDB.AllObjectsPrefix;
            yield return ObjectDB.AllDictionariesPrefix;
        }

        public void ImportAllKeys(IObjectDBTransaction sourceDbTr)
        {
            var itr = (IInternalObjectDBTransaction)sourceDbTr;
            var trkv = itr.KeyValueDBTransaction;
            foreach (var prefix in SupportedKeySpaces())
                ImportKeysWithPrefix(prefix, trkv);
        }

        void ImportKeysWithPrefix(byte[] prefix, IKeyValueDBTransaction sourceKvTr)
        {
            sourceKvTr.SetKeyPrefixUnsafe(prefix);
            if (!sourceKvTr.FindFirstKey())
                return;
            using (var kvtr = _keyValueDb.StartWritingTransaction().Result)
            {
                do
                {
                    //create all keys instead of value store only byte length of value
                    kvtr.CreateOrUpdateKeyValue(sourceKvTr.GetKey(), Vuint2ByteBuffer((uint)kvtr.GetValue().Length));
                } while (sourceKvTr.FindNextKey());
                kvtr.Commit();
            }
        }

        public void Iterate(IObjectDBTransaction tr)
        {
            var iterator = new ODBIterator(tr, this);
            using (_kvtr = _keyValueDb.StartWritingTransaction().Result)
            {
                iterator.Iterate();
                _kvtr.Commit();
            }
            _kvtr = null;
        }

        public IEnumerable<UnseenKey> UnseenKeys()
        {
            using (var trkv = _keyValueDb.StartReadOnlyTransaction())
            {
                foreach (var prefix in SupportedKeySpaces())
                {
                    trkv.SetKeyPrefixUnsafe(prefix);
                    if (!trkv.FindFirstKey())
                        continue;
                    var reader = new KeyValueDBValueReader(trkv);
                    do
                    {
                        reader.Restart();
                        yield return new UnseenKey
                        {
                            Key = trkv.GetKey().ToByteArray(),
                            ValueSize = reader.ReadVUInt32()
                        };
                    } while (trkv.FindNextKey());
                }
            }
        }

        public void DeleteUnused(IObjectDBTransaction tr)
        {
            var itr = (IInternalObjectDBTransaction)tr;
            var kvtr = itr.KeyValueDBTransaction;
            foreach (var unseen in UnseenKeys())
            {
                kvtr.SetKeyPrefix(unseen.Key);
                kvtr.EraseAll();
            }
        }

        public void Dispose()
        {
            _keyValueDb?.Dispose();
            _keyValueDb = null;
            _memoryFileCollection?.Dispose();
            _memoryFileCollection = null;
        }

        public void MarkCurrentKeyAsUsed(IKeyValueDBTransaction tr)
        {
            _kvtr.SetKeyPrefix(tr.GetKey());            
            if (_kvtr.FindFirstKey())
                _kvtr.EraseAll();
        }

        public bool VisitSingleton(uint tableId, string tableName, ulong oid)
        {
            return true;
        }

        public bool StartObject(ulong oid, uint tableId, string tableName, uint version)
        {
            return true;
        }

        public bool StartField(string name)
        {
            return true;
        }

        public bool SimpleField(object content)
        {
            return true;
        }

        public bool EndField()
        {
            return true;
        }

        public bool VisitFieldText(string name, string content)
        {
            return true;
        }

        public void VisitOidField(string name, ulong oid)
        {
        }

        public bool StartDictionary(string name)
        {
            return true;
        }

        public bool StartDictKey()
        {
            return true;
        }

        public void EndDictKey()
        {
        }

        public bool StartDictValue()
        {
            return true;
        }

        public void EndDictionary()
        {
        }

        public void EndObject()
        {
        }

        ByteBuffer Vuint2ByteBuffer(uint v)
        {
            var ofs = 0;
            PackUnpack.PackVUInt(_tempBytes, ref ofs, v);
            return ByteBuffer.NewSync(_tempBytes, 0, ofs);
        }
    }
}
