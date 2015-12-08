using System;
using System.Collections.Generic;
using System.Text;
using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public class FindUnusedKeysVisitor : IDisposable
    {
        IFileCollection _memoryFileCollection;
        IKeyValueDB _keyValueDb;
        IKeyValueDBTransaction _kvtr;
        readonly byte[] _tempBytes = new byte[32];
        readonly byte[] _tempKeyBytes = new byte[32];

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
            var sourceKvTr = itr.KeyValueDBTransaction;
            foreach (var prefix in SupportedKeySpaces())
                ImportKeysWithPrefix(prefix, sourceKvTr);
        }

        void ImportKeysWithPrefix(byte[] prefix, IKeyValueDBTransaction sourceKvTr)
        {
            sourceKvTr.SetKeyPrefix(prefix);
            if (!sourceKvTr.FindFirstKey())
                return;
            using (var kvtr = _keyValueDb.StartWritingTransaction().Result)
            {
                kvtr.SetKeyPrefix(prefix);
                do
                {
                    //create all keys, instead of value store only byte length of value
                    kvtr.CreateOrUpdateKeyValue(sourceKvTr.GetKey(), Vuint2ByteBuffer(kvtr.GetStorageSizeOfCurrentKey().Value));
                } while (sourceKvTr.FindNextKey());
                kvtr.Commit();
            }
        }

        public void Iterate(IObjectDBTransaction tr)
        {
            var iterator = new ODBIterator(tr, new VisitorForFindUnused(this));
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
                            Key = Merge(prefix, trkv.GetKeyAsByteArray()),
                            ValueSize = reader.ReadVUInt32()
                        };
                    } while (trkv.FindNextKey());
                }
            }
        }

        byte[] Merge(byte[] prefix, byte[] suffix)
        {
            var result = new byte[prefix.Length + suffix.Length];
            System.Buffer.BlockCopy(prefix, 0, result, 0, prefix.Length);
            System.Buffer.BlockCopy(suffix, 0, result, prefix.Length, suffix.Length);
            return result;
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

        ByteBuffer Vuint2ByteBuffer(uint v)
        {
            var ofs = 0;
            PackUnpack.PackVUInt(_tempBytes, ref ofs, v);
            return ByteBuffer.NewSync(_tempBytes, 0, ofs);
        }

        void MarkKeyAsUsed(IKeyValueDBTransaction tr)
        {
            _kvtr.SetKeyPrefix(Merge(tr.GetKeyPrefix(), tr.GetKeyAsByteArray()));
            _kvtr.EraseAll();
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
}
