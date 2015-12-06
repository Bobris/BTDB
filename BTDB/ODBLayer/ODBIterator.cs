using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class ODBIterator
    {
        readonly IInternalObjectDBTransaction _tr;
        readonly IODBVisitor _visitor;
        Dictionary<uint, string> _tableId2Name;
        readonly IKeyValueDBTransaction _trkv;
        Dictionary<uint, ulong> _singletons;
        readonly HashSet<uint> _usedTableIds;
        readonly byte[] _tempBytes = new byte[32];
        readonly HashSet<ulong> _visitedOids;
        readonly HashSet<TableIdVersion> _usedTableVersions;
        readonly Dictionary<TableIdVersion, TableVersionInfo> _tableVersionInfos;

        struct TableIdVersion : IEquatable<TableIdVersion>
        {
            readonly uint _tableid;
            readonly uint _version;

            public TableIdVersion(uint tableid, uint version)
            {
                _tableid = tableid;
                _version = version;
            }

            public bool Equals(TableIdVersion other)
            {
                return _tableid == other._tableid && _version == other._version;
            }

            public override int GetHashCode()
            {
                return (int)(_tableid * 33 + _version);
            }
        }
        public ODBIterator(IObjectDBTransaction tr, IODBVisitor visitor)
        {
            _tr = (IInternalObjectDBTransaction)tr;
            _trkv = _tr.KeyValueDBTransaction;
            _visitor = visitor;
            _usedTableIds = new HashSet<uint>();
            _visitedOids = new HashSet<ulong>();
            _usedTableVersions = new HashSet<TableIdVersion>();
            _tableVersionInfos = new Dictionary<TableIdVersion, TableVersionInfo>();
        }

        public void Iterate()
        {
            LoadTableNamesDict();
            MarkLastDictId();
            _trkv.SetKeyPrefixUnsafe(ObjectDB.TableSingletonsPrefix);
            var keyReader = new KeyValueDBKeyReader(_trkv);
            var valueReader = new KeyValueDBValueReader(_trkv);
            _singletons = new Dictionary<uint, ulong>();
            while (_trkv.FindNextKey())
            {
                keyReader.Restart();
                valueReader.Restart();
                _singletons.Add(keyReader.ReadVUInt32(), valueReader.ReadVUInt64());
            }
            foreach (var singleton in _singletons)
            {
                string name;
                if (
                    !_visitor.VisitSingleton(singleton.Key,
                        _tableId2Name.TryGetValue(singleton.Key, out name) ? name : null, singleton.Value))
                    continue;
                MarkTableName(singleton.Key);
                _trkv.SetKeyPrefixUnsafe(ObjectDB.TableSingletonsPrefix);
                if (_trkv.Find(Vuint2ByteBuffer(singleton.Key)) == FindResult.Exact)
                {
                    _visitor.MarkCurrentKeyAsUsed(_trkv);
                }
                IterateOid(singleton.Value);
            }
        }

        void IterateOid(ulong oid)
        {
            if (!_visitedOids.Add(oid))
                return;
            _tr.TransactionProtector.Start();
            _trkv.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
            if (_trkv.Find(Vuint2ByteBuffer(oid)) != FindResult.Exact)
                return; // Object oid was deleted
            _visitor.MarkCurrentKeyAsUsed(_trkv);
            var reader = new KeyValueDBValueReader(_trkv);
            var tableId = reader.ReadVUInt32();
            var version = reader.ReadVUInt32();
            MarkTableIdVersionFieldInfo(tableId, version);
            string tableName;
            if (!_visitor.StartObject(oid, tableId, _tableId2Name.TryGetValue(tableId, out tableName) ? tableName : null,
                version))
                return;
            var tvi = GetTableVersionInfo(tableId, version);
            for (var i = 0; i < tvi.FieldCount; i++)
            {
                var fi = tvi[i];
                if (_visitor.StartField(fi.Name))
                {
                    IterateHandler(reader, fi.Handler);
                    _visitor.EndField();
                }
            }
            _visitor.EndObject();
        }

        void IterateDict(ulong dictId, IFieldHandler keyHandler, IFieldHandler valueHandler)
        {
            if (!_visitor.StartDictionary())
                return;
            var o = ObjectDB.AllDictionariesPrefix.Length;
            var prefix = new byte[o + PackUnpack.LengthVUInt(dictId)];
            Array.Copy(ObjectDB.AllDictionariesPrefix, prefix, o);
            PackUnpack.PackVUInt(prefix, ref o, dictId);
            _trkv.SetKeyPrefix(prefix);
            var protector = _tr.TransactionProtector;
            long prevProtectionCounter = 0;
            long pos = 0;
            while (true)
            {
                protector.Start();
                if (pos == 0)
                {
                    _trkv.SetKeyPrefix(prefix);
                    if (!_trkv.FindFirstKey()) break;
                }
                else
                {
                    if (protector.WasInterupted(prevProtectionCounter))
                    {
                        _trkv.SetKeyPrefix(prefix);
                        if (!_trkv.SetKeyIndex(pos)) break;
                    }
                    else
                    {
                        if (!_trkv.FindNextKey()) break;
                    }
                }
                _visitor.MarkCurrentKeyAsUsed(_trkv);
                prevProtectionCounter = protector.ProtectionCounter;
                if (_visitor.StartDictKey())
                {
                    var keyReader = new KeyValueDBKeyReader(_trkv);
                    IterateHandler(keyReader, keyHandler);
                    _visitor.EndDictKey();
                }
                if (protector.WasInterupted(prevProtectionCounter))
                {
                    _trkv.SetKeyPrefix(prefix);
                    if (!_trkv.SetKeyIndex(pos)) break;
                }
                if (_visitor.StartDictValue())
                {
                    var valueReader = new KeyValueDBValueReader(_trkv);
                    IterateHandler(valueReader, valueHandler);
                    _visitor.EndDictValue();
                }
                pos++;
            }
            _visitor.EndDictionary();
        }

        void IterateHandler(AbstractBufferedReader reader, IFieldHandler handler)
        {
            if (handler is ODBDictionaryFieldHandler)
            {
                var dictId = reader.ReadVUInt64();
                var kvHandlers = ((IFieldHandlerWithNestedFieldHandlers)handler).EnumerateNestedFieldHandlers().ToArray();
                IterateDict(dictId, kvHandlers[0], kvHandlers[1]);
            }
            else if (handler is DBObjectFieldHandler)
            {
                var oid = reader.ReadVInt64();
                if (oid == 0)
                {
                    _visitor.OidReference(0);
                }
                else if (oid <= int.MinValue || oid > 0)
                {
                    _visitor.OidReference((ulong)oid);
                    IterateOid((ulong)oid);
                }
                else
                {
                    var tableId = reader.ReadVUInt32();
                    var version = reader.ReadVUInt32();
                    MarkTableIdVersionFieldInfo(tableId, version);
                    string tableName;
                    var skip =
                        !_visitor.StartInlineObject(tableId,
                            _tableId2Name.TryGetValue(tableId, out tableName) ? tableName : null, version);
                    var tvi = GetTableVersionInfo(tableId, version);
                    for (var i = 0; i < tvi.FieldCount; i++)
                    {
                        var fi = tvi[i];
                        var skipField = skip || !_visitor.StartField(fi.Name);
                        IterateHandler(reader, fi.Handler);
                        if (!skipField) _visitor.EndField();
                    }
                    if (!skip) _visitor.EndInlineObject();
                }
            }
            else if (handler is ListFieldHandler)
            {
                var oid = reader.ReadVInt64();
                var itemHandler = ((IFieldHandlerWithNestedFieldHandlers)handler).EnumerateNestedFieldHandlers().First();
                if (oid == 0)
                {
                    _visitor.OidReference(0);
                }
                else if (oid <= int.MinValue || oid > 0)
                {
                    _visitor.OidReference((ulong)oid);
                    IterateOid((ulong)oid);
                }
                else
                {
                    IterateInlineList(reader, itemHandler);
                }
            }
            else if (handler.NeedsCtx() || handler.HandledType() == null)
            {
                throw new BTDBException("Don't know how to iterate " + handler.Name);
            }
            else
            {
                var meth =
                    ILBuilder.Instance.NewMethod<Func<AbstractBufferedReader, object>>("Load" + handler.Name);
                var il = meth.Generator;
                handler.Load(il, il2 => il2.Ldarg(0));
                il.Box(handler.HandledType()).Ret();
                var obj = meth.Create()(reader);
                if (_visitor.NeedScalarAsObject())
                {
                    _visitor.ScalarAsObject(obj);
                }
                if (_visitor.NeedScalarAsText())
                {
                    _visitor.ScalarAsText(obj?.ToString() ?? "null");
                }
            }
        }

        void IterateInlineList(AbstractBufferedReader reader, IFieldHandler itemHandler)
        {
            var skip = !_visitor.StartList();
            var count = reader.ReadVUInt32();
            while (count-- > 0)
            {
                var skipItem = skip || !_visitor.StartItem();
                IterateHandler(reader, itemHandler); // TODO pass skipItem
                if (!skipItem) _visitor.EndItem();
            }
            if (!skip) _visitor.EndList();
        }

        void MarkTableIdVersionFieldInfo(uint tableId, uint version)
        {
            if (!_usedTableVersions.Add(new TableIdVersion(tableId, version)))
                return;
            MarkTableName(tableId);
            _tr.TransactionProtector.Start();
            _trkv.SetKeyPrefixUnsafe(ObjectDB.TableVersionsPrefix);
            if (_trkv.Find(TwiceVuint2ByteBuffer(tableId, version)) == FindResult.Exact)
            {
                _visitor.MarkCurrentKeyAsUsed(_trkv);
            }
        }

        void MarkLastDictId()
        {
            _trkv.SetKeyPrefixUnsafe(null);
            if (_trkv.FindExactKey(ObjectDB.LastDictIdKey))
            {
                _visitor.MarkCurrentKeyAsUsed(_trkv);
            }
        }

        void MarkTableName(uint tableId)
        {
            if (!_usedTableIds.Add(tableId))
                return;
            _tr.TransactionProtector.Start();
            _trkv.SetKeyPrefixUnsafe(ObjectDB.TableNamesPrefix);
            if (_trkv.Find(Vuint2ByteBuffer(tableId)) == FindResult.Exact)
            {
                _visitor.MarkCurrentKeyAsUsed(_trkv);
            }
        }

        ByteBuffer Vuint2ByteBuffer(uint v)
        {
            var ofs = 0;
            PackUnpack.PackVUInt(_tempBytes, ref ofs, v);
            return ByteBuffer.NewSync(_tempBytes, 0, ofs);
        }

        ByteBuffer Vuint2ByteBuffer(ulong v)
        {
            var ofs = 0;
            PackUnpack.PackVUInt(_tempBytes, ref ofs, v);
            return ByteBuffer.NewSync(_tempBytes, 0, ofs);
        }

        ByteBuffer TwiceVuint2ByteBuffer(uint v1, uint v2)
        {
            var ofs = 0;
            PackUnpack.PackVUInt(_tempBytes, ref ofs, v1);
            PackUnpack.PackVUInt(_tempBytes, ref ofs, v2);
            return ByteBuffer.NewSync(_tempBytes, 0, ofs);
        }

        void LoadTableNamesDict()
        {
            _tableId2Name = ObjectDB.LoadTablesEnum(_tr.KeyValueDBTransaction).ToDictionary(pair => pair.Key, pair => pair.Value);
        }

        TableVersionInfo GetTableVersionInfo(uint tableId, uint version)
        {
            TableVersionInfo res;
            if (_tableVersionInfos.TryGetValue(new TableIdVersion(tableId, version), out res))
                return res;
            _trkv.SetKeyPrefixUnsafe(ObjectDB.TableVersionsPrefix);
            if (_trkv.Find(TwiceVuint2ByteBuffer(tableId, version)) == FindResult.Exact)
            {
                var reader = new KeyValueDBValueReader(_trkv);
                res = TableVersionInfo.Load(reader, _tr.Owner.FieldHandlerFactory, _tableId2Name[tableId]);
                _tableVersionInfos.Add(new TableIdVersion(tableId, version), res);
                return res;
            }
            throw new ArgumentException($"TableVersionInfo not found {tableId}-{version}");
        }
    }
}
