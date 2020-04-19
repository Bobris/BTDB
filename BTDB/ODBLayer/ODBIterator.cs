using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class ODBIterator
    {
        readonly IInternalObjectDBTransaction _tr;
        IODBFastVisitor _fastVisitor;
        IODBVisitor? _visitor;
        Dictionary<uint, string> _tableId2Name;
        readonly IKeyValueDBTransaction _trkv;
        Dictionary<uint, ulong> _singletons;
        readonly HashSet<uint> _usedTableIds;
        readonly byte[] _tempBytes = new byte[32];
        readonly HashSet<ulong> _visitedOids;
        readonly HashSet<TableIdVersionId> _usedTableVersions;
        readonly Dictionary<TableIdVersionId, TableVersionInfo> _tableVersionInfos;
        readonly Dictionary<IFieldHandler, Action<AbstractBufferedReader>> _skippers;
        readonly Dictionary<IFieldHandler, Func<AbstractBufferedReader, object>> _loaders;
        //relations
        Dictionary<uint, ODBIteratorRelationInfo> _relationId2Info;

        public IDictionary<uint, string> TableId2Name { get => _tableId2Name; }
        public IReadOnlyDictionary<uint, ulong> TableId2SingletonOid { get => _singletons; }
        public IReadOnlyDictionary<uint, ODBIteratorRelationInfo> RelationId2Info { get => _relationId2Info; }
        public IReadOnlyDictionary<TableIdVersionId, TableVersionInfo> TableVersionInfos { get => _tableVersionInfos; }
        public bool SkipAlreadyVisitedOidChecks;

        public ODBIterator(IObjectDBTransaction tr, IODBFastVisitor visitor)
        {
            _tr = (IInternalObjectDBTransaction)tr;
            _trkv = _tr.KeyValueDBTransaction;
            _fastVisitor = visitor;
            _visitor = visitor as IODBVisitor;
            _usedTableIds = new HashSet<uint>();
            _visitedOids = new HashSet<ulong>();
            _usedTableVersions = new HashSet<TableIdVersionId>();
            _tableVersionInfos = new Dictionary<TableIdVersionId, TableVersionInfo>();

            _skippers = new Dictionary<IFieldHandler, Action<AbstractBufferedReader>>(ReferenceEqualityComparer<IFieldHandler>.Instance);
            _loaders = new Dictionary<IFieldHandler, Func<AbstractBufferedReader, object>>(ReferenceEqualityComparer<IFieldHandler>.Instance);
        }

        public void LoadGlobalInfo(bool sortTableByNameAsc = false)
        {
            LoadTableNamesDict();
            LoadRelationInfoDict();
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

            if (sortTableByNameAsc)
            {
                _singletons = _singletons.OrderBy(item =>
                {
                    return _tableId2Name.TryGetValue(item.Key, out var name) ? name : string.Empty;
                }).ToDictionary(item => item.Key, item => item.Value);
            }
        }

        public void Iterate(bool sortTableByNameAsc = false)
        {
            LoadGlobalInfo(sortTableByNameAsc);
            foreach (var singleton in _singletons)
            {
                if (_visitor != null &&
                    !_visitor.VisitSingleton(singleton.Key,
                        _tableId2Name.TryGetValue(singleton.Key, out var name) ? name : null, singleton.Value))
                    continue;
                MarkTableName(singleton.Key);
                _trkv.SetKeyPrefixUnsafe(ObjectDB.TableSingletonsPrefix);
                if (_trkv.Find(Vuint2ByteBuffer(singleton.Key)) == FindResult.Exact)
                {
                    _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
                }
                IterateOid(singleton.Value);
            }
            foreach (var relation in _relationId2Info)
            {
                if (_visitor != null && !_visitor.StartRelation(relation.Value.Name))
                    continue;
                MarkRelationName(relation.Key);
                IterateRelation(relation.Value);
                _visitor?.EndRelation();
            }
        }

        public void IterateUnseenOid(ulong oid, IODBFastVisitor visitor)
        {
            var visitorBackup = _visitor;
            var fastVisitorBackup = _fastVisitor;
            try
            {
                _visitor = visitor as IODBVisitor;
                _fastVisitor = visitor;
                IterateOid(oid);
            }
            finally
            {
                _visitor = visitorBackup;
                _fastVisitor = fastVisitorBackup;
            }
        }

        public void IterateOid(ulong oid)
        {
            if (!SkipAlreadyVisitedOidChecks && !_visitedOids.Add(oid))
                return;
            _tr.TransactionProtector.Start();
            _trkv.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
            if (_trkv.Find(Vuint2ByteBuffer(oid)) != FindResult.Exact)
                return; // Object oid was deleted
            _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
            var reader = new KeyValueDBValueReader(_trkv);
            var tableId = reader.ReadVUInt32();
            var version = reader.ReadVUInt32();
            MarkTableIdVersionFieldInfo(tableId, version);
            string tableName;
            if (_visitor != null && !_visitor.StartObject(oid, tableId, _tableId2Name.TryGetValue(tableId, out tableName) ? tableName : null,
                version))
                return;
            var tvi = GetTableVersionInfo(tableId, version);
            var knownInlineId = new HashSet<int>();
            for (var i = 0; i < tvi.FieldCount; i++)
            {
                var fi = tvi[i];
                if (_visitor == null || _visitor.StartField(fi.Name))
                {
                    IterateHandler(reader, fi.Handler, false, knownInlineId);
                    _visitor?.EndField();
                }
                else
                {
                    IterateHandler(reader, fi.Handler, true, knownInlineId);
                }
            }
            _visitor?.EndObject();
        }

        public void IterateRelationRow(ODBIteratorRelationInfo relation, long pos)
        {
            var prefix = BuildRelationPrefix(relation.Id);
            var protector = _tr.TransactionProtector;
            protector.Start();
            _trkv.SetKeyPrefix(prefix);
            if (!_trkv.SetKeyIndex(pos)) return;
            long prevProtectionCounter = protector.ProtectionCounter;
            if (_visitor == null || _visitor.StartRelationKey())
            {
                var keyReader = new KeyValueDBKeyReader(_trkv);
                var relationInfo = relation.VersionInfos[relation.LastPersistedVersion];
                IterateFields(keyReader, relationInfo.GetPrimaryKeyFields(), null);
                _visitor?.EndRelationKey();
            }
            if (protector.WasInterupted(prevProtectionCounter))
            {
                _trkv.SetKeyPrefix(prefix);
                if (!_trkv.SetKeyIndex(pos)) return;
            }
            if (_visitor == null || _visitor.StartRelationValue())
            {
                var valueReader = new KeyValueDBValueReader(_trkv);
                var version = valueReader.ReadVUInt32();
                var relationInfo = relation.VersionInfos[version];
                IterateFields(valueReader, relationInfo.GetValueFields(), new HashSet<int>());
                _visitor?.EndRelationValue();
            }
        }

        public void IterateRelation(ODBIteratorRelationInfo relation)
        {
            var prefix = BuildRelationPrefix(relation.Id);

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
                _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
                prevProtectionCounter = protector.ProtectionCounter;
                if (_visitor == null || _visitor.StartRelationKey())
                {
                    var keyReader = new KeyValueDBKeyReader(_trkv);
                    var relationInfo = relation.VersionInfos[relation.LastPersistedVersion];
                    IterateFields(keyReader, relationInfo.GetPrimaryKeyFields(), null);
                    _visitor?.EndRelationKey();
                }
                if (protector.WasInterupted(prevProtectionCounter))
                {
                    _trkv.SetKeyPrefix(prefix);
                    if (!_trkv.SetKeyIndex(pos)) break;
                }
                if (_visitor == null || _visitor.StartRelationValue())
                {
                    var valueReader = new KeyValueDBValueReader(_trkv);
                    var version = valueReader.ReadVUInt32();
                    var relationInfo = relation.VersionInfos[version];
                    IterateFields(valueReader, relationInfo.GetValueFields(), new HashSet<int>());
                    _visitor?.EndRelationValue();
                }
                pos++;
            }
        }

        static byte[] BuildRelationPrefix(uint relationIndex)
        {
            var o = ObjectDB.AllRelationsPKPrefix.Length;
            var prefix = new byte[o + PackUnpack.LengthVUInt(relationIndex)];
            Array.Copy(ObjectDB.AllRelationsPKPrefix, prefix, o);
            PackUnpack.PackVUInt(prefix, ref o, relationIndex);
            return prefix;
        }

        void IterateFields(ByteBufferReader reader, IEnumerable<TableFieldInfo> fields, HashSet<int> knownInlineRefs)
        {
            foreach (var fi in fields)
            {
                if (_visitor == null || _visitor.StartField(fi.Name))
                {
                    IterateHandler(reader, fi.Handler, false, knownInlineRefs);
                    _visitor?.EndField();
                }
                else
                {
                    IterateHandler(reader, fi.Handler, true, knownInlineRefs);
                }
            }
        }

        uint ReadRelationVersions(uint relationIndex, string name, Dictionary<uint, RelationVersionInfo> relationVersions)
        {
            uint lastPersistedVersion = 0;
            var relationInfoResolver = new RelationInfoResolver((ObjectDB)_tr.Owner);

            var writer = new ByteBufferWriter();
            writer.WriteByteArrayRaw(ObjectDB.RelationVersionsPrefix);
            writer.WriteVUInt32(relationIndex);
            _trkv.SetKeyPrefix(writer.Data);
            if (!_trkv.FindFirstKey())
            {
                return lastPersistedVersion;
            }
            var keyReader = new KeyValueDBKeyReader(_trkv);
            var valueReader = new KeyValueDBValueReader(_trkv);
            do
            {
                keyReader.Restart();
                valueReader.Restart();
                lastPersistedVersion = keyReader.ReadVUInt32();
                var relationVersionInfo = RelationVersionInfo.LoadUnresolved(valueReader, name);
                relationVersionInfo.ResolveFieldHandlers(relationInfoResolver.FieldHandlerFactory);
                relationVersions[lastPersistedVersion] = relationVersionInfo;
            } while (_trkv.FindNextKey());

            return lastPersistedVersion;
        }

        void IterateDict(ulong dictId, IFieldHandler keyHandler, IFieldHandler valueHandler)
        {
            if (_visitor != null && !_visitor.StartDictionary())
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
                _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
                prevProtectionCounter = protector.ProtectionCounter;
                if (_visitor == null || _visitor.StartDictKey())
                {
                    var keyReader = new KeyValueDBKeyReader(_trkv);
                    IterateHandler(keyReader, keyHandler, false, null);
                    _visitor?.EndDictKey();
                }
                if (protector.WasInterupted(prevProtectionCounter))
                {
                    _trkv.SetKeyPrefix(prefix);
                    if (!_trkv.SetKeyIndex(pos)) break;
                }
                if (_visitor == null || _visitor.StartDictValue())
                {
                    var valueReader = new KeyValueDBValueReader(_trkv);
                    IterateHandler(valueReader, valueHandler, false, null);
                    _visitor?.EndDictValue();
                }
                pos++;
            }
            _visitor?.EndDictionary();
        }

        void IterateSet(ulong dictId, IFieldHandler keyHandler)
        {
            if (_visitor != null && !_visitor.StartSet())
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
                _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
                prevProtectionCounter = protector.ProtectionCounter;
                if (_visitor == null || _visitor.StartSetKey())
                {
                    var keyReader = new KeyValueDBKeyReader(_trkv);
                    IterateHandler(keyReader, keyHandler, false, null);
                    _visitor?.EndSetKey();
                }
                pos++;
            }
            _visitor?.EndSet();
        }

        void IterateHandler(AbstractBufferedReader reader, IFieldHandler handler, bool skipping, HashSet<int>? knownInlineRefs)
        {
            if (handler is ODBDictionaryFieldHandler)
            {
                var dictId = reader.ReadVUInt64();
                if (!skipping)
                {
                    var kvHandlers = ((IFieldHandlerWithNestedFieldHandlers)handler).EnumerateNestedFieldHandlers().ToArray();
                    IterateDict(dictId, kvHandlers[0], kvHandlers[1]);
                }
            }
            else if (handler is ODBSetFieldHandler)
            {
                var dictId = reader.ReadVUInt64();
                if (!skipping)
                {
                    var keyHandler = ((IFieldHandlerWithNestedFieldHandlers)handler).EnumerateNestedFieldHandlers().First();
                    IterateSet(dictId, keyHandler);
                }
            }
            else if (handler is DBObjectFieldHandler)
            {
                var oid = reader.ReadVInt64();
                if (oid == 0)
                {
                    if (!skipping) _visitor?.OidReference(0);
                }
                else if (oid <= int.MinValue || oid > 0)
                {
                    if (!skipping)
                    {
                        _visitor?.OidReference((ulong)oid);
                        IterateOid((ulong)oid);
                    }
                }
                else
                {
                    if (knownInlineRefs != null)
                    {
                        if (knownInlineRefs.Contains((int) oid))
                        {
                            if (!skipping) _visitor?.InlineBackRef((int)oid);
                            return;
                        }
                        if (!skipping) _visitor?.InlineRef((int)oid);
                        knownInlineRefs.Add((int) oid);
                    }
                    var tableId = reader.ReadVUInt32();
                    var version = reader.ReadVUInt32();
                    if (!skipping) MarkTableIdVersionFieldInfo(tableId, version);
                    string tableName;
                    var skip = skipping ||
                        _visitor != null && !_visitor.StartInlineObject(tableId,
                            _tableId2Name.TryGetValue(tableId, out tableName) ? tableName : null, version);
                    var tvi = GetTableVersionInfo(tableId, version);
                    var knownInlineRefsNested = new HashSet<int>();
                    for (var i = 0; i < tvi.FieldCount; i++)
                    {
                        var fi = tvi[i];
                        var skipField = skip || _visitor != null && !_visitor.StartField(fi.Name);
                        IterateHandler(reader, fi.Handler, skipField, knownInlineRefsNested);
                        if (!skipField) _visitor?.EndField();
                    }
                    if (!skip) _visitor?.EndInlineObject();
                }
            }
            else if (handler is ListFieldHandler)
            {
                var oid = reader.ReadVInt64();
                if (oid == 0)
                {
                    if (!skipping) _visitor?.OidReference(0);
                }
                else if (oid <= int.MinValue || oid > 0)
                {
                    if (!skipping)
                    {
                        _visitor?.OidReference((ulong)oid);
                        IterateOid((ulong)oid);
                    }
                }
                else
                {
                    var itemHandler = ((IFieldHandlerWithNestedFieldHandlers)handler).EnumerateNestedFieldHandlers().First();
                    IterateInlineList(reader, itemHandler, skipping, knownInlineRefs);
                }
            }
            else if (handler is DictionaryFieldHandler)
            {
                var oid = reader.ReadVInt64();
                if (oid == 0)
                {
                    if (!skipping) _visitor?.OidReference(0);
                }
                else if (oid <= int.MinValue || oid > 0)
                {
                    if (!skipping)
                    {
                        _visitor?.OidReference((ulong)oid);
                        IterateOid((ulong)oid);
                    }
                }
                else
                {
                    var kvHandlers = ((IFieldHandlerWithNestedFieldHandlers)handler).EnumerateNestedFieldHandlers().ToArray();
                    IterateInlineDict(reader, kvHandlers[0], kvHandlers[1], skipping, knownInlineRefs);
                }
            }
            else if (handler is NullableFieldHandler)
            {
                var hasValue = reader.ReadBool();
                if (hasValue)
                {
                    var itemHandler = ((IFieldHandlerWithNestedFieldHandlers)handler).EnumerateNestedFieldHandlers().First();
                    IterateHandler(reader, itemHandler, skipping, null);
                }
            }
            else if (handler is EncryptedStringHandler)
            {
                var length = reader.ReadVUInt32();
                _visitor?.ScalarAsText($"Encrypted[{length}]");
                if (length>0)
                    reader.SkipBlock(length - 1);
            }
            else if (handler.NeedsCtx() || handler.HandledType() == null)
            {
                throw new BTDBException("Don't know how to iterate " + handler.Name);
            }
            else
            {
                if (skipping || _visitor == null)
                {
                    Action<AbstractBufferedReader> skipper;
                    if (!_skippers.TryGetValue(handler, out skipper))
                    {
                        var meth =
                            ILBuilder.Instance.NewMethod<Action<AbstractBufferedReader>>("Skip" + handler.Name);
                        var il = meth.Generator;
                        handler.Skip(il, il2 => il2.Ldarg(0));
                        il.Ret();
                        skipper = meth.Create();
                        _skippers.Add(handler, skipper);
                    }
                    skipper(reader);
                }
                else
                {
                    Func<AbstractBufferedReader, object> loader;
                    if (!_loaders.TryGetValue(handler, out loader))
                    {
                        var meth =
                            ILBuilder.Instance.NewMethod<Func<AbstractBufferedReader, object>>("Load" + handler.Name);
                        var il = meth.Generator;
                        handler.Load(il, il2 => il2.Ldarg(0));
                        il.Box(handler.HandledType()).Ret();
                        loader = meth.Create();
                        _loaders.Add(handler, loader);
                    }
                    var obj = loader(reader);
                    if (_visitor.NeedScalarAsObject())
                    {
                        _visitor.ScalarAsObject(obj);
                    }
                    if (_visitor.NeedScalarAsText())
                    {
                        _visitor.ScalarAsText(obj == null
                            ? "null"
                            : string.Format(CultureInfo.InvariantCulture, "{0}", obj));
                    }
                }
            }
        }

        void IterateInlineDict(AbstractBufferedReader reader, IFieldHandler keyHandler, IFieldHandler valueHandler, bool skipping, HashSet<int> knownInlineRefs)
        {
            var skip = skipping || _visitor != null && !_visitor.StartDictionary();
            var count = reader.ReadVUInt32();
            while (count-- > 0)
            {
                var skipKey = skip || _visitor != null && !_visitor.StartDictKey();
                IterateHandler(reader, keyHandler, skipKey, knownInlineRefs);
                if (!skipKey) _visitor?.EndDictKey();
                var skipValue = skip || _visitor != null && !_visitor.StartDictValue();
                IterateHandler(reader, valueHandler, skipValue, knownInlineRefs);
                if (!skipValue) _visitor?.EndDictValue();
            }
            if (!skip) _visitor?.EndDictionary();
        }

        void IterateInlineList(AbstractBufferedReader reader, IFieldHandler itemHandler, bool skipping, HashSet<int> knownInlineRefs)
        {
            var skip = skipping || _visitor != null && !_visitor.StartList();
            var count = reader.ReadVUInt32();
            while (count-- > 0)
            {
                var skipItem = skip || _visitor != null && !_visitor.StartItem();
                IterateHandler(reader, itemHandler, skipItem, knownInlineRefs);
                if (!skipItem) _visitor?.EndItem();
            }
            if (!skip) _visitor?.EndList();
        }

        void MarkTableIdVersionFieldInfo(uint tableId, uint version)
        {
            if (!_usedTableVersions.Add(new TableIdVersionId(tableId, version)))
                return;
            MarkTableName(tableId);
            _tr.TransactionProtector.Start();
            _trkv.SetKeyPrefixUnsafe(ObjectDB.TableVersionsPrefix);
            if (_trkv.Find(TwiceVuint2ByteBuffer(tableId, version)) == FindResult.Exact)
            {
                _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
            }
        }

        void MarkLastDictId()
        {
            _trkv.SetKeyPrefixUnsafe(null);
            if (_trkv.FindExactKey(ObjectDB.LastDictIdKey))
            {
                _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
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
                _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
            }
        }

        void MarkRelationName(uint relationId)
        {
            _tr.TransactionProtector.Start();
            _trkv.SetKeyPrefixUnsafe(ObjectDB.RelationNamesPrefix);
            if (_trkv.Find(Vuint2ByteBuffer(relationId)) == FindResult.Exact)
            {
                _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
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
            _tableId2Name = ObjectDB.LoadTablesEnum(_tr.KeyValueDBTransaction)
                    .ToDictionary(pair => pair.Key, pair => pair.Value);
        }

        void LoadRelationInfoDict()
        {
            _relationId2Info = ObjectDB.LoadRelationNamesEnum(_tr.KeyValueDBTransaction).ToList()
                .ToDictionary(pair => pair.Key, pair => LoadRelationInfo(pair));
        }

        ODBIteratorRelationInfo LoadRelationInfo(KeyValuePair<uint, string> idName)
        {
            var res = new ODBIteratorRelationInfo
            {
                Id = idName.Key,
                Name = idName.Value,
            };
            var relationVersions = new Dictionary<uint, RelationVersionInfo>();
            res.LastPersistedVersion = ReadRelationVersions(res.Id, res.Name, relationVersions);
            res.VersionInfos = relationVersions;
            _trkv.SetKeyPrefix(BuildRelationPrefix(res.Id));
            res.RowCount = _trkv.GetKeyValueCount();
            return res;
        }

        TableVersionInfo GetTableVersionInfo(uint tableId, uint version)
        {
            TableVersionInfo res;
            if (_tableVersionInfos.TryGetValue(new TableIdVersionId(tableId, version), out res))
                return res;
            _trkv.SetKeyPrefixUnsafe(ObjectDB.TableVersionsPrefix);
            if (_trkv.Find(TwiceVuint2ByteBuffer(tableId, version)) == FindResult.Exact)
            {
                var reader = new KeyValueDBValueReader(_trkv);
                res = TableVersionInfo.Load(reader, _tr.Owner.FieldHandlerFactory, _tableId2Name[tableId]);
                _tableVersionInfos.Add(new TableIdVersionId(tableId, version), res);
                return res;
            }
            throw new ArgumentException($"TableVersionInfo not found {tableId}-{version}");
        }
    }
}
