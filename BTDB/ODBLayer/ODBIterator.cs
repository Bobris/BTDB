using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

delegate void SkipperFun(ref SpanReader reader);

delegate object? LoaderFun(ref SpanReader reader);

public class ODBIterator
{
    readonly IInternalObjectDBTransaction _tr;
    IODBFastVisitor _fastVisitor;
    IODBVisitor? _visitor;
    Dictionary<uint, string> _tableId2Name;
    readonly IKeyValueDBTransaction _trkv;
    Dictionary<uint, ulong> _singletons;
    readonly HashSet<uint> _usedTableIds;
    readonly HashSet<ulong> _visitedOids;
    readonly HashSet<TableIdVersionId> _usedTableVersions;
    readonly Dictionary<TableIdVersionId, TableVersionInfo> _tableVersionInfos;
    readonly Dictionary<IFieldHandler, SkipperFun> _skippers;

    readonly Dictionary<IFieldHandler, LoaderFun> _loaders;

    //relations
    Dictionary<uint, ODBIteratorRelationInfo> _relationId2Info;

    public IDictionary<uint, string> TableId2Name => _tableId2Name;
    public IReadOnlyDictionary<uint, ulong> TableId2SingletonOid => _singletons;
    public IReadOnlyDictionary<uint, ODBIteratorRelationInfo> RelationId2Info => _relationId2Info;
    public IReadOnlyDictionary<TableIdVersionId, TableVersionInfo> TableVersionInfos => _tableVersionInfos;
    public bool SkipAlreadyVisitedOidChecks;

    public ODBIterator(IObjectDBTransaction tr, IODBFastVisitor visitor)
    {
        _tr = (IInternalObjectDBTransaction)tr;
        _trkv = _tr.KeyValueDBTransaction;
        _fastVisitor = visitor;
        _visitor = visitor as IODBVisitor;
        _usedTableIds = new();
        _visitedOids = new();
        _usedTableVersions = new();
        _tableVersionInfos = new();

        _skippers = new(ReferenceEqualityComparer<IFieldHandler>.Instance);
        _loaders = new(ReferenceEqualityComparer<IFieldHandler>.Instance);
    }

    public void LoadGlobalInfo(bool sortTableByNameAsc = false)
    {
        LoadTableNamesDict();
        LoadRelationInfoDict();
        MarkLastDictId();
        _trkv.InvalidateCurrentKey();
        _singletons = new();
        while (_trkv.FindNextKey(ObjectDB.TableSingletonsPrefix))
        {
            _singletons.Add(
                new SpanReader(_trkv.GetKey().Slice((int)ObjectDB.TableSingletonsPrefixLen)).ReadVUInt32(),
                new SpanReader(_trkv.GetValue()).ReadVUInt64());
        }

        if (sortTableByNameAsc)
        {
            _singletons = _singletons.OrderBy(item => _tableId2Name.TryGetValue(item.Key, out var name) ? name : "")
                .ToDictionary(item => item.Key, item => item.Value);
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
            if (_trkv.Find(Vuint2ByteBuffer(ObjectDB.TableSingletonsPrefix, singleton.Key), 0) == FindResult.Exact)
            {
                _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
            }

            IterateOid(singleton.Value);
        }

        foreach (var relation in _relationId2Info)
        {
            if (_visitor != null && !_visitor.StartRelation(relation.Value))
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
        if (!_trkv.FindExactKey(Vuint2ByteBuffer(ObjectDB.AllObjectsPrefix, oid)))
            return; // Object oid was deleted
        _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
        var reader = new SpanReader(_trkv.GetValue());
        var tableId = reader.ReadVUInt32();
        var version = reader.ReadVUInt32();
        MarkTableIdVersionFieldInfo(tableId, version);
        if (_visitor != null && !_visitor.StartObject(oid, tableId,
            _tableId2Name.TryGetValue(tableId, out var tableName) ? tableName : null, version))
            return;
        var tvi = GetTableVersionInfo(tableId, version);
        var knownInlineId = new HashSet<int>();
        for (var i = 0; i < tvi.FieldCount; i++)
        {
            var fi = tvi[i];
            if (_visitor == null || _visitor.StartField(fi.Name))
            {
                IterateHandler(ref reader, fi.Handler!, false, knownInlineId);
                _visitor?.EndField();
            }
            else
            {
                IterateHandler(ref reader, fi.Handler!, true, knownInlineId);
            }
        }

        _visitor?.EndObject();
    }

    public void IterateRelationRow(ODBIteratorRelationInfo relation, long pos)
    {
        var prefix = BuildRelationPrefix(relation.Id);
        if (!_trkv.SetKeyIndex(prefix, pos)) return;
        var prevProtectionCounter = _trkv.CursorMovedCounter;
        var valueCorrupted = _trkv.IsValueCorrupted();
        if (_visitor == null || _visitor.StartRelationKey(valueCorrupted))
        {
            var keyReader = new SpanReader(_trkv.GetKey().Slice(prefix.Length));
            var relationInfo = relation.VersionInfos[relation.LastPersistedVersion];
            IterateFields(ref keyReader, relationInfo.PrimaryKeyFields.Span, null);
            _visitor?.EndRelationKey();
        }

        if (_trkv.CursorMovedCounter != prevProtectionCounter)
        {
            if (!_trkv.SetKeyIndex(prefix, pos)) return;
        }

        if (!valueCorrupted && (_visitor == null || _visitor.StartRelationValue()))
        {
            var valueReader = new SpanReader(_trkv.GetValue());
            var version = valueReader.ReadVUInt32();
            var relationInfo = relation.VersionInfos[version];
            IterateFields(ref valueReader, relationInfo.Fields.Span, new());
            _visitor?.EndRelationValue();
        }
    }

    public void IterateRelation(ODBIteratorRelationInfo relation)
    {
        IterateSecondaryIndexes(relation);
        var prefix = BuildRelationPrefix(relation.Id);

        long prevProtectionCounter = 0;
        long pos = 0;
        while (true)
        {
            if (pos == 0)
            {
                if (!_trkv.FindFirstKey(prefix)) break;
            }
            else
            {
                if (_trkv.CursorMovedCounter != prevProtectionCounter)
                {
                    if (!_trkv.SetKeyIndex(prefix, pos)) break;
                }
                else
                {
                    if (!_trkv.FindNextKey(prefix)) break;
                }
            }

            _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
            prevProtectionCounter = _trkv.CursorMovedCounter;
            var valueCorrupted = _trkv.IsValueCorrupted();
            if (_visitor == null || _visitor.StartRelationKey(valueCorrupted))
            {
                var keyReader = new SpanReader(_trkv.GetKey().Slice(prefix.Length));
                var relationInfo = relation.VersionInfos[relation.LastPersistedVersion];
                IterateFields(ref keyReader, relationInfo.PrimaryKeyFields.Span, null);
                _visitor?.EndRelationKey();
            }

            if (_trkv.CursorMovedCounter != prevProtectionCounter)
            {
                if (!_trkv.SetKeyIndex(prefix, pos)) break;
            }

            if (!valueCorrupted && (_visitor == null || _visitor.StartRelationValue()))
            {
                var valueReader = new SpanReader(_trkv.GetValue());
                var version = valueReader.ReadVUInt32();
                var relationInfo = relation.VersionInfos[version];
                IterateFields(ref valueReader, relationInfo.Fields.Span, new());
                _visitor?.EndRelationValue();
            }

            pos++;
        }
    }

    public List<(string, string)> IterateRelationStats(ODBIteratorRelationInfo relation)
    {
        var res = new List<(string, string)>();
        //IterateSecondaryIndexes(relation);
        var prefix = BuildRelationPrefix(relation.Id);

        var stat1 = new RefDictionary<uint, uint>();
        var stat2 = new RefDictionary<uint, uint>();
        var stat3 = new RefDictionary<uint, uint>();
        long prevProtectionCounter = 0;
        long pos = 0;
        while (true)
        {
            if (pos == 0)
            {
                if (!_trkv.FindFirstKey(prefix)) break;
            }
            else
            {
                if (_trkv.CursorMovedCounter != prevProtectionCounter)
                {
                    if (!_trkv.SetKeyIndex(prefix, pos)) break;
                }
                else
                {
                    if (!_trkv.FindNextKey(prefix)) break;
                }
            }

            var ss = _trkv.GetStorageSizeOfCurrentKey();
            stat2.GetOrAddValueRef(ss.Key)++;
            stat3.GetOrAddValueRef(ss.Value)++;
            var valueReader = new SpanReader(_trkv.GetValue());
            var version = valueReader.ReadVUInt32();
            stat1.GetOrAddValueRef(version)++;
            pos++;
        }

        foreach (var p in stat1.OrderBy(p => p.Key))
        {
            res.Add(("Version " + p.Key + "used count", p.Value.ToString()));
        }

        foreach (var p in stat2.OrderBy(p => p.Key))
        {
            res.Add(("Key size " + p.Key + " count", p.Value.ToString()));
        }

        foreach (var p in stat3.OrderBy(p => p.Key))
        {
            res.Add(("Value size " + p.Key + " count", p.Value.ToString()));
        }

        return res;
    }

    void IterateSecondaryIndexes(ODBIteratorRelationInfo relation)
    {
        var version = relation.VersionInfos[relation.LastPersistedVersion];
        foreach (var (secKeyName, secKeyIdx) in version.SecondaryKeysNames)
        {
            var secondaryKeyFields = version.GetSecondaryKeyFields(secKeyIdx);
            if (_visitor == null || _visitor.StartSecondaryIndex(secKeyName))
            {
                var prefix = BuildRelationSecondaryKeyPrefix(relation.Id, secKeyIdx);
                long prevProtectionCounter = 0;
                long pos = 0;
                while (true)
                {
                    if (pos == 0)
                    {
                        if (!_trkv.FindFirstKey(prefix)) break;
                    }
                    else
                    {
                        if (_trkv.CursorMovedCounter != prevProtectionCounter)
                        {
                            if (!_trkv.SetKeyIndex(prefix, pos)) break;
                        }
                        else
                        {
                            if (!_trkv.FindNextKey(prefix)) break;
                        }
                    }

                    var reader = new SpanReader(_trkv.GetKey().Slice(prefix.Length));
                    foreach (var fi in secondaryKeyFields)
                    {
                        if (_visitor == null || !_visitor.StartField(fi.Name)) continue;
                        IterateHandler(ref reader, fi.Handler!, false, null);
                    }

                    _visitor?.NextSecondaryKey();
                    pos++;
                }

                _visitor?.EndSecondaryIndex();
            }
        }
    }

    static byte[] BuildRelationSecondaryKeyPrefix(uint relationIndex, uint secondaryKeyIndex)
    {
        var prefix =
            new byte[1 + PackUnpack.LengthVUInt(relationIndex) + PackUnpack.LengthVUInt(secondaryKeyIndex)];
        prefix[0] = ObjectDB.AllRelationsSKPrefixByte;
        int pos = 1;
        PackUnpack.PackVUInt(prefix, ref pos, relationIndex);
        PackUnpack.PackVUInt(prefix, ref pos, secondaryKeyIndex);
        return prefix;
    }

    static byte[] BuildRelationPrefix(uint relationIndex)
    {
        var o = ObjectDB.AllRelationsPKPrefix.Length;
        var prefix = new byte[o + PackUnpack.LengthVUInt(relationIndex)];
        Array.Copy(ObjectDB.AllRelationsPKPrefix, prefix, o);
        PackUnpack.PackVUInt(prefix, ref o, relationIndex);
        return prefix;
    }

    void IterateFields(ref SpanReader reader, ReadOnlySpan<TableFieldInfo> fields, HashSet<int>? knownInlineRefs)
    {
        foreach (var fi in fields)
        {
            if (_visitor == null || _visitor.StartField(fi.Name))
            {
                IterateHandler(ref reader, fi.Handler!, false, knownInlineRefs);
                _visitor?.EndField();
            }
            else
            {
                IterateHandler(ref reader, fi.Handler!, true, knownInlineRefs);
            }
        }
    }

    uint ReadRelationVersions(uint relationIndex, string name,
        Dictionary<uint, RelationVersionInfo> relationVersions)
    {
        uint lastPersistedVersion = 0;
        var relationInfoResolver = new RelationInfoResolver((ObjectDB)_tr.Owner);

        var writer = new SpanWriter();
        writer.WriteByteArrayRaw(ObjectDB.RelationVersionsPrefix);
        writer.WriteVUInt32(relationIndex);
        var prefix = writer.GetSpan().ToArray();
        if (!_trkv.FindFirstKey(prefix))
        {
            return lastPersistedVersion;
        }

        do
        {
            var keyReader = new SpanReader(_trkv.GetKey().Slice(prefix.Length));
            var valueReader = new SpanReader(_trkv.GetValue());
            lastPersistedVersion = keyReader.ReadVUInt32();
            var relationVersionInfo = RelationVersionInfo.LoadUnresolved(ref valueReader, name);
            relationVersionInfo.ResolveFieldHandlers(relationInfoResolver.FieldHandlerFactory);
            relationVersions[lastPersistedVersion] = relationVersionInfo;
        } while (_trkv.FindNextKey(prefix));

        return lastPersistedVersion;
    }

    [SkipLocalsInit]
    void IterateDict(ulong dictId, IFieldHandler keyHandler, IFieldHandler valueHandler)
    {
        if (_visitor != null && !_visitor.StartDictionary(dictId))
            return;
        var o = ObjectDB.AllDictionariesPrefix.Length;
        var prefix = new byte[o + PackUnpack.LengthVUInt(dictId)];
        Array.Copy(ObjectDB.AllDictionariesPrefix, prefix, o);
        PackUnpack.PackVUInt(prefix, ref o, dictId);
        long prevProtectionCounter = 0;
        long pos = 0;
        Span<byte> keyBuffer = stackalloc byte[512];
        while (true)
        {
            if (pos == 0)
            {
                if (!_trkv.FindFirstKey(prefix)) break;
            }
            else
            {
                if (_trkv.CursorMovedCounter != prevProtectionCounter)
                {
                    if (!_trkv.SetKeyIndex(prefix, pos)) break;
                }
                else
                {
                    if (!_trkv.FindNextKey(prefix)) break;
                }
            }

            _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
            prevProtectionCounter = _trkv.CursorMovedCounter;
            if (_visitor == null || _visitor.StartDictKey())
            {
                var keyReader = new SpanReader(_trkv
                    .GetKey(ref MemoryMarshal.GetReference(keyBuffer), keyBuffer.Length).Slice(prefix.Length));
                IterateHandler(ref keyReader, keyHandler, false, null);
                _visitor?.EndDictKey();
            }

            if (_trkv.CursorMovedCounter != prevProtectionCounter)
            {
                if (!_trkv.SetKeyIndex(prefix, pos)) break;
            }

            if (_visitor == null || _visitor.StartDictValue())
            {
                var valueReader = new SpanReader(_trkv.GetValue());
                IterateHandler(ref valueReader, valueHandler, false, null);
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
        long prevProtectionCounter = 0;
        long pos = 0;
        while (true)
        {
            if (pos == 0)
            {
                if (!_trkv.FindFirstKey(prefix)) break;
            }
            else
            {
                if (_trkv.CursorMovedCounter != prevProtectionCounter)
                {
                    if (!_trkv.SetKeyIndex(prefix, pos)) break;
                }
                else
                {
                    if (!_trkv.FindNextKey(prefix)) break;
                }
            }

            _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
            prevProtectionCounter = _trkv.CursorMovedCounter;
            if (_visitor == null || _visitor.StartSetKey())
            {
                var keyReader = new SpanReader(_trkv.GetKey().Slice(prefix.Length));
                IterateHandler(ref keyReader, keyHandler, false, null);
                _visitor?.EndSetKey();
            }

            pos++;
        }

        _visitor?.EndSet();
    }

    void IterateHandler(ref SpanReader reader, IFieldHandler handler, bool skipping, HashSet<int>? knownInlineRefs)
    {
        if (handler is ODBDictionaryFieldHandler)
        {
            var dictId = reader.ReadVUInt64();
            if (!skipping)
            {
                var kvHandlers = ((IFieldHandlerWithNestedFieldHandlers)handler).EnumerateNestedFieldHandlers()
                    .ToArray();
                IterateDict(dictId, kvHandlers[0], kvHandlers[1]);
            }
        }
        else if (handler is ODBSetFieldHandler)
        {
            var dictId = reader.ReadVUInt64();
            if (!skipping)
            {
                var keyHandler = ((IFieldHandlerWithNestedFieldHandlers)handler).EnumerateNestedFieldHandlers()
                    .First();
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
                    if (knownInlineRefs.Contains((int)oid))
                    {
                        if (!skipping) _visitor?.InlineBackRef((int)oid);
                        return;
                    }

                    if (!skipping) _visitor?.InlineRef((int)oid);
                    knownInlineRefs.Add((int)oid);
                }

                var tableId = reader.ReadVUInt32();
                var version = reader.ReadVUInt32();
                if (!skipping) MarkTableIdVersionFieldInfo(tableId, version);
                var skip = skipping ||
                           _visitor != null && !_visitor.StartInlineObject(tableId,
                               _tableId2Name.TryGetValue(tableId, out var tableName) ? tableName : null, version);
                var tvi = GetTableVersionInfo(tableId, version);
                var knownInlineRefsNested = new HashSet<int>();
                for (var i = 0; i < tvi.FieldCount; i++)
                {
                    var fi = tvi[i];
                    var skipField = skip || _visitor != null && !_visitor.StartField(fi.Name);
                    IterateHandler(ref reader, fi.Handler!, skipField, knownInlineRefsNested);
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
                var itemHandler = ((IFieldHandlerWithNestedFieldHandlers)handler).EnumerateNestedFieldHandlers()
                    .First();
                IterateInlineList(ref reader, itemHandler, skipping, knownInlineRefs);
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
                var kvHandlers = ((IFieldHandlerWithNestedFieldHandlers)handler).EnumerateNestedFieldHandlers()
                    .ToArray();
                IterateInlineDict(ref reader, kvHandlers[0], kvHandlers[1], skipping, knownInlineRefs);
            }
        }
        else if (handler is NullableFieldHandler)
        {
            var hasValue = reader.ReadBool();
            if (hasValue)
            {
                var itemHandler = ((IFieldHandlerWithNestedFieldHandlers)handler).EnumerateNestedFieldHandlers()
                    .First();
                IterateHandler(ref reader, itemHandler, skipping, null);
            }
        }
        else if (handler is OrderedEncryptedStringHandler)
        {
            var cipher = _tr.Owner.GetSymmetricCipher();
            if (cipher is InvalidSymmetricCipher)
            {
                var length = reader.ReadVUInt32();
                _visitor?.ScalarAsText($"Encrypted[{length}]");
                if (length > 0)
                    reader.SkipBlock(length - 1);
            }
            else
            {
                var enc = reader.ReadByteArray();
                var size = cipher.CalcOrderedPlainSizeFor(enc);
                var dec = new byte[size];
                if (!cipher.OrderedDecrypt(enc, dec))
                {
                    _visitor?.ScalarAsText($"Encrypted[{enc!.Length}] failed to decrypt");
                }

                var r = new SpanReader(dec);
                _visitor?.ScalarAsText(r.ReadString()!);
            }
        }
        else if (handler is EncryptedStringHandler)
        {
            var cipher = _tr.Owner.GetSymmetricCipher();
            if (cipher is InvalidSymmetricCipher)
            {
                var length = reader.ReadVUInt32();
                _visitor?.ScalarAsText($"Encrypted[{length}]");
                if (length > 0)
                    reader.SkipBlock(length - 1);
            }
            else
            {
                var enc = reader.ReadByteArray();
                var size = cipher.CalcPlainSizeFor(enc);
                var dec = new byte[size];
                if (!cipher.Decrypt(enc, dec))
                {
                    _visitor?.ScalarAsText($"Encrypted[{enc!.Length}] failed to decrypt");
                }

                var r = new SpanReader(dec.AsSpan());
                _visitor?.ScalarAsText(r.ReadString()!);
            }
        }
        else if (handler is TupleFieldHandler tupleFieldHandler)
        {
            foreach (var fieldHandler in tupleFieldHandler.EnumerateNestedFieldHandlers())
            {
                var skipField = _visitor != null && !_visitor.StartItem();
                IterateHandler(ref reader, fieldHandler, skipField, knownInlineRefs);
                if (!skipField) _visitor?.EndItem();
            }
        }
        else if (handler.NeedsCtx() || handler.HandledType() == null)
        {
            throw new BTDBException("Don't know how to iterate " + handler.Name);
        }
        else
        {
            if (skipping || _visitor == null)
            {
                if (!_skippers.TryGetValue(handler, out var skipper))
                {
                    var meth =
                        ILBuilder.Instance.NewMethod<SkipperFun>("Skip" + handler.Name);
                    var il = meth.Generator;
                    handler.Skip(il, il2 => il2.Ldarg(0), null);
                    il.Ret();
                    skipper = meth.Create();
                    _skippers.Add(handler, skipper);
                }

                skipper(ref reader);
            }
            else
            {
                if (!_loaders.TryGetValue(handler, out var loader))
                {
                    var meth =
                        ILBuilder.Instance.NewMethod<LoaderFun>("Load" + handler.Name);
                    var il = meth.Generator;
                    handler.Load(il, il2 => il2.Ldarg(0), null);
                    il.Box(handler.HandledType()!).Ret();
                    loader = meth.Create();
                    _loaders.Add(handler, loader);
                }

                var obj = loader(ref reader);
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

    void IterateInlineDict(ref SpanReader reader, IFieldHandler keyHandler, IFieldHandler valueHandler,
        bool skipping, HashSet<int>? knownInlineRefs)
    {
        var skip = skipping || _visitor != null && !_visitor.StartDictionary();
        var count = reader.ReadVUInt32();
        while (count-- > 0)
        {
            var skipKey = skip || _visitor != null && !_visitor.StartDictKey();
            IterateHandler(ref reader, keyHandler, skipKey, knownInlineRefs);
            if (!skipKey) _visitor?.EndDictKey();
            var skipValue = skip || _visitor != null && !_visitor.StartDictValue();
            IterateHandler(ref reader, valueHandler, skipValue, knownInlineRefs);
            if (!skipValue) _visitor?.EndDictValue();
        }

        if (!skip) _visitor?.EndDictionary();
    }

    void IterateInlineList(ref SpanReader reader, IFieldHandler itemHandler, bool skipping,
        HashSet<int>? knownInlineRefs)
    {
        var skip = skipping || _visitor != null && !_visitor.StartList();
        var count = reader.ReadVUInt32();
        while (count-- > 0)
        {
            var skipItem = skip || _visitor != null && !_visitor.StartItem();
            IterateHandler(ref reader, itemHandler, skipItem, knownInlineRefs);
            if (!skipItem) _visitor?.EndItem();
        }

        if (!skip) _visitor?.EndList();
    }

    void MarkTableIdVersionFieldInfo(uint tableId, uint version)
    {
        if (!_usedTableVersions.Add(new(tableId, version)))
            return;
        MarkTableName(tableId);
        if (_trkv.Find(TwiceVuint2ByteBuffer(ObjectDB.TableVersionsPrefix, tableId, version), 0) ==
            FindResult.Exact)
        {
            _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
        }
    }

    void MarkLastDictId()
    {
        if (_trkv.FindExactKey(ObjectDB.LastDictIdKey))
        {
            _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
        }
    }

    void MarkTableName(uint tableId)
    {
        if (!_usedTableIds.Add(tableId))
            return;
        if (_trkv.FindExactKey(Vuint2ByteBuffer(ObjectDB.TableNamesPrefix, tableId)))
        {
            _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
        }
    }

    void MarkRelationName(uint relationId)
    {
        if (_trkv.FindExactKey(Vuint2ByteBuffer(ObjectDB.RelationNamesPrefix, relationId)))
        {
            _fastVisitor.MarkCurrentKeyAsUsed(_trkv);
        }
    }

    ReadOnlySpan<byte> Vuint2ByteBuffer(in ReadOnlySpan<byte> prefix, ulong value)
    {
        var writer = new SpanWriter();
        writer.WriteBlock(prefix);
        writer.WriteVUInt64(value);
        return writer.GetSpan();
    }

    ReadOnlySpan<byte> TwiceVuint2ByteBuffer(in ReadOnlySpan<byte> prefix, uint v1, uint v2)
    {
        var writer = new SpanWriter();
        writer.WriteBlock(prefix);
        writer.WriteVUInt32(v1);
        writer.WriteVUInt32(v2);
        return writer.GetSpan();
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
        res.RowCount = _trkv.GetKeyValueCount(BuildRelationPrefix(res.Id));
        return res;
    }

    TableVersionInfo GetTableVersionInfo(uint tableId, uint version)
    {
        if (_tableVersionInfos.TryGetValue(new(tableId, version), out var res))
            return res;
        if (_trkv.FindExactKey(TwiceVuint2ByteBuffer(ObjectDB.TableVersionsPrefix, tableId, version)))
        {
            var reader = new SpanReader(_trkv.GetValue());
            res = TableVersionInfo.Load(ref reader, _tr.Owner.FieldHandlerFactory, _tableId2Name[tableId]);
            _tableVersionInfos.Add(new(tableId, version), res);
            return res;
        }

        throw new ArgumentException($"TableVersionInfo not found {tableId}-{version}");
    }
}
