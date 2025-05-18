using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

delegate void SkipperFun(ref MemReader reader);

delegate object? LoaderFun(ref MemReader reader);

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

        _loaders = new(ReferenceEqualityComparer<IFieldHandler>.Instance);
    }

    public void LoadGlobalInfo(bool sortTableByNameAsc = false)
    {
        LoadTableNamesDict();
        LoadRelationInfoDict();
        MarkLastDictId();
        using var cursor = _trkv.CreateCursor();
        _singletons = new();
        Span<byte> buf = stackalloc byte[16];
        while (cursor.FindNextKey(ObjectDB.TableSingletonsPrefix))
        {
            _singletons.Add(
                (uint)PackUnpack.UnpackVUInt(cursor.GetKeySpan(ref buf)[(int)ObjectDB.TableSingletonsPrefixLen..]),
                PackUnpack.UnpackVUInt(cursor.GetValueSpan(ref buf)));
        }

        if (sortTableByNameAsc)
        {
            _singletons = _singletons.OrderBy(item => _tableId2Name.GetValueOrDefault(item.Key, ""))
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
            using var cursor = _trkv.CreateCursor();
            if (cursor.Find(Vuint2ByteBuffer(ObjectDB.TableSingletonsPrefix, singleton.Key), 0) == FindResult.Exact)
            {
                _fastVisitor.MarkCurrentKeyAsUsed(cursor);
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

    public unsafe void IterateOid(ulong oid)
    {
        if (!SkipAlreadyVisitedOidChecks && !_visitedOids.Add(oid))
            return;
        using var cursor = _trkv.CreateCursor();
        if (!cursor.FindExactKey(Vuint2ByteBuffer(ObjectDB.AllObjectsPrefix, oid)))
            return; // Object oid was deleted
        _fastVisitor.MarkCurrentKeyAsUsed(cursor);

        Span<byte> buf = default;
        var valueSpan = cursor.GetValueSpan(ref buf);
        fixed (void* p = valueSpan)
        {
            var reader = new MemReader(p, valueSpan.Length);
            var tableId = reader.ReadVUInt32();
            var version = reader.ReadVUInt32();
            MarkTableIdVersionFieldInfo(tableId, version);
            if (_visitor != null && !_visitor.StartObject(oid, tableId,
                    _tableId2Name.GetValueOrDefault(tableId), version))
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
    }

    public unsafe void IterateRelationRow(ODBIteratorRelationInfo relation, long pos)
    {
        var prefix = BuildRelationPrefix(relation.Id);
        using var cursor = _trkv.CreateCursor();
        if (!cursor.FindKeyIndex(prefix, pos)) return;
        var valueCorrupted = cursor.IsValueCorrupted();
        if (_visitor == null || _visitor.StartRelationKey(valueCorrupted))
        {
            Span<byte> buf = default;
            var keySpan = cursor.GetKeySpan(ref buf)[prefix.Length..];
            fixed (void* _ = keySpan)
            {
                var keyReader = MemReader.CreateFromPinnedSpan(keySpan);
                var relationInfo = relation.VersionInfos[relation.LastPersistedVersion];
                IterateFields(ref keyReader, relationInfo.PrimaryKeyFields.Span, null);
            }

            _visitor?.EndRelationKey();
        }

        if (!valueCorrupted && (_visitor == null || _visitor.StartRelationValue()))
        {
            Span<byte> buf = default;
            var valueSpan = cursor.GetValueSpan(ref buf);
            fixed (void* _ = valueSpan)
            {
                var valueReader = MemReader.CreateFromPinnedSpan(valueSpan);
                var version = valueReader.ReadVUInt32();
                var relationInfo = relation.VersionInfos[version];
                IterateFields(ref valueReader, relationInfo.Fields.Span, []);
            }

            _visitor?.EndRelationValue();
        }
    }

    public unsafe void IterateRelation(ODBIteratorRelationInfo relation)
    {
        IterateSecondaryIndexes(relation);
        var prefix = BuildRelationPrefix(relation.Id);
        using var cursor = _trkv.CreateCursor();
        var buf = Span<byte>.Empty;
        while (cursor.FindNextKey(prefix))
        {
            _fastVisitor.MarkCurrentKeyAsUsed(cursor);
            var valueCorrupted = cursor.IsValueCorrupted();
            if (_visitor == null || _visitor.StartRelationKey(valueCorrupted))
            {
                var keyBuf = cursor.GetKeySpan(ref buf)[prefix.Length..];
                fixed (void* _ = keyBuf)
                {
                    var keyReader = MemReader.CreateFromPinnedSpan(keyBuf);
                    var relationInfo = relation.VersionInfos[relation.LastPersistedVersion];
                    IterateFields(ref keyReader, relationInfo.PrimaryKeyFields.Span, null);
                }

                _visitor?.EndRelationKey();
            }

            if (!valueCorrupted && (_visitor == null || _visitor.StartRelationValue()))
            {
                var valueSpan = cursor.GetValueSpan(ref buf);
                fixed (void* _ = valueSpan)
                {
                    var valueReader = MemReader.CreateFromPinnedSpan(valueSpan);
                    var version = valueReader.ReadVUInt32();
                    var relationInfo = relation.VersionInfos[version];
                    IterateFields(ref valueReader, relationInfo.Fields.Span, new());
                    _visitor?.EndRelationValue();
                }
            }
        }
    }

    public unsafe List<(string, string)> IterateRelationStats(ODBIteratorRelationInfo relation)
    {
        var res = new List<(string, string)>();
        //IterateSecondaryIndexes(relation);
        var prefix = BuildRelationPrefix(relation.Id);

        var stat1 = new RefDictionary<uint, uint>();
        var stat2 = new RefDictionary<uint, uint>();
        var stat3 = new RefDictionary<uint, uint>();
        Span<byte> buf = default;
        using var cursor = _trkv.CreateCursor();
        while (cursor.FindNextKey(prefix))
        {
            var ss = cursor.GetStorageSizeOfCurrentKey();
            stat2.GetOrAddValueRef(ss.Key)++;
            stat3.GetOrAddValueRef(ss.Value)++;
            var valueSpan = cursor.GetValueSpan(ref buf);
            fixed (void* _ = valueSpan)
            {
                var valueReader = MemReader.CreateFromPinnedSpan(valueSpan);
                var version = valueReader.ReadVUInt32();
                stat1.GetOrAddValueRef(version)++;
            }
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

    unsafe void IterateSecondaryIndexes(ODBIteratorRelationInfo relation)
    {
        var version = relation.VersionInfos[relation.LastPersistedVersion];
        foreach (var (secKeyName, secKeyIdx) in version.SecondaryKeysNames)
        {
            var secondaryKeyFields = version.GetSecondaryKeyFields(secKeyIdx);
            if (_visitor == null || _visitor.StartSecondaryIndex(secKeyName))
            {
                var prefix = BuildRelationSecondaryKeyPrefix(relation.Id, secKeyIdx);
                using var cursor = _trkv.CreateCursor();
                var buf = Span<byte>.Empty;
                while (cursor.FindNextKey(prefix))
                {
                    var keySpan = cursor.GetKeySpan(ref buf)[prefix.Length..];
                    fixed (void* _ = keySpan)
                    {
                        var reader = MemReader.CreateFromPinnedSpan(keySpan);
                        foreach (var fi in secondaryKeyFields)
                        {
                            if (_visitor == null || !_visitor.StartField(fi.Name)) continue;
                            IterateHandler(ref reader, fi.Handler!, false, null);
                        }
                    }

                    _visitor?.NextSecondaryKey();
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

    void IterateFields(ref MemReader reader, ReadOnlySpan<TableFieldInfo> fields, HashSet<int>? knownInlineRefs)
    {
        foreach (var fi in fields)
        {
            if (fi.Computed) continue;
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

    unsafe uint ReadRelationVersions(uint relationIndex, string name,
        Dictionary<uint, RelationVersionInfo> relationVersions)
    {
        uint lastPersistedVersion = 0;
        var relationInfoResolver = new RelationInfoResolver((ObjectDB)_tr.Owner);

        using var cursor = _trkv.CreateCursor();
        var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[8]);
        writer.WriteByteArrayRaw(ObjectDB.RelationVersionsPrefix);
        writer.WriteVUInt32(relationIndex);
        var prefix = writer.GetSpan();

        var keyBuf = Span<byte>.Empty;
        var valueBuf = Span<byte>.Empty;
        while (cursor.FindNextKey(prefix))
        {
            var keySpan = cursor.GetKeySpan(ref keyBuf)[prefix.Length..];
            var valueSpan = cursor.GetValueSpan(ref valueBuf);
            fixed (void* _ = keySpan)
            fixed (void* __ = valueSpan)
            {
                var keyReader = MemReader.CreateFromPinnedSpan(keySpan);
                lastPersistedVersion = keyReader.ReadVUInt32();
                var valueReader = MemReader.CreateFromPinnedSpan(valueSpan);
                var relationVersionInfo = RelationVersionInfo.LoadUnresolved(ref valueReader, name);
                relationVersionInfo.ResolveFieldHandlers(relationInfoResolver.FieldHandlerFactory);
                relationVersions[lastPersistedVersion] = relationVersionInfo;
            }
        }

        return lastPersistedVersion;
    }

    [SkipLocalsInit]
    unsafe void IterateDict(ulong dictId, IFieldHandler keyHandler, IFieldHandler valueHandler)
    {
        if (_visitor != null && !_visitor.StartDictionary(dictId))
            return;
        var o = ObjectDB.AllDictionariesPrefix.Length;
        var prefix = new byte[o + PackUnpack.LengthVUInt(dictId)];
        Array.Copy(ObjectDB.AllDictionariesPrefix, prefix, o);
        PackUnpack.PackVUInt(prefix, ref o, dictId);
        Span<byte> buf = stackalloc byte[512];
        using var cursor = _trkv.CreateCursor();
        while (cursor.FindNextKey(prefix))
        {
            _fastVisitor.MarkCurrentKeyAsUsed(cursor);
            if (_visitor == null || _visitor.StartDictKey())
            {
                var keySpan = cursor.GetKeySpan(ref buf)[prefix.Length..];
                fixed (void* _ = keySpan)
                {
                    var keyReader = MemReader.CreateFromPinnedSpan(keySpan);
                    IterateHandler(ref keyReader, keyHandler, false, null);
                }

                _visitor?.EndDictKey();
            }

            if (_visitor == null || _visitor.StartDictValue())
            {
                var valueSpan = cursor.GetValueSpan(ref buf);
                fixed (void* _ = valueSpan)
                {
                    var valueReader = MemReader.CreateFromPinnedSpan(valueSpan);
                    IterateHandler(ref valueReader, valueHandler, false, null);
                    _visitor?.EndDictValue();
                }
            }
        }

        _visitor?.EndDictionary();
    }

    unsafe void IterateSet(ulong dictId, IFieldHandler keyHandler)
    {
        if (_visitor != null && !_visitor.StartSet())
            return;
        var o = ObjectDB.AllDictionariesPrefix.Length;
        var prefix = new byte[o + PackUnpack.LengthVUInt(dictId)];
        Array.Copy(ObjectDB.AllDictionariesPrefix, prefix, o);
        PackUnpack.PackVUInt(prefix, ref o, dictId);
        using var cursor = _trkv.CreateCursor();
        var buf = Span<byte>.Empty;
        while (cursor.FindNextKey(prefix))
        {
            _fastVisitor.MarkCurrentKeyAsUsed(cursor);
            if (_visitor == null || _visitor.StartSetKey())
            {
                var keySpan = cursor.GetKeySpan(ref buf)[prefix.Length..];
                fixed (void* _ = keySpan)
                {
                    var keyReader = MemReader.CreateFromPinnedSpan(keySpan);
                    IterateHandler(ref keyReader, keyHandler, false, null);
                }

                _visitor?.EndSetKey();
            }
        }

        _visitor?.EndSet();
    }

    unsafe void IterateHandler(ref MemReader reader, IFieldHandler handler, bool skipping,
        HashSet<int>? knownInlineRefs)
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
            else
            {
                if (_visitor?.NeedScalarAsText() ?? false)
                {
                    _visitor.ScalarAsText("null");
                }
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
                var enc = reader.ReadByteArrayAsSpan();
                var size = cipher.CalcOrderedPlainSizeFor(enc);
                var dec = GC.AllocateUninitializedArray<byte>(size);
                if (!cipher.OrderedDecrypt(enc, dec))
                {
                    _visitor?.ScalarAsText($"Encrypted[{enc!.Length}] failed to decrypt");
                }

                fixed (void* _ = dec)
                {
                    var r = MemReader.CreateFromPinnedSpan(dec);
                    _visitor?.ScalarAsText(r.ReadString()!);
                }
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
                var enc = reader.ReadByteArrayAsSpan();
                var size = cipher.CalcPlainSizeFor(enc);
                var dec = GC.AllocateUninitializedArray<byte>(size);
                if (!cipher.Decrypt(enc, dec))
                {
                    _visitor?.ScalarAsText($"Encrypted[{enc!.Length}] failed to decrypt");
                }

                fixed (void* _ = dec)
                {
                    var r = MemReader.CreateFromPinnedSpan(dec);
                    _visitor?.ScalarAsText(r.ReadString()!);
                }
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
                handler.Skip(ref reader, null);
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

    void IterateInlineDict(ref MemReader reader, IFieldHandler keyHandler, IFieldHandler valueHandler,
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

    void IterateInlineList(ref MemReader reader, IFieldHandler itemHandler, bool skipping,
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
        using var cursor = _trkv.CreateCursor();
        if (cursor.FindExactKey(TwiceVuint2ByteBuffer(ObjectDB.TableVersionsPrefix, tableId, version)))
        {
            _fastVisitor.MarkCurrentKeyAsUsed(cursor);
        }
    }

    void MarkLastDictId()
    {
        using var cursor = _trkv.CreateCursor();
        if (cursor.FindExactKey(ObjectDB.LastDictIdKey))
        {
            _fastVisitor.MarkCurrentKeyAsUsed(cursor);
        }
    }

    void MarkTableName(uint tableId)
    {
        if (!_usedTableIds.Add(tableId))
            return;
        using var cursor = _trkv.CreateCursor();
        if (cursor.FindExactKey(Vuint2ByteBuffer(ObjectDB.TableNamesPrefix, tableId)))
        {
            _fastVisitor.MarkCurrentKeyAsUsed(cursor);
        }
    }

    void MarkRelationName(uint relationId)
    {
        using var cursor = _trkv.CreateCursor();
        if (cursor.FindExactKey(Vuint2ByteBuffer(ObjectDB.RelationNamesPrefix, relationId)))
        {
            _fastVisitor.MarkCurrentKeyAsUsed(cursor);
        }
    }

    ReadOnlySpan<byte> Vuint2ByteBuffer(in ReadOnlySpan<byte> prefix, ulong value)
    {
        var len = PackUnpack.LengthVUInt(value);
        var res = new byte[prefix.Length + len];
        prefix.CopyTo(res);
        PackUnpack.UnsafePackVUInt(ref res[prefix.Length], value, len);
        return res;
    }

    ReadOnlySpan<byte> TwiceVuint2ByteBuffer(in ReadOnlySpan<byte> prefix, uint v1, uint v2)
    {
        var len1 = PackUnpack.LengthVUInt(v1);
        var len2 = PackUnpack.LengthVUInt(v2);
        var res = new byte[prefix.Length + len1 + len2];
        prefix.CopyTo(res);
        PackUnpack.UnsafePackVUInt(ref res[prefix.Length], v1, len1);
        PackUnpack.UnsafePackVUInt(ref res[prefix.Length + len1], v2, len2);
        return res;
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
        using var cursor = _trkv.CreateCursor();
        res.RowCount = cursor.GetKeyValueCount(BuildRelationPrefix(res.Id));
        return res;
    }

    [SkipLocalsInit]
    unsafe TableVersionInfo GetTableVersionInfo(uint tableId, uint version)
    {
        if (_tableVersionInfos.TryGetValue(new(tableId, version), out var res))
            return res;
        using var cursor = _trkv.CreateCursor();
        if (cursor.FindExactKey(TwiceVuint2ByteBuffer(ObjectDB.TableVersionsPrefix, tableId, version)))
        {
            Span<byte> buf = stackalloc byte[4096];
            var valueSpan = cursor.GetValueSpan(ref buf);
            fixed (void* _ = valueSpan)
            {
                var reader = MemReader.CreateFromPinnedSpan(valueSpan);
                res = TableVersionInfo.Load(ref reader, _tr.Owner.FieldHandlerFactory, _tableId2Name[tableId]);
                _tableVersionInfos.Add(new(tableId, version), res);
            }

            return res;
        }

        throw new ArgumentException($"TableVersionInfo not found {tableId}-{version}");
    }
}
