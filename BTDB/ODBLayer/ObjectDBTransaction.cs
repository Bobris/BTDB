using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

class ObjectDBTransaction : IInternalObjectDBTransaction
{
    readonly ObjectDB _owner;
    IKeyValueDBTransaction? _keyValueTr;
    readonly bool _readOnly;
    readonly long _transactionNumber;

    Dictionary<ulong, object>? _objSmallCache;
    Dictionary<object, DBObjectMetadata>? _objSmallMetadata;
    Dictionary<ulong, WeakReference>? _objBigCache;
    ConditionalWeakTable<object, DBObjectMetadata>? _objBigMetadata;
    int _lastGCIndex;

    Dictionary<ulong, object>? _dirtyObjSet;
    HashSet<TableInfo>? _updatedTables;

    public ObjectDBTransaction(ObjectDB owner, IKeyValueDBTransaction keyValueTr, bool readOnly)
    {
        _owner = owner;
        _keyValueTr = keyValueTr;
        _readOnly = readOnly;
        _transactionNumber = keyValueTr.GetTransactionNumber();
    }

    public void Dispose()
    {
        if (_keyValueTr == null) return;
        _keyValueTr.Dispose();
        _keyValueTr = null;
    }

    public IObjectDB Owner => _owner;

    public IKeyValueDBTransaction? KeyValueDBTransaction => _keyValueTr;

    public bool RollbackAdvised
    {
        get => _keyValueTr!.RollbackAdvised;
        set => _keyValueTr!.RollbackAdvised = value;
    }

    public ulong AllocateDictionaryId()
    {
        return _owner.AllocateNewDictId();
    }

    public object ReadInlineObject(ref SpanReader reader, IReaderCtx readerCtx, bool skipping)
    {
        var tableId = reader.ReadVUInt32();
        var tableVersion = reader.ReadVUInt32();
        var tableInfo = _owner.TablesInfo.FindById(tableId);
        if (tableInfo == null) _owner.ActualOptions.ThrowBTDBException($"Unknown TypeId {tableId} of inline object");
        if (skipping && !TryToEnsureClientTypeNotNull(tableInfo))
        {
            var obj = new BTDBException("Skipped InlineObject "+tableInfo.Name);
            readerCtx.RegisterObject(obj);
            tableInfo.GetSkipper(tableVersion)(this, null, ref reader, obj);
            readerCtx.ReadObjectDone(ref reader);
            return obj;
        }
        else
        {
            EnsureClientTypeNotNull(tableInfo);
            var obj = tableInfo.Creator(this, null);
            readerCtx.RegisterObject(obj);
            tableInfo.GetLoader(tableVersion)(this, null, ref reader, obj);
            readerCtx.ReadObjectDone(ref reader);
            return obj;
        }
    }

    public void FreeContentInNativeObject(ref SpanReader reader, IReaderCtx readerCtx)
    {
        var tableId = reader.ReadVUInt32();
        var tableVersion = reader.ReadVUInt32();
        var tableInfo = _owner.TablesInfo.FindById(tableId);
        if (tableInfo == null) _owner.ActualOptions.ThrowBTDBException($"Unknown TypeId {tableId} of inline object");
        if (TryToEnsureClientTypeNotNull(tableInfo))
        {
            tableInfo.GetLoader(tableVersion); // Create loader eagerly will register all nested types
        }
        var freeContentTuple = tableInfo.GetFreeContent(tableVersion);
        var readerWithFree = (DBReaderWithFreeInfoCtx)readerCtx;
        freeContentTuple.Item2(this, null, ref reader, readerWithFree.DictIds);
    }

    public void SetSerializationCallbacks(IInternalSerializationCallbacks? callbacks)
    {
        if (callbacks == null)
        {
            // Commit all dirty indirect objects
            while (_dirtyObjSet != null)
            {
                var curObjsToStore = _dirtyObjSet;
                _dirtyObjSet = null;
                foreach (var o in curObjsToStore)
                {
                    StoreObject(o.Value);
                }
            }
        }
        _serializationCallbacks = callbacks;
    }

    public bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
    {
        if (_serializationCallbacks != null)
        {
            _serializationCallbacks.CreateKeyValue(key, value);
            return true;
        }

        return _keyValueTr!.CreateOrUpdateKeyValue(key, value);
    }

    public void WriteInlineObject(ref SpanWriter writer, object @object, IWriterCtx writerCtx)
    {
        var ti = GetTableInfoFromType(@object.GetType());
        if (ti == null)
        {
            _owner.ActualOptions.ThrowBTDBException(
                $"Object of type {@object.GetType().ToSimpleName()} is not known how to store as inline object.");
        }

        EnsureClientTypeNotNull(ti!);
        IfNeededPersistTableInfo(ti);
        writer.WriteVUInt32(ti.Id);
        writer.WriteVUInt32(ti.ClientTypeVersion);
        ti.Saver(this, null, ref writer, @object);
    }

    void IfNeededPersistTableInfo(TableInfo tableInfo)
    {
        if (_serializationCallbacks != null)
        {
            if (tableInfo.LastPersistedVersion != tableInfo.ClientTypeVersion || tableInfo.NeedStoreSingletonOid)
            {
                SerializeTableInfo(tableInfo);
                tableInfo.LastPersistedVersion = tableInfo.ClientTypeVersion;
                tableInfo.ResetNeedStoreSingletonOid();
            }

            return;
        }
        if (_readOnly) return;
        if (tableInfo.LastPersistedVersion != tableInfo.ClientTypeVersion || tableInfo.NeedStoreSingletonOid)
        {
            if (_updatedTables == null) _updatedTables = new HashSet<TableInfo>();
            if (_updatedTables.Add(tableInfo))
            {
                PersistTableInfo(tableInfo);
            }
        }
    }

    public IEnumerable<T> Enumerate<T>() where T : class
    {
        return Enumerate(typeof(T)).Cast<T>();
    }

    public IEnumerable<object> Enumerate(Type? type)
    {
        if (type == typeof(object)) type = null;
        else if (type != null) AutoRegisterType(type);
        ulong oid = 0;
        var finalOid = _owner.GetLastAllocatedOid();
        long prevProtectionCounter = 0;
        while (true)
        {
            if (oid == 0)
            {
                if (!_keyValueTr!.FindFirstKey(ObjectDB.AllObjectsPrefix)) break;
                prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            }
            else
            {
                if (prevProtectionCounter != _keyValueTr!.CursorMovedCounter)
                {
                    oid++;
                    var key = BuildKeyFromOidWithAllObjectsPrefix(oid);
                    var result = _keyValueTr.Find(key, 0);
                    if (result == FindResult.Previous)
                    {
                        if (!_keyValueTr.FindNextKey(ObjectDB.AllObjectsPrefix))
                        {
                            result = FindResult.NotFound;
                        }
                    }

                    if (result == FindResult.NotFound)
                    {
                        oid--;
                        break;
                    }
                }
                else
                {
                    if (!_keyValueTr!.FindNextKey(ObjectDB.AllObjectsPrefix)) break;
                }

                prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            }

            oid = ReadOidFromCurrentKeyInTransaction();
            var o = GetObjFromObjCacheByOid(oid);
            if (o != null)
            {
                if (type == null || type.IsInstanceOfType(o))
                {
                    yield return o;
                }

                continue;
            }

            ReadObjStart(oid, out var tableInfo, out var reader);
            if (type != null && !type.IsAssignableFrom(tableInfo.ClientType)) continue;
            var obj = ReadObjFinish(oid, tableInfo, ref reader);
            yield return obj;
        }

        if (_dirtyObjSet == null) yield break;
        var dirtyObjsToEnum = _dirtyObjSet.Where(p => p.Key > oid && p.Key <= finalOid).ToList();
        dirtyObjsToEnum.Sort((p1, p2) => p1.Key < p2.Key ? -1 : p1.Key > p2.Key ? 1 : 0);
        foreach (var dObjPair in dirtyObjsToEnum)
        {
            var obj = dObjPair.Value;
            if (type != null && !type.IsInstanceOfType(obj)) continue;
            yield return obj;
        }
    }

    object? GetObjFromObjCacheByOid(ulong oid)
    {
        if (_objSmallCache != null)
        {
            return !_objSmallCache.TryGetValue(oid, out var result) ? null : result;
        }

        if (_objBigCache != null)
        {
            if (_objBigCache.TryGetValue(oid, out var weakObj))
            {
                return weakObj.Target;
            }
        }

        return null;
    }

    object ReadObjFinish(ulong oid, TableInfo tableInfo, ref SpanReader reader)
    {
        var tableVersion = reader.ReadVUInt32();
        var metadata = new DBObjectMetadata(oid, DBObjectState.Read);
        var obj = tableInfo.Creator(this, metadata);
        AddToObjCache(oid, obj, metadata);
        tableInfo.GetLoader(tableVersion)(this, metadata, ref reader, obj);
        return obj;
    }

    void AddToObjCache(ulong oid, object obj, DBObjectMetadata metadata)
    {
        if (_objBigCache != null)
        {
            CompactObjCacheIfNeeded();
            _objBigCache![oid] = new WeakReference(obj);
            _objBigMetadata!.Add(obj, metadata);
            return;
        }

        if (_objSmallCache == null)
        {
            _objSmallCache = new Dictionary<ulong, object>();
            _objSmallMetadata =
                new Dictionary<object, DBObjectMetadata>(ReferenceEqualityComparer<object>.Instance);
        }
        else if (_objSmallCache.Count > 30)
        {
            _objBigCache = new Dictionary<ulong, WeakReference>();
            _objBigMetadata = new ConditionalWeakTable<object, DBObjectMetadata>();
            foreach (var pair in _objSmallCache)
            {
                _objBigCache.Add(pair.Key, new WeakReference(pair.Value));
            }

            _objSmallCache = null;
            foreach (var pair in _objSmallMetadata!)
            {
                _objBigMetadata.Add(pair.Key, pair.Value);
            }

            _objSmallMetadata = null;
            _objBigCache.Add(oid, new WeakReference(obj));
            _objBigMetadata.Add(obj, metadata);
            return;
        }

        _objSmallCache.Add(oid, obj);
        _objSmallMetadata!.Add(obj, metadata);
    }

    void CompactObjCacheIfNeeded()
    {
        if (_objBigCache == null) return;
        var gcIndex = GC.CollectionCount(0);
        if (_lastGCIndex == gcIndex) return;
        _lastGCIndex = gcIndex;
        CompactObjCache();
    }

    void CompactObjCache()
    {
        var toRemove = new StructList<ulong>();
        foreach (var pair in _objBigCache!)
        {
            if (!pair.Value.IsAlive)
            {
                toRemove.Add(pair.Key);
            }
        }

        foreach (var k in toRemove)
        {
            _objBigCache.Remove(k);
        }
    }

    void ReadObjStart(ulong oid, out TableInfo tableInfo, [UnscopedRef] out SpanReader reader)
    {
        reader = new(_keyValueTr!.GetValue());
        var tableId = reader.ReadVUInt32();
        tableInfo = _owner.TablesInfo.FindById(tableId)!;
        if (tableInfo == null)
        {
            _owner.ActualOptions.ThrowBTDBException($"Unknown TypeId {tableId} of Oid {oid}");
        }

        EnsureClientTypeNotNull(tableInfo!);
    }

    public object? Get(ulong oid)
    {
        var o = GetObjFromObjCacheByOid(oid);
        if (o != null)
        {
            return o;
        }

        return GetDirectlyFromStorage(oid);
    }

    object? GetDirectlyFromStorage(ulong oid)
    {
        if (!_keyValueTr!.FindExactKey(BuildKeyFromOidWithAllObjectsPrefix(oid)))
        {
            return null;
        }

        ReadObjStart(oid, out var tableInfo, out var reader);
        return ReadObjFinish(oid, tableInfo, ref reader);
    }

    public ulong GetOid(object? obj)
    {
        if (obj == null) return 0;
        DBObjectMetadata meta;
        if (_objSmallMetadata != null)
        {
            return !_objSmallMetadata.TryGetValue(obj, out meta) ? 0 : meta.Id;
        }

        if (_objBigMetadata != null)
        {
            return !_objBigMetadata.TryGetValue(obj, out meta) ? 0 : meta.Id;
        }

        return 0;
    }

    public KeyValuePair<uint, uint> GetStorageSize(ulong oid)
    {
        if (!_keyValueTr!.FindExactKey(BuildKeyFromOidWithAllObjectsPrefix(oid)))
        {
            return new KeyValuePair<uint, uint>(0, 0);
        }

        var res = _keyValueTr.GetStorageSizeOfCurrentKey();
        return res;
    }

    public IEnumerable<Type> EnumerateSingletonTypes()
    {
        foreach (var tableInfo in _owner.TablesInfo.EnumerateTableInfos().ToArray())
        {
            var oid = tableInfo.LazySingletonOid;
            if (oid == 0) continue;
            // Ignore impossibility to create type
            if (TryToEnsureClientTypeNotNull(tableInfo))
            {
                yield return tableInfo.ClientType!;
            }
        }
    }

    public IEnumerable<Type> EnumerateRelationTypes()
    {
        foreach (var relationInfo in _owner.RelationsInfo.EnumerateRelationInfos())
        {
            var oid = relationInfo.Id;
            if (oid == 0) continue;

            var type = relationInfo.InterfaceType;

            if (type != null)
                yield return type;
        }
    }

    IRelation? _relationInstances;
    Dictionary<Type, IRelation>? _relationsInstanceCache;
    IInternalSerializationCallbacks? _serializationCallbacks;
    const int LinearSearchLimit = 4;

    public IRelation GetRelation(Type type)
    {
        if (_relationsInstanceCache != null)
        {
            if (_relationsInstanceCache.TryGetValue(type, out var res))
                return res;
        }
        else
        {
            var top = _relationInstances;
            var complexity = 0;
            while (top != null)
            {
                if (top.BtdbInternalGetRelationInterfaceType() == type)
                {
                    if (complexity >= LinearSearchLimit)
                    {
                        var cache = _relationsInstanceCache = new Dictionary<Type, IRelation>(complexity);
                        var t = _relationInstances;
                        while (t != null)
                        {
                            cache.Add(t.BtdbInternalGetRelationInterfaceType(), t);
                            t = t.BtdbInternalNextInChain;
                        }
                    }

                    return top;
                }

                top = top.BtdbInternalNextInChain;
                complexity++;
            }
        }

        while (true)
        {
            if (_owner.RelationFactories.TryGetValue(type, out var factory))
            {
                var res = (IRelation)factory(this);
                res.BtdbInternalNextInChain = _relationInstances;
                _relationInstances = res;
                return res;
            }

            CreateAndRegisterRelationFactory(type);
        }
    }

    void CreateAndRegisterRelationFactory(Type type)
    {
        if (!_owner.AllowAutoRegistrationOfRelations)
            _owner.ActualOptions.ThrowBTDBException("AutoRegistration of " + type.ToSimpleName() + " is forbidden");

        var spec = type.SpecializationOf(typeof(ICovariantRelation<>));
        if (spec == null)
            _owner.ActualOptions.ThrowBTDBException("Relation type " + type.ToSimpleName() +
                                    " must implement ICovariantRelation<>");
        if (!spec.GenericTypeArguments[0].IsClass)
            _owner.ActualOptions.ThrowBTDBException("Relation type " + type.ToSimpleName() + " does not have item as class");
        var name = type.GetCustomAttribute<PersistedNameAttribute>() is { } persistedNameAttribute
            ? persistedNameAttribute.Name
            : type.ToSimpleName();
        if (!_keyValueTr!.IsReadOnly())
        {
            _owner.RegisterCustomRelation(type, InitRelation(name, type));
        }
        else
        {
            using var tr = _owner.StartWritingTransaction().Result;
            _owner.RegisterCustomRelation(type, ((ObjectDBTransaction)tr).InitRelation(name, type));
            tr.Commit();
        }
    }

    public object Singleton(Type type)
    {
        var tableInfo = AutoRegisterType(type, true);
        tableInfo.EnsureClientTypeVersion();
        var oid = (ulong)tableInfo.SingletonOid;
        var obj = GetObjFromObjCacheByOid(oid);
        if (obj == null)
        {
            var content = tableInfo.SingletonContent(_transactionNumber);
            if (content == null)
            {
                if (_keyValueTr!.FindExactKey(BuildKeyFromOidWithAllObjectsPrefix(oid)))
                {
                    content = _keyValueTr.GetValue().ToArray();
                    tableInfo.CacheSingletonContent(_transactionNumber, content);
                }
            }

            if (content != null)
            {
                var reader = new SpanReader(content.AsSpan());
                reader.SkipVUInt32();
                obj = ReadObjFinish(oid, tableInfo, ref reader);
            }
        }

        if (obj != null)
        {
            if (!type.IsInstanceOfType(obj))
            {
                _owner.ActualOptions.ThrowBTDBException($"Internal error oid {oid} does not belong to {tableInfo.Name}");
            }

            return obj;
        }

        _updatedTables?.Remove(tableInfo);
        var metadata = new DBObjectMetadata(oid, DBObjectState.Dirty);
        obj = tableInfo.Creator(this, metadata);
        tableInfo.Initializer(this, metadata, obj);
        AddToObjCache(oid, obj, metadata);
        AddToDirtySet(oid, obj);
        return obj;
    }

    void AddToDirtySet(ulong oid, object obj)
    {
        if (_dirtyObjSet == null) _dirtyObjSet = new Dictionary<ulong, object>();
        _dirtyObjSet.Add(oid, obj);
    }

    public T Singleton<T>() where T : class
    {
        return (T)Singleton(typeof(T));
    }

    public object New(Type type)
    {
        var tableInfo = AutoRegisterType(type);
        tableInfo.EnsureClientTypeVersion();
        const ulong oid = 0ul;
        var metadata = new DBObjectMetadata(oid, DBObjectState.Dirty);
        var obj = tableInfo.Creator(this, metadata);
        tableInfo.Initializer(this, metadata, obj);
        return obj;
    }

    public T New<T>() where T : class
    {
        return (T)New(typeof(T));
    }

    public ulong Store(object @object)
    {
        if (@object is IIndirect indirect)
        {
            if (GetObjFromObjCacheByOid(indirect.Oid) == null)
                return indirect.Oid;
            @object = indirect.ValueAsObject;
        }

        var ti = AutoRegisterType(@object.GetType());
        ti.EnsureClientTypeVersion();
        DBObjectMetadata metadata;
        if (_objSmallMetadata != null)
        {
            if (_objSmallMetadata.TryGetValue(@object, out metadata))
            {
                if (metadata.Id == 0)
                {
                    metadata.Id = _owner.AllocateNewOid();
                    _objSmallCache!.Add(metadata.Id, @object);
                }

                if (metadata.State != DBObjectState.Dirty)
                {
                    metadata.State = DBObjectState.Dirty;
                    AddToDirtySet(metadata.Id, @object);
                }

                return metadata.Id;
            }
        }
        else if (_objBigMetadata != null)
        {
            if (_objBigMetadata.TryGetValue(@object, out metadata))
            {
                if (metadata.Id == 0)
                {
                    metadata.Id = _owner.AllocateNewOid();
                    CompactObjCacheIfNeeded();
                    _objBigCache!.Add(metadata.Id, new WeakReference(@object));
                }

                if (metadata.State != DBObjectState.Dirty)
                {
                    metadata.State = DBObjectState.Dirty;
                    AddToDirtySet(metadata.Id, @object);
                }

                return metadata.Id;
            }
        }

        return RegisterNewObject(@object);
    }

    public ulong StoreAndFlush(object @object)
    {
        if (@object is IIndirect indirect)
        {
            if (GetObjFromObjCacheByOid(indirect.Oid) == null)
                return indirect.Oid;
            @object = indirect.ValueAsObject;
        }

        var ti = AutoRegisterType(@object.GetType());
        ti.EnsureClientTypeVersion();
        DBObjectMetadata metadata;
        if (_objSmallMetadata != null)
        {
            if (_objSmallMetadata.TryGetValue(@object, out metadata))
            {
                if (metadata.Id == 0)
                {
                    metadata.Id = _owner.AllocateNewOid();
                    _objSmallCache!.Add(metadata.Id, @object);
                }

                StoreObject(@object);
                metadata.State = DBObjectState.Read;
                return metadata.Id;
            }
        }
        else if (_objBigMetadata != null)
        {
            if (_objBigMetadata.TryGetValue(@object, out metadata))
            {
                if (metadata.Id == 0)
                {
                    metadata.Id = _owner.AllocateNewOid();
                    CompactObjCacheIfNeeded();
                    _objBigCache!.Add(metadata.Id, new WeakReference(@object));
                }

                StoreObject(@object);
                metadata.State = DBObjectState.Read;
                return metadata.Id;
            }
        }

        var id = _owner.AllocateNewOid();
        AddToObjCache(id, @object, new DBObjectMetadata(id, DBObjectState.Read));
        StoreObject(@object);
        return id;
    }

    public ulong StoreIfNotInlined(object @object, bool autoRegister, bool forceInline)
    {
        TableInfo ti;
        if (autoRegister)
        {
            ti = AutoRegisterType(@object.GetType());
        }
        else
        {
            ti = GetTableInfoFromType(@object.GetType());
            if (ti == null) return ulong.MaxValue;
        }

        ti.EnsureClientTypeVersion();
        DBObjectMetadata metadata;
        if (_objSmallMetadata != null)
        {
            if (_objSmallMetadata.TryGetValue(@object, out metadata))
            {
                if (metadata.Id == 0 || metadata.State == DBObjectState.Deleted) return 0;
                if (forceInline)
                {
                    Delete(metadata.Id);
                    return ulong.MaxValue;
                }

                return metadata.Id;
            }
        }
        else if (_objBigMetadata != null)
        {
            if (_objBigMetadata.TryGetValue(@object, out metadata))
            {
                if (metadata.Id == 0 || metadata.State == DBObjectState.Deleted) return 0;
                if (forceInline)
                {
                    Delete(metadata.Id);
                    return ulong.MaxValue;
                }

                return metadata.Id;
            }
        }

        return forceInline ? ulong.MaxValue : RegisterNewObject(@object);
    }

    ulong RegisterNewObject(object obj)
    {
        var id = _owner.AllocateNewOid();
        AddToObjCache(id, obj, new DBObjectMetadata(id, DBObjectState.Dirty));
        AddToDirtySet(id, obj);
        return id;
    }

    void EnsureClientTypeNotNull(TableInfo tableInfo)
    {
        if (!TryToEnsureClientTypeNotNull(tableInfo))
        {
            _owner.ActualOptions.ThrowBTDBException($"Type {tableInfo.Name} is not registered.");
        }
    }

    bool TryToEnsureClientTypeNotNull(TableInfo tableInfo)
    {
        if (tableInfo.ClientType == null)
        {
            var typeByName = _owner.Type2NameRegistry.FindTypeByName(tableInfo.Name);
            if (typeByName != null)
            {
                tableInfo.ClientType = typeByName;
            }
            else
            {
                return false;
            }
        }

        return true;
    }

    [SkipLocalsInit]
    ulong ReadOidFromCurrentKeyInTransaction()
    {
        Span<byte> buffer = stackalloc byte[16];
        return PackUnpack.UnpackVUInt(_keyValueTr!.GetKey(ref MemoryMarshal.GetReference(buffer), buffer.Length).Slice(1));
    }

    internal static byte[] BuildKeyFromOidWithAllObjectsPrefix(ulong oid)
    {
        var len = PackUnpack.LengthVUInt(oid);
        var key = new byte[ObjectDB.AllObjectsPrefixLen + len];
        key[0] = ObjectDB.AllObjectsPrefixByte;
        PackUnpack.UnsafePackVUInt(ref key[ObjectDB.AllObjectsPrefixLen], oid, len);
        return key;
    }

    internal static byte[] BuildKeyFromOid(in ReadOnlySpan<byte> prefix, ulong oid)
    {
        var len = PackUnpack.LengthVUInt(oid);
        var key = new byte[prefix.Length + len];
        Unsafe.CopyBlockUnaligned(ref MemoryMarshal.GetReference(key.AsSpan()),
            ref MemoryMarshal.GetReference(prefix), (uint)prefix.Length);
        PackUnpack.UnsafePackVUInt(
            ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(key.AsSpan()), (IntPtr)prefix.Length), oid,
            len);
        return key;
    }

    TableInfo AutoRegisterType(Type type, bool forceAutoRegistration = false)
    {
        var ti = _owner.TablesInfo.FindByType(type);
        if (ti != null) return ti;
        if (type.InheritsOrImplements(typeof(IEnumerable<>)) || !(type.IsClass || type.IsInterface) || type.IsDelegate() || type == typeof(string))
        {
            throw new InvalidOperationException("Cannot store " + type.ToSimpleName() +
                                                " type to DB directly.");
        }

        var name = _owner.Type2NameRegistry.FindNameByType(type);
        if (name == null)
        {
            if (!_owner.AutoRegisterTypes && !forceAutoRegistration)
            {
                _owner.ActualOptions.ThrowBTDBException($"Type {type.ToSimpleName()} is not registered.");
            }

            name = _owner.RegisterType(type, manualRegistration: false);
        }

        ti = _owner.TablesInfo.LinkType2Name(type, name);

        return ti;
    }

    TableInfo? GetTableInfoFromType(Type type)
    {
        var ti = _owner.TablesInfo.FindByType(type);
        if (ti == null)
        {
            var name = _owner.Type2NameRegistry.FindNameByType(type);
            if (name == null) return null;
            ti = _owner.TablesInfo.LinkType2Name(type, name);
        }

        return ti;
    }

    public void Delete(object @object)
    {
        if (@object == null) throw new ArgumentNullException(nameof(@object));
        if (@object is IIndirect indirect)
        {
            if (indirect.Oid != 0)
                Delete(indirect.Oid);
            else if (indirect.ValueAsObject != null)
                Delete(indirect.ValueAsObject);
            return;
        }

        var tableInfo = AutoRegisterType(@object.GetType());
        DBObjectMetadata metadata;
        if (_objSmallMetadata != null)
        {
            if (!_objSmallMetadata.TryGetValue(@object, out metadata))
            {
                _objSmallMetadata.Add(@object, new DBObjectMetadata(0, DBObjectState.Deleted));
                return;
            }
        }
        else if (_objBigMetadata != null)
        {
            if (!_objBigMetadata.TryGetValue(@object, out metadata))
            {
                _objBigMetadata.Add(@object, new DBObjectMetadata(0, DBObjectState.Deleted));
                return;
            }
        }
        else return;

        if (metadata.Id == 0 || metadata.State == DBObjectState.Deleted) return;
        metadata.State = DBObjectState.Deleted;
        _keyValueTr!.EraseCurrent(BuildKeyFromOidWithAllObjectsPrefix(metadata.Id));
        tableInfo.CacheSingletonContent(_transactionNumber + 1, null);
        if (_objSmallCache != null)
        {
            _objSmallCache.Remove(metadata.Id);
        }
        else
        {
            _objBigCache?.Remove(metadata.Id);
        }

        _dirtyObjSet?.Remove(metadata.Id);
    }

    public void Delete(ulong oid)
    {
        object obj = null;
        if (_objSmallCache != null)
        {
            if (_objSmallCache.TryGetValue(oid, out obj))
            {
                _objSmallCache.Remove(oid);
            }
        }
        else if (_objBigCache != null)
        {
            if (_objBigCache.TryGetValue(oid, out var weakObj))
            {
                obj = weakObj.Target;
                _objBigCache.Remove(oid);
            }
        }

        _dirtyObjSet?.Remove(oid);
        _keyValueTr!.EraseCurrent(BuildKeyFromOidWithAllObjectsPrefix(oid));
        if (obj == null) return;
        DBObjectMetadata metadata = null;
        if (_objSmallMetadata != null)
        {
            if (!_objSmallMetadata.TryGetValue(obj, out metadata))
            {
                return;
            }
        }
        else if (_objBigMetadata != null)
        {
            if (!_objBigMetadata.TryGetValue(obj, out metadata))
            {
                return;
            }
        }

        if (metadata == null) return;
        metadata.State = DBObjectState.Deleted;
    }

    public void DeleteAll<T>() where T : class
    {
        DeleteAll(typeof(T));
    }

    public void DeleteAll(Type type)
    {
        foreach (var o in Enumerate(type))
        {
            Delete(o);
        }
    }

    public ulong GetCommitUlong() => _keyValueTr!.GetCommitUlong();
    public void SetCommitUlong(ulong value) => _keyValueTr!.SetCommitUlong(value);

    public void NextCommitTemporaryCloseTransactionLog()
    {
        _keyValueTr!.NextCommitTemporaryCloseTransactionLog();
    }

    public void Commit()
    {
        try
        {
            while (_dirtyObjSet != null)
            {
                var curObjsToStore = _dirtyObjSet;
                _dirtyObjSet = null;
                foreach (var o in curObjsToStore)
                {
                    StoreObject(o.Value);
                }
            }

            _owner.CommitLastObjIdAndDictId(_keyValueTr!);
            _keyValueTr.Commit();
            if (_updatedTables != null)
                foreach (var updatedTable in _updatedTables)
                {
                    updatedTable.LastPersistedVersion = updatedTable.ClientTypeVersion;
                    updatedTable.ResetNeedStoreSingletonOid();
                }
        }
        finally
        {
            Dispose();
        }
    }

    void StoreObject(object o)
    {
        var type = o.GetType();
        if (!type.IsClass) _owner.ActualOptions.ThrowBTDBException("You can store only classes, not " + type.ToSimpleName());
        var tableInfo = _owner.TablesInfo.FindByType(type);
        IfNeededPersistTableInfo(tableInfo);
        DBObjectMetadata metadata = null;
        if (_objSmallMetadata != null)
        {
            _objSmallMetadata.TryGetValue(o, out metadata);
        }
        else
        {
            _objBigMetadata?.TryGetValue(o, out metadata);
        }

        if (metadata == null) _owner.ActualOptions.ThrowBTDBException("Metadata for object not found");
        if (metadata.State == DBObjectState.Deleted) return;
        var writer = new SpanWriter();
        writer.WriteVUInt32(tableInfo.Id);
        writer.WriteVUInt32(tableInfo.ClientTypeVersion);
        tableInfo.Saver(this, metadata, ref writer, o);
        if (_serializationCallbacks != null)
        {
            _serializationCallbacks.CreateKeyValue(BuildKeyFromOidWithAllObjectsPrefix(metadata.Id), writer.GetSpan());
        }
        if (tableInfo.IsSingletonOid(metadata.Id))
        {
            tableInfo.CacheSingletonContent(_transactionNumber + 1, null);
        }
        _keyValueTr!.CreateOrUpdateKeyValue(BuildKeyFromOidWithAllObjectsPrefix(metadata.Id), writer.GetSpan());
    }

    [SkipLocalsInit]
    void SerializeTableInfo(TableInfo tableInfo)
    {
        if (tableInfo.LastPersistedVersion != tableInfo.ClientTypeVersion)
        {
            Span<byte> buf = stackalloc byte[512];
            var writer = new SpanWriter(buf);

            if (tableInfo.LastPersistedVersion <= 0)
            {
                var keyTableId = BuildKeyFromOid(ObjectDB.TableNamesPrefix, tableInfo.Id);
                writer.WriteString(tableInfo.Name);
                _serializationCallbacks!.MetadataCreateKeyValue(keyTableId, writer.GetSpanAndReset());
            }

            var tableVersionInfo = tableInfo.ClientTableVersionInfo;
            tableVersionInfo!.Save(ref writer);
            _serializationCallbacks!.MetadataCreateKeyValue(
                TableInfo.BuildKeyForTableVersions(tableInfo.Id, tableInfo.ClientTypeVersion),
                writer.GetSpan());
        }

        if (tableInfo.NeedStoreSingletonOid)
        {
            _serializationCallbacks!.MetadataCreateKeyValue(BuildKeyFromOid(ObjectDB.TableSingletonsPrefix, tableInfo.Id),
                BuildKeyFromOid(new ReadOnlySpan<byte>(), (ulong)tableInfo.SingletonOid));
        }
    }

    [SkipLocalsInit]
    void PersistTableInfo(TableInfo tableInfo)
    {
        if (tableInfo.LastPersistedVersion != tableInfo.ClientTypeVersion)
        {
            if (tableInfo.LastPersistedVersion <= 0)
            {
                var keyTableId = BuildKeyFromOid(ObjectDB.TableNamesPrefix, tableInfo.Id);
                if (!_keyValueTr!.FindExactKey(keyTableId))
                {
                    Span<byte> buf = stackalloc byte[128];
                    var writer = new SpanWriter(buf);
                    writer.WriteString(tableInfo.Name);
                    _keyValueTr.CreateOrUpdateKeyValue(keyTableId, writer.GetSpan());
                }
            }

            if (!_keyValueTr!.FindExactKey(
                TableInfo.BuildKeyForTableVersions(tableInfo.Id, tableInfo.ClientTypeVersion)))
            {
                var tableVersionInfo = tableInfo.ClientTableVersionInfo;
                var writer = new SpanWriter();
                tableVersionInfo!.Save(ref writer);
                _keyValueTr.CreateOrUpdateKeyValue(
                    TableInfo.BuildKeyForTableVersions(tableInfo.Id, tableInfo.ClientTypeVersion),
                    writer.GetSpan());
            }
        }

        if (tableInfo.NeedStoreSingletonOid)
        {
            _keyValueTr!.CreateOrUpdateKeyValue(BuildKeyFromOid(ObjectDB.TableSingletonsPrefix, tableInfo.Id),
                BuildKeyFromOid(new ReadOnlySpan<byte>(), (ulong)tableInfo.SingletonOid));
        }
    }

    public Func<IObjectDBTransaction, T> InitRelation<T>(string relationName) where T : class, IRelation
    {
        var interfaceType = typeof(T);
        return Unsafe.As<Func<IObjectDBTransaction, T>>(InitRelation(relationName, interfaceType));
    }

    Func<IObjectDBTransaction, IRelation> InitRelation(string relationName, Type interfaceType)
    {
        var builder = RelationBuilder.GetFromCache(interfaceType, _owner.RelationInfoResolver);
        var relationInfo = _owner.RelationsInfo.CreateByName(this, relationName, interfaceType, builder);
        var factory = (Func<IObjectDBTransaction, IRelation>)builder.DelegateCreator.Create(relationInfo);
        if (relationInfo.LastPersistedVersion == 0)
        {
            var upgrader =
                Owner.ActualOptions.Container?.ResolveOptional(
                    typeof(IRelationOnCreate<>).MakeGenericType(interfaceType));
            if (upgrader != null)
                upgrader.GetType().GetMethod("OnCreate", BindingFlags.Instance | BindingFlags.Public,
                    new[] { typeof(IObjectDBTransaction), interfaceType })!.Invoke(upgrader, new object?[] {this, factory(this) });
        }
        return factory;
    }

    public void DeleteAllData()
    {
        // Resetting last oid is risky due to singletons. Resetting lastDictId is risky due to parallelism. So better to waste something.
        _keyValueTr!.EraseAll(ObjectDB.AllObjectsPrefix);
        _keyValueTr.EraseAll(ObjectDB.AllDictionariesPrefix);
        _keyValueTr.EraseAll(ObjectDB.AllRelationsPKPrefix);
        _keyValueTr.EraseAll(ObjectDB.AllRelationsSKPrefix);
    }
}
