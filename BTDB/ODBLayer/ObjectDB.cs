﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.IOC;
using BTDB.KVDBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public class ObjectDB : IObjectDB
{
    IKeyValueDB _keyValueDB;
    internal ISymmetricCipher SymmetricCipher;
    IType2NameRegistry _type2Name;
    IPolymorphicTypesRegistry _polymorphicTypesRegistry;
    TablesInfo _tablesInfo;
    RelationsInfo _relationsInfo;

    internal Dictionary<Type, Func<IObjectDBTransaction, IRelation>> RelationFactories =
        new Dictionary<Type, Func<IObjectDBTransaction, IRelation>>();

    bool _dispose;
    internal static readonly byte[] TableNamesPrefix = [0, 0]; // Index Table => Name
    const int TableNamesPrefixLen = 2;
    internal static readonly byte[] TableVersionsPrefix = [0, 1]; // Index Table, Version => TableVersionInfo
    const uint TableVersionsPrefixLen = 2;
    internal static readonly byte[] TableSingletonsPrefix = [0, 2]; // Index Table => singleton oid
    internal const uint TableSingletonsPrefixLen = 2;

    internal static readonly byte[]
        LastDictIdKey =
            [0, 3]; //  => Last Dictionary Index - only for backward compatibility newly stored in Ulong[1]

    internal static readonly byte[] RelationNamesPrefix = [0, 4]; // Name => Index Relation
    const int RelationNamesPrefixLen = 2;
    internal static readonly byte[] RelationVersionsPrefix = [0, 5]; // Index Relation, version number => metadata
    internal static readonly byte[] AllObjectsPrefix = [1]; // oid => Index Table, version number, Value
    internal const int AllObjectsPrefixLen = 1;
    internal static readonly byte[] AllDictionariesPrefix = [2]; // Index Dictionary, Key => Value
    internal const int AllDictionariesPrefixLen = 1;

    internal static readonly byte[]
        AllRelationsPKPrefix = [3]; // Index Relation, Primary Key => version number, Value (without primary key)

    internal static readonly byte[]
        AllRelationsSKPrefix =
        [
            4
        ]; // Index Relation, Secondary Key Index, Secondary Key, primary key fields not present in secondary key => {}

    internal const byte AllObjectsPrefixByte = 1;
    internal const byte AllDictionariesPrefixByte = 2;
    internal const byte AllRelationsPKPrefixByte = 3;
    internal const byte AllRelationsSKPrefixByte = 4;
    ITableInfoResolver _tableInfoResolver;
    IRelationInfoResolver _relationsInfoResolver;
    long _lastObjId;
    long _lastDictId;

    // ReSharper disable once NotNullMemberIsNotInitialized
    public ObjectDB()
    {
        FieldHandlerFactory = new DefaultODBFieldHandlerFactory(this);
        TypeConvertorGenerator = DefaultTypeConvertorGenerator.Instance;
        TypeConverterFactory = new DefaultTypeConverterFactory();
    }

    /// Use only for tests of upgrade compatibility in same process, never use in production code
    public static void ResetAllMetadataCaches()
    {
        RelationBuilder.Reset();
        ODBDictionaryConfiguration.Reset();
    }

    internal IType2NameRegistry Type2NameRegistry => _type2Name;

    public bool AutoRegisterTypes { get; set; }
    public bool AutoRegisterRelations { get; set; }
    public bool AutoSkipUnknownTypes { get; set; }
    public DBOptions ActualOptions { get; private set; }

    public TablesInfo TablesInfo => _tablesInfo;

    public RelationsInfo RelationsInfo => _relationsInfo;
    internal IRelationInfoResolver RelationInfoResolver => _relationsInfoResolver;

    public void Open(IKeyValueDB keyValueDB, bool dispose)
    {
        Open(keyValueDB, dispose, new DBOptions());
    }

    public void Open(IKeyValueDB keyValueDB, bool dispose, DBOptions options)
    {
        _keyValueDB = keyValueDB ?? throw new ArgumentNullException(nameof(keyValueDB));
        _dispose = dispose;
        _type2Name = options.CustomType2NameRegistry ?? new Type2NameRegistry();
        _polymorphicTypesRegistry = new PolymorphicTypesRegistry();
        AutoRegisterTypes = options.AutoRegisterType;
        AllowAutoRegistrationOfRelations = options.AutoRegisterRelations;
        AutoSkipUnknownTypes = options.AutoSkipUnknownTypes;
        ActualOptions = options;
        SymmetricCipher = options.SymmetricCipher ?? new InvalidSymmetricCipher();
        FieldHandlerLogger = options.FieldHandlerLogger;
        Logger = options.Logger ?? Logger;

        _tableInfoResolver = new TableInfoResolver(keyValueDB, this);
        _tablesInfo = new TablesInfo(_tableInfoResolver);
        _relationsInfoResolver = new RelationInfoResolver(this);
        _relationsInfo = new RelationsInfo(_relationsInfoResolver);

        using var tr = _keyValueDB.StartTransaction();
        _lastObjId = (long)tr.GetUlong(0);
        _lastDictId = (long)tr.GetUlong(1);
        Span<byte> stackBuffer = stackalloc byte[16];
        var buf = stackBuffer;
        if (_lastObjId == 0)
        {
            using var cursor = tr.CreateCursor();
            if (cursor.FindLastKey(AllObjectsPrefix))
            {
                _lastObjId = (long)PackUnpack.UnpackVUInt(cursor.GetKeySpan(ref buf)[AllObjectsPrefixLen..]);
            }
        }

        _tablesInfo.LoadTables(LoadTablesEnum(tr));
        _relationsInfo.LoadRelations(LoadRelationNamesEnum(tr));
        if (_lastDictId == 0)
        {
            using var cursor = tr.CreateCursor();
            if (cursor.FindExactKey(LastDictIdKey))
            {
                _lastDictId = (long)PackUnpack.UnpackVUInt(cursor.GetValueSpan(ref buf));
            }
        }
    }

    internal void CommitLastObjIdAndDictId(IKeyValueDBTransaction tr)
    {
        var lastOid = GetLastAllocatedOid();
        if (tr.GetUlong(0) < lastOid)
        {
            tr.SetUlong(0, lastOid);
        }

        var lastDistId = GetLastAllocatedDictId();
        if (tr.GetUlong(1) < lastDistId)
        {
            tr.SetUlong(1, lastDistId);
        }
    }

    internal static IEnumerable<KeyValuePair<uint, string>> LoadTablesEnum(IKeyValueDBTransaction tr)
    {
        using var cursor = tr.CreateCursor();
        while (cursor.FindNextKey(TableNamesPrefix))
        {
            yield return ReadTableNameFromCursor(cursor);
        }
    }

    [SkipLocalsInit]
    static unsafe KeyValuePair<uint, string> ReadTableNameFromCursor(IKeyValueDBCursor cursor)
    {
        Span<byte> buf = stackalloc byte[4096];
        var valueSpan = cursor.GetValueSpan(ref buf);
        string name;
        fixed (void* ptr = valueSpan)
        {
            name = new MemReader(ptr, valueSpan.Length).ReadString()!;
        }

        var tableId = (uint)PackUnpack.UnpackVUInt(cursor.GetKeySpan(ref buf)[TableNamesPrefixLen..]);
        return new(tableId, name);
    }

    internal static IEnumerable<KeyValuePair<uint, string>> LoadRelationNamesEnum(IKeyValueDBTransaction tr)
    {
        using var cursor = tr.CreateCursor();
        while (cursor.FindNextKey(RelationNamesPrefix))
        {
            [SkipLocalsInit]
            unsafe string ReadName()
            {
                Span<byte> buf = stackalloc byte[4096];
                var bufName = cursor.GetKeySpan(ref buf)[RelationNamesPrefixLen..];
                fixed (void* bufPtr = bufName)
                {
                    return new MemReader(bufPtr, bufName.Length).ReadString()!;
                }
            }

            [SkipLocalsInit]
            unsafe uint ReadId()
            {
                Span<byte> buf = stackalloc byte[16];
                var bufId = cursor.GetValueSpan(ref buf);
                return (uint)PackUnpack.UnpackVUInt(bufId);
            }

            yield return new(ReadId(), ReadName());
        }
    }

    public IObjectDBTransaction StartTransaction()
    {
        return new ObjectDBTransaction(this, _keyValueDB.StartTransaction(), false);
    }

    public IObjectDBTransaction StartReadOnlyTransaction()
    {
        return new ObjectDBTransaction(this, _keyValueDB.StartReadOnlyTransaction(), true);
    }

    public async ValueTask<IObjectDBTransaction> StartWritingTransaction()
    {
        return new ObjectDBTransaction(this, await _keyValueDB.StartWritingTransaction(), false);
    }

    public string RegisterType(Type type)
    {
        return RegisterType(type, true);
    }

    public string RegisterType(Type type, string withName)
    {
        return RegisterType(type, withName, true);
    }

    internal string RegisterType(Type type, bool manualRegistration)
    {
        if (type == null) throw new ArgumentNullException(nameof(type));
        var name = Type2NameRegistry.FindNameByType(type);
        if (name != null) return name;

        static string NiceName(Type type1)
        {
            var niceName = type1.Name;
            if (type1.IsInterface && niceName.StartsWith('I'))
                niceName = niceName[1..];

            if (!type1.IsGenericType) return niceName;
            var genericTypes = type1.GenericTypeArguments;
            niceName = $"{niceName.Split('`')[0]}<{string.Join(",", genericTypes.Select(NiceName))}>";
            return niceName;
        }

        name = NiceName(type);

        return RegisterType(type, name, manualRegistration);
    }

    string RegisterType(Type type, string asName, bool manualRegistration)
    {
        if (manualRegistration)
            _polymorphicTypesRegistry.RegisterPolymorphicType(type);
        return Type2NameRegistry.RegisterType(type, asName);
    }

    public IEnumerable<Type> GetPolymorphicTypes(Type baseType)
    {
        return _polymorphicTypesRegistry.GetPolymorphicTypes(baseType);
    }

    public Type? TypeByName(string name)
    {
        return Type2NameRegistry.FindTypeByName(name);
    }

    public IObjectDBLogger? Logger { get; set; }

    public ISymmetricCipher GetSymmetricCipher() => SymmetricCipher;

    public void RegisterCustomRelation(Type type, Func<IObjectDBTransaction, IRelation> factory)
    {
        while (true)
        {
            var currentRelationFactories = Volatile.Read(ref RelationFactories);
            if (currentRelationFactories!.ContainsKey(type)) return;
            var newRelationFactories =
                new Dictionary<Type, Func<IObjectDBTransaction, IRelation>>(currentRelationFactories)
                {
                    { type, factory }
                };
            if (Interlocked.CompareExchange(ref RelationFactories, newRelationFactories,
                    currentRelationFactories) == currentRelationFactories)
                return;
        }
    }

    public bool AllowAutoRegistrationOfRelations { get; set; } = true;

    public void Dispose()
    {
        if (_dispose)
        {
            _keyValueDB.Dispose();
            _dispose = false;
        }
    }

    class TableInfoResolver : ITableInfoResolver
    {
        readonly IKeyValueDB _keyValueDB;
        readonly ObjectDB _objectDB;

        internal TableInfoResolver(IKeyValueDB keyValueDB, ObjectDB objectDB)
        {
            _keyValueDB = keyValueDB;
            _objectDB = objectDB;
        }

        uint ITableInfoResolver.GetLastPersistedVersion(uint id)
        {
            using var tr = _keyValueDB.StartReadOnlyTransaction();
            using var cursor = tr.CreateCursor();
            var key = TableInfo.BuildKeyForTableVersions(id, 0);
            var ofs = PackUnpack.LengthVUInt(id);
            if (!cursor.FindLastKey(key.AsSpan()[..(int)(TableVersionsPrefixLen + ofs)]))
                return 0;
            Span<byte> buf = stackalloc byte[16];
            var key2 = cursor.GetKeySpan(ref buf)[(int)(TableVersionsPrefixLen + ofs)..];
            return checked((uint)PackUnpack.UnpackVUInt(key2));
        }

        [SkipLocalsInit]
        unsafe TableVersionInfo ITableInfoResolver.LoadTableVersionInfo(uint id, uint version, string tableName)
        {
            using var tr = _keyValueDB.StartReadOnlyTransaction();
            using var cursor = tr.CreateCursor();
            var key = TableInfo.BuildKeyForTableVersions(id, version);
            if (!cursor.FindExactKey(key))
                _objectDB.ActualOptions.ThrowBTDBException(
                    $"Missing TableVersionInfo Id: {id} Version: {version} TableName: {tableName}");
            Span<byte> buffer = stackalloc byte[4096];
            var valueSpan = cursor.GetValueSpan(ref buffer);
            fixed (void* ptr = valueSpan)
            {
                var reader = new MemReader(ptr, valueSpan.Length);
                return TableVersionInfo.Load(ref reader, _objectDB.FieldHandlerFactory, tableName);
            }
        }

        public long GetSingletonOid(uint id)
        {
            using var tr = _keyValueDB.StartTransaction();
            using var cursor = tr.CreateCursor();
            var len = PackUnpack.LengthVUInt(id);
            Span<byte> buf = stackalloc byte[16];
            Span<byte> key = buf[..(int)(TableSingletonsPrefixLen + len)];
            TableSingletonsPrefix.CopyTo(key);
            PackUnpack.UnsafePackVUInt(
                ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(key), (IntPtr)TableSingletonsPrefixLen),
                id, len);
            if (cursor.FindExactKey(key))
            {
                return (long)PackUnpack.UnpackVUInt(cursor.GetValueSpan(ref buf));
            }

            return 0;
        }

        public ulong AllocateNewOid()
        {
            return _objectDB.AllocateNewOid();
        }

        public IFieldHandlerFactory FieldHandlerFactory => _objectDB.FieldHandlerFactory;

        public ITypeConvertorGenerator TypeConvertorGenerator => _objectDB.TypeConvertorGenerator;

        public ITypeConverterFactory TypeConverterFactory => _objectDB.TypeConverterFactory;

        public IContainer? Container => _objectDB.ActualOptions.Container;
        public DBOptions ActualOptions => _objectDB.ActualOptions;
        public IFieldHandlerLogger? FieldHandlerLogger => _objectDB.FieldHandlerLogger;
    }

    public ITypeConvertorGenerator TypeConvertorGenerator { get; set; }

    public ITypeConverterFactory TypeConverterFactory { get; set; }

    public IFieldHandlerFactory FieldHandlerFactory { get; set; }
    public IFieldHandlerLogger? FieldHandlerLogger { get; set; }

    internal ulong AllocateNewOid()
    {
        return (ulong)Interlocked.Increment(ref _lastObjId);
    }

    internal ulong GetLastAllocatedOid()
    {
        return (ulong)Interlocked.Read(ref _lastObjId);
    }

    internal ulong AllocateNewDictId()
    {
        return (ulong)Interlocked.Increment(ref _lastDictId) - 1; // It should start at 0
    }

    internal ulong GetLastAllocatedDictId()
    {
        return (ulong)Interlocked.Read(ref _lastDictId);
    }

    internal TableInfo? GetTableInfoFromType(Type type)
    {
        var ti = TablesInfo.FindByType(type);
        if (ti == null)
        {
            var name = Type2NameRegistry.FindNameByType(type);
            if (name == null) return null;
            ti = TablesInfo.LinkType2Name(type, name);
        }

        return ti;
    }
}
