using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.KVDBLayer.Interface;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer
{
    internal class TableInfo
    {
        readonly uint _id;
        readonly string _name;
        readonly ITableInfoResolver _tableInfoResolver;
        uint _clientTypeVersion;
        Type _clientType;
        Type _implType;
        readonly ConcurrentDictionary<uint, TableVersionInfo> _tableVersions = new ConcurrentDictionary<uint, TableVersionInfo>();
        Func<IObjectDBTransactionInternal, ulong, object> _inserter;
        Action<object> _saver;
        readonly ConcurrentDictionary<uint, Func<IObjectDBTransactionInternal, ulong, AbstractBufferedReader, object>> _loaders = new ConcurrentDictionary<uint, Func<IObjectDBTransactionInternal, ulong, AbstractBufferedReader, object>>();
        ulong? _singletonOid;
        readonly object _singletonLock = new object();

        internal TableInfo(uint id, string name, ITableInfoResolver tableInfoResolver)
        {
            _id = id;
            _name = name;
            _tableInfoResolver = tableInfoResolver;
        }

        internal uint Id
        {
            get { return _id; }
        }

        internal string Name
        {
            get { return _name; }
        }

        internal Type ClientType
        {
            get { return _clientType; }
            set
            {
                _clientType = value;
                ClientTypeVersion = 0;
            }
        }

        internal TableVersionInfo ClientTableVersionInfo
        {
            get
            {
                TableVersionInfo tvi;
                if (_tableVersions.TryGetValue(_clientTypeVersion, out tvi)) return tvi;
                return null;
            }
        }

        internal uint LastPersistedVersion { get; set; }

        internal uint ClientTypeVersion
        {
            get { return _clientTypeVersion; }
            private set { _clientTypeVersion = value; }
        }

        void EnsureImplType()
        {
            if (_implType != null) return;
            System.Threading.Interlocked.CompareExchange(ref _implType, CreateImplType(Id, Name, ClientType, ClientTypeVersion, _tableVersions[ClientTypeVersion]), null);
        }

        static Type CreateImplType(uint id, string name, Type clientType, uint clientTypeVersion, TableVersionInfo tableVersionInfo)
        {
            AssemblyBuilder ab = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name + "Asm"), AssemblyBuilderAccess.RunAndCollect);
            ModuleBuilder mb = ab.DefineDynamicModule(name + "Asm.dll", true);
            var symbolDocumentWriter = mb.DefineDocument("just_dynamic_" + name, Guid.Empty, Guid.Empty, Guid.Empty);
            TypeBuilder tb = mb.DefineType(name + "Impl", TypeAttributes.Public, typeof(object), new[] { clientType, typeof(IDBObject) });
            var properties = clientType.GetProperties();
            var oidFieldBuilder = tb.DefineField("Oid", typeof(ulong), FieldAttributes.InitOnly | FieldAttributes.Public);
            var trFieldBuilder = tb.DefineField("ObjectDBTransaction", typeof(IObjectDBTransactionInternal),
                                                FieldAttributes.InitOnly | FieldAttributes.Public);
            var deletedFieldBuilder = tb.DefineField("_deleted", typeof(bool), FieldAttributes.Family);
            var propInfo = typeof(IDBObject).GetProperty("TableName");
            var getMethodBuilder = tb.DefineMethod("get_" + propInfo.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, propInfo.PropertyType, Type.EmptyTypes);
            var ilGenerator = getMethodBuilder.GetILGenerator(symbolDocumentWriter, 16);
            ilGenerator.Ldstr(name).Ret();
            tb.DefineMethodOverride(getMethodBuilder, propInfo.GetGetMethod());
            var propertyBuilder = tb.DefineProperty(propInfo.Name, PropertyAttributes.None, propInfo.PropertyType, Type.EmptyTypes);
            propertyBuilder.SetGetMethod(getMethodBuilder);
            propInfo = typeof(IDBObject).GetProperty("TableId");
            getMethodBuilder = tb.DefineMethod("get_" + propInfo.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, propInfo.PropertyType, Type.EmptyTypes);
            ilGenerator = getMethodBuilder.GetILGenerator(symbolDocumentWriter, 16);
            ilGenerator.LdcI4((int)id).Ret();
            tb.DefineMethodOverride(getMethodBuilder, propInfo.GetGetMethod());
            propertyBuilder = tb.DefineProperty(propInfo.Name, PropertyAttributes.None, propInfo.PropertyType, Type.EmptyTypes);
            propertyBuilder.SetGetMethod(getMethodBuilder);
            propInfo = typeof(IDBObject).GetProperty("Oid");
            getMethodBuilder = tb.DefineMethod("get_" + propInfo.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, propInfo.PropertyType, Type.EmptyTypes);
            ilGenerator = getMethodBuilder.GetILGenerator(symbolDocumentWriter, 16);
            ilGenerator.Ldarg(0).Ldfld(oidFieldBuilder).Ret();
            tb.DefineMethodOverride(getMethodBuilder, propInfo.GetGetMethod());
            propertyBuilder = tb.DefineProperty(propInfo.Name, PropertyAttributes.None, propInfo.PropertyType, Type.EmptyTypes);
            propertyBuilder.SetGetMethod(getMethodBuilder);
            propInfo = typeof(IDBObject).GetProperty("Deleted");
            getMethodBuilder = tb.DefineMethod("get_" + propInfo.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, propInfo.PropertyType, Type.EmptyTypes);
            ilGenerator = getMethodBuilder.GetILGenerator(symbolDocumentWriter, 16);
            ilGenerator.Ldarg(0).Ldfld(deletedFieldBuilder).Ret();
            tb.DefineMethodOverride(getMethodBuilder, propInfo.GetGetMethod());
            propertyBuilder = tb.DefineProperty(propInfo.Name, PropertyAttributes.None, propInfo.PropertyType, Type.EmptyTypes);
            propertyBuilder.SetGetMethod(getMethodBuilder);
            var constructorBuilder = tb.DefineConstructor(MethodAttributes.Family, CallingConventions.Standard,
                                                          new[] { typeof(ulong), typeof(IObjectDBTransactionInternal) });
            var ilg = constructorBuilder.GetILGenerator();
            ilg.MarkSequencePoint(symbolDocumentWriter, 1, 1, 1, 1);
            ilg
                .Ldarg(0)
                .Call(() => new object())
                .Ldarg(0)
                .Ldarg(1)
                .Stfld(oidFieldBuilder)
                .Ldarg(0)
                .Ldarg(2)
                .Stfld(trFieldBuilder)
                .Ret();
            var metbCi = tb.DefineMethod("CreateInstance",
                MethodAttributes.Public | MethodAttributes.Static, typeof(object), new[] { typeof(ulong), typeof(IObjectDBTransactionInternal) });
            ilg = metbCi.GetILGenerator(symbolDocumentWriter);
            ilg
                .Ldarg(0)
                .Ldarg(1)
                .Newobj(constructorBuilder)
                .Ret();
            var metb = tb.DefineMethod("Inserter",
                            MethodAttributes.Public | MethodAttributes.Static, typeof(object), new[] { typeof(IObjectDBTransactionInternal), typeof(ulong) });
            ilg = metb.GetILGenerator(symbolDocumentWriter);
            ilg.DeclareLocal(typeof(object));
            ilg
                .Ldarg(1)
                .Ldarg(0)
                .Call(metbCi)
                .Stloc(0)
                .Ldarg(0)
                .Ldarg(1)
                .Ldloc(0)
                .Callvirt(() => ((IObjectDBTransactionInternal)null).RegisterNewObject(0, null))
                .Ldloc(0)
                .Ret();
            metb = tb.DefineMethod("Delete",
                            MethodAttributes.Public | MethodAttributes.Virtual, typeof(void), Type.EmptyTypes);
            ilg = metb.GetILGenerator(symbolDocumentWriter);
            ilg
                .Ldarg(0)
                .LdcI4(1)
                .Stfld(deletedFieldBuilder)
                .Ldarg(0)
                .Ldfld(trFieldBuilder)
                .Ldarg(0)
                .Tail().Call(() => ((IObjectDBTransactionInternal)null).InternalDelete(null))
                .Ret();
            metb = tb.DefineMethod("Saver",
                MethodAttributes.Public | MethodAttributes.Static, typeof(void), new[] { typeof(object) });
            ilg = metb.GetILGenerator(symbolDocumentWriter);
            ilg.DeclareLocal(tb);
            ilg.DeclareLocal(typeof(AbstractBufferedWriter));
            var skipException = ilg.DefineLabel();
            ilg
                .Ldarg(0)
                .Isinst(tb)
                .Stloc(0)
                .Ldloc(0)
                .BrtrueS(skipException)
                .Ldstr("Type of object in Saver does not match")
                .Newobj(() => new BTDBException(null))
                .Throw()
                .Mark(skipException)
                .Ldloc(0)
                .Ldfld(trFieldBuilder)
                .Ldloc(0)
                .Ldfld(oidFieldBuilder)
                .Callvirt(() => ((IObjectDBTransactionInternal)null).PrepareToWriteObject(0))
                .Stloc(1)
                .Ldloc(1)
                .LdcI4((int)id)
                .Call(() => ((AbstractBufferedWriter)null).WriteVUInt32(0))
                .Ldloc(1)
                .LdcI4((int)clientTypeVersion)
                .Call(() => ((AbstractBufferedWriter)null).WriteVUInt32(0));
            var objHolder = new Dictionary<string, object>();
            for (int fieldIndex = 0; fieldIndex < tableVersionInfo.FieldCount; fieldIndex++)
            {
                var tableFieldInfo = tableVersionInfo[fieldIndex];
                var property = properties.First(pi => pi.Name == tableFieldInfo.Name);
                var fieldHandlerCreateImpl = new FieldHandlerCreateImpl
                    {
                        FieldName = tableFieldInfo.Name,
                        ImplType = tb,
                        SymbolDocWriter = symbolDocumentWriter,
                        ObjectStorage = objHolder,
                        Generator = null,
                        PropertyInfo = property,
                        FieldMidLevelDBTransaction = trFieldBuilder,
                        CallObjectModified = generator =>
                            {
                                generator.Ldarg(0);
                                generator.Ldfld(trFieldBuilder);
                                generator.Ldarg(0);
                                generator.Ldfld(oidFieldBuilder);
                                generator.Ldarg(0);
                                generator.Callvirt(() => ((IObjectDBTransactionInternal)null).ObjectModified(0, null));
                            }
                    };
                tableFieldInfo.Handler.CreateStorage(fieldHandlerCreateImpl);
                propertyBuilder = tb.DefineProperty(property.Name, PropertyAttributes.None, property.PropertyType, Type.EmptyTypes);
                if (property.CanRead)
                {
                    getMethodBuilder = tb.DefineMethod("get_" + property.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, property.PropertyType, Type.EmptyTypes);
                    ilGenerator = getMethodBuilder.GetILGenerator(symbolDocumentWriter);
                    ilGenerator
                        .Ldarg(0)
                        .Ldfld(trFieldBuilder)
                        .Ldarg(0)
                        .Callvirt(() => ((IObjectDBTransactionInternal)null).CheckPropertyOperationValidity(null));
                    fieldHandlerCreateImpl.Generator = ilGenerator;
                    tableFieldInfo.Handler.CreatePropertyGetter(fieldHandlerCreateImpl);
                    ilGenerator.Ret();
                    tb.DefineMethodOverride(getMethodBuilder, property.GetGetMethod());
                    propertyBuilder.SetGetMethod(getMethodBuilder);
                }
                if (property.CanWrite)
                {
                    var setMethodBuilder = tb.DefineMethod("set_" + property.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, typeof(void),
                                                           new[] { property.PropertyType });
                    ilGenerator = setMethodBuilder.GetILGenerator(symbolDocumentWriter);
                    ilGenerator
                        .Ldarg(0)
                        .Ldfld(trFieldBuilder)
                        .Ldarg(0)
                        .Callvirt(() => ((IObjectDBTransactionInternal)null).CheckPropertyOperationValidity(null));
                    fieldHandlerCreateImpl.Generator = ilGenerator;
                    tableFieldInfo.Handler.CreatePropertySetter(fieldHandlerCreateImpl);
                    ilGenerator.Ret();
                    propertyBuilder.SetSetMethod(setMethodBuilder);
                }
                fieldHandlerCreateImpl.Generator = ilg;
                tableFieldInfo.Handler.CreateSaver(fieldHandlerCreateImpl);
            }
            ilg.Ldloc(1);
            ilg.Castclass(typeof(IDisposable));
            ilg.Callvirt(() => ((IDisposable)null).Dispose());
            ilg.Ret();
            Type result = tb.CreateType();
            //ab.Save(name + "asm.dll");
            return result;
        }

        internal Func<IObjectDBTransactionInternal, ulong, object> Inserter
        {
            get
            {
                if (_inserter == null) CreateInserter();
                return _inserter;
            }
        }

        void CreateInserter()
        {
            EnsureImplType();
            var inserter = (Func<IObjectDBTransactionInternal, ulong, object>)Delegate.CreateDelegate(typeof(Func<IObjectDBTransactionInternal, ulong, object>), _implType.GetMethod("Inserter"));
            System.Threading.Interlocked.CompareExchange(ref _inserter, inserter, null);
        }

        internal Action<object> Saver
        {
            get
            {
                if (_saver == null) CreateSaver();
                return _saver;
            }
        }

        public ulong SingletonOid
        {
            get
            {
                if (_singletonOid.HasValue) return _singletonOid.Value;
                _singletonOid = _tableInfoResolver.GetSingletonOid(_id);
                return _singletonOid.Value;
            }
        }

        public object SingletonLock
        {
            get
            {
                return _singletonLock;
            }
        }

        void CreateSaver()
        {
            EnsureImplType();
            var saver = (Action<object>)Delegate.CreateDelegate(typeof(Action<object>), _implType.GetMethod("Saver"));
            System.Threading.Interlocked.CompareExchange(ref _saver, saver, null);
        }

        internal void EnsureClientTypeVersion()
        {
            if (ClientTypeVersion != 0) return;
            EnsureKnownLastPersistedVersion();
            var props = _clientType.GetProperties();
            var fields = new List<TableFieldInfo>(props.Length);
            foreach (var pi in props)
            {
                fields.Add(TableFieldInfo.Build(Name, pi, _tableInfoResolver.FieldHandlerFactory, ClientType));
            }
            var tvi = new TableVersionInfo(fields.ToArray());
            if (LastPersistedVersion == 0)
            {
                _tableVersions.TryAdd(1, tvi);
                ClientTypeVersion = 1;
            }
            else
            {
                var last = _tableVersions.GetOrAdd(LastPersistedVersion, v => _tableInfoResolver.LoadTableVersionInfo(_id, v, Name));
                if (TableVersionInfo.Equal(last, tvi))
                {
                    ClientTypeVersion = LastPersistedVersion;
                }
                else
                {
                    _tableVersions.TryAdd(LastPersistedVersion + 1, tvi);
                    ClientTypeVersion = LastPersistedVersion + 1;
                }
            }
        }

        void EnsureKnownLastPersistedVersion()
        {
            if (LastPersistedVersion != 0) return;
            LastPersistedVersion = _tableInfoResolver.GetLastPesistedVersion(_id);
        }

        internal Func<IObjectDBTransactionInternal, ulong, AbstractBufferedReader, object> GetLoader(uint version)
        {
            return _loaders.GetOrAdd(version, CreateLoader);
        }

        Func<IObjectDBTransactionInternal, ulong, AbstractBufferedReader, object> CreateLoader(uint version)
        {
            EnsureClientTypeVersion();
            EnsureImplType();
            var method = new DynamicMethod(string.Format("{0}_loader_{1}", Name, version), typeof(object), new[] { typeof(IObjectDBTransactionInternal), typeof(ulong), typeof(AbstractBufferedReader) });
            var ilGenerator = method.GetILGenerator();
            ilGenerator.DeclareLocal(_implType);
            ilGenerator
                .Ldarg(1)
                .Ldarg(0)
                .Call(_implType.GetMethod("CreateInstance"))
                .Isinst(_implType)
                .Stloc(0);
            var tableVersionInfo = _tableVersions.GetOrAdd(version, version1 => _tableInfoResolver.LoadTableVersionInfo(_id, version1, Name));
            for (int fi = 0; fi < tableVersionInfo.FieldCount; fi++)
            {
                var srcFieldInfo = tableVersionInfo[fi];
                var destFieldInfo = ClientTableVersionInfo[srcFieldInfo.Name];
                if (destFieldInfo != null)
                {
                    if (srcFieldInfo.Handler == destFieldInfo.Handler && srcFieldInfo.Handler.LoadToSameHandler(ilGenerator, ig => ig.Ldarg(2), ig => ig.Ldloc(0), _implType, destFieldInfo.Name))
                    {
                        continue;
                    }
                    var willLoad = srcFieldInfo.Handler.WillLoad();
                    var fieldInfo = _implType.GetField("_FieldStorage_" + destFieldInfo.Name);
                    var canConvertThrough = _tableInfoResolver.TypeConvertorGenerator.CanConvertThrough(willLoad, t => t == fieldInfo.FieldType);
                    if (canConvertThrough != null)
                    {
                        ilGenerator.Ldloc(0);
                        srcFieldInfo.Handler.LoadToWillLoad(ilGenerator, ig => ig.Ldarg(2));
                        _tableInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad, canConvertThrough)(ilGenerator);
                        ilGenerator.Stfld(fieldInfo);
                        continue;
                    }
                }
                srcFieldInfo.Handler.SkipLoad(ilGenerator, ig => ig.Ldarg(2));
            }
            ilGenerator.Ldloc(0).Ret();
            return method.CreateDelegate<Func<IObjectDBTransactionInternal, ulong, AbstractBufferedReader, object>>();
        }
    }
}