using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;

namespace BTDB.ODBLayer
{
    internal class TableInfo
    {
        readonly uint _id;
        readonly string _name;
        readonly ITableInfoResolver _tableInfoResolver;
        uint _clientTypeVersion;
        Type _clientType;
        readonly ConcurrentDictionary<uint, TableVersionInfo> _tableVersions = new ConcurrentDictionary<uint, TableVersionInfo>();
        Func<IObjectDBTransaction, DBObjectMetadata, object> _creator;
        Action<object> _saver;
        readonly ConcurrentDictionary<uint, Func<IObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object>> _loaders = new ConcurrentDictionary<uint, Func<IObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object>>();
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

        internal Func<IObjectDBTransaction, DBObjectMetadata, object> Creator
        {
            get
            {
                if (_creator == null) CreateCreator();
                return _creator;
            }
        }

        void CreateCreator()
        {
            var method = new DynamicMethod<Func<IObjectDBTransaction, DBObjectMetadata, object>>(string.Format("Creator_{0}", Name));
            var ilGenerator = method.GetILGenerator();
            ilGenerator
                .Newobj(_clientType.GetConstructor(Type.EmptyTypes))
                .Ret();
            var creator = method.Create();
            System.Threading.Interlocked.CompareExchange(ref _creator, creator, null);
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
            var saver = (Action<object>)null;
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
                fields.Add(TableFieldInfo.Build(Name, pi, _tableInfoResolver.FieldHandlerFactory));
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

        internal Func<IObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object> GetLoader(uint version)
        {
            return _loaders.GetOrAdd(version, CreateLoader);
        }

        Func<IObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object> CreateLoader(uint version)
        {
            EnsureClientTypeVersion();
            var method = new DynamicMethod<Func<IObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object>>(string.Format("Loader_{0}_{1}", Name, version));
            var ilGenerator = method.GetILGenerator();
            ilGenerator.DeclareLocal(_clientType);
            ilGenerator
                .Newobj(_clientType.GetConstructor(Type.EmptyTypes))
                .Stloc(0);
            var tableVersionInfo = _tableVersions.GetOrAdd(version, version1 => _tableInfoResolver.LoadTableVersionInfo(_id, version1, Name));
            for (int fi = 0; fi < tableVersionInfo.FieldCount; fi++)
            {
                var srcFieldInfo = tableVersionInfo[fi];
                var destFieldInfo = ClientTableVersionInfo[srcFieldInfo.Name];
                if (destFieldInfo != null)
                {
                    srcFieldInfo.Handler.InformAboutDestinationHandler(destFieldInfo.Handler);
                    var willLoad = srcFieldInfo.Handler.HandledType();
                    var fieldInfo = _clientType.GetProperty(destFieldInfo.Name).GetSetMethod();
                    var converterGenerator = _tableInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad, fieldInfo.GetParameters()[0].ParameterType);
                    if (converterGenerator != null)
                    {
                        ilGenerator.Ldloc(0);
                        srcFieldInfo.Handler.Load(ilGenerator, ig => ig.Ldarg(2), null);
                        converterGenerator(ilGenerator);
                        ilGenerator.Call(fieldInfo);
                        continue;
                    }
                }
                srcFieldInfo.Handler.SkipLoad(ilGenerator, ig => ig.Ldarg(2), null);
            }
            ilGenerator.Ldloc(0).Ret();
            return method.Create();
        }
    }
}