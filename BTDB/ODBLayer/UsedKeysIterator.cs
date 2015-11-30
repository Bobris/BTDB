using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using BTDB.Buffer;
using BTDB.FieldHandler;

namespace BTDB.ODBLayer
{
    public interface IUsedKeysStore
    {
        void PushContext(string name);
        void Add(byte[] key);
        void Add(byte[] prefix, byte[] suffix);
        void PopContext();
    }

    public class UsedKeysIterator
    {
        IInternalObjectDBTransaction _tr;
        IUsedKeysStore _store;
        ObjectDB _db;

        public void Iterate(IObjectDBTransaction tr, IUsedKeysStore store)
        {
            _tr = tr as IInternalObjectDBTransaction;
            _store = store;
            _db = _tr.Owner as ObjectDB;
            InContext("Singletons", IterateSingletons);
        }

        void IterateSingletons()
        {

            foreach (var type in _tr.EnumerateSingletonTypes())
            {
                _store.PushContext(type.Name);
                var singleton = _tr.Singleton(type);
                var ti = _db.TablesInfo.FindByType(type);
                _store.Add(ObjectDB.TableSingletonsPrefix, BuildKeyFromOid(ti.Id));
                IterateObject(singleton, type);
                _store.PopContext();
            }
        }

        void IterateObject(object obj, Type type)
        {
            var oid = _tr.GetOid(obj);
            if (oid != 0)
                _store.Add(ObjectDB.AllObjectsPrefix, BuildKeyFromOid(oid));
            var tvi = _db.TablesInfo.FindByType(type).ClientTableVersionInfo;
            for (var i = 0; i < tvi.FieldCount; i++)
            {
                var tvfi = tvi[i];
                var fieldType = tvfi.Handler.HandledType();
                if (DictionaryFieldHandler.IsCompatibleWith(fieldType))
                {
                    var keyType = fieldType.GetGenericArguments()[0];
                    var valueType = fieldType.GetGenericArguments()[1];
                    var iterateKeys = NeedIterate(keyType);
                    var iterateValues = NeedIterate(valueType);

                    if (iterateKeys || iterateValues)
                    {
                        InContext("[]", () =>
                        {
                            var keyValueType = typeof(KeyValuePair<,>).MakeGenericType(keyType, valueType);
                            var keyProp = keyValueType.GetProperty("Key");
                            var valueProp = keyValueType.GetProperty("Value");

                            var dictField = type.GetProperty(tvfi.Name);
                            var content = dictField.GetValue(obj) as IEnumerable;

                            foreach (var kvp in content)
                            {
                                if (iterateKeys)
                                    IterateObject(keyProp.GetValue(kvp), keyType);
                                if (iterateValues)
                                    IterateObject(valueProp.GetValue(kvp), valueType);
                            }
                        });
                    }
                }
                else if (NeedIterate(fieldType))
                {
                    var fieldGetter = type.GetProperty(tvfi.Name);
                    InContext(tvfi.Name, () => IterateObject(fieldGetter.GetValue(obj), tvfi.Handler.HandledType()));
                }
                //todo IIndirect
            }
        }

        bool NeedIterate(Type type)
        {
            //todo better check & cache for speed
            return !type.IsValueType && type != typeof(string);
        }

        static byte[] BuildKeyFromOid(ulong oid)
        {
            var key = new byte[PackUnpack.LengthVUInt(oid)];
            int ofs = 0;
            PackUnpack.PackVUInt(key, ref ofs, oid);
            return key;
        }

        void InContext(string name, Action action)
        {
            _store.PushContext(name);
            try
            {
                action();
            }
            finally
            {
                _store.PopContext();
            }
        }
    }
}