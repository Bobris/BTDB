using System;
using BTDB.Buffer;

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
        public void Iterate(IObjectDBTransaction tr, IUsedKeysStore store)
        {
            var itr = tr as IInternalObjectDBTransaction;
            InContext(store, "Singletons", () => IterateSingletons(itr, store));
        }

        void IterateSingletons(IInternalObjectDBTransaction tr, IUsedKeysStore store)
        {
            var db = tr.Owner as ObjectDB;
            foreach (var type in tr.EnumerateSingletonTypes())
            {
                store.PushContext(type.Name);
                var singleton = tr.Singleton(type);
                var ti = db.TablesInfo.FindByType(type);
                store.Add(ObjectDB.TableSingletonsPrefix, BuildKeyFromOid(ti.Id));
                IterateObject(tr, store, singleton, ti.ClientTableVersionInfo);
                store.PopContext();
            }
        }

        void IterateObject(IInternalObjectDBTransaction tr, IUsedKeysStore store, object obj, TableVersionInfo tvi)
        {
            for (var i = 0; i < tvi.FieldCount; i++)
            {
                var tvfi = tvi[i];
                if (tvfi.Handler.Name == "ODBDictionary")
                {
                    store.PushContext(tvfi.Name + "[]");
                    //var db=obj[tvfi.Name]
                    //store.Add(db._prefix);
                    store.PopContext();
                }
                //if !StoredInline object
                //IterateObject(tr, store, obj[tvfi.Name], ..)
            }
        }

        static byte[] BuildKeyFromOid(ulong oid)
        {
            var key = new byte[PackUnpack.LengthVUInt(oid)];
            int ofs = 0;
            PackUnpack.PackVUInt(key, ref ofs, oid);
            return key;
        }

        void InContext(IUsedKeysStore store, string name, Action action)
        {
            store.PushContext(name);
            try
            {
                action();
            }
            finally
            {
                store.PopContext();
            }
        }
    }
}