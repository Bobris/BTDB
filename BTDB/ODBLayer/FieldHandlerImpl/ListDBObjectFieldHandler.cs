using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Threading;
using BTDB.IL;
using BTDB.KVDBLayer.Interface;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class ListDBObjectFieldHandler : IFieldHandler
    {
        static readonly IFieldHandler MemberHandler = new DBObjectFieldHandler();

        // ReSharper disable MemberCanBePrivate.Global
        public class ListOfDBObject<T> : IList<T> where T : class
        // ReSharper restore MemberCanBePrivate.Global
        {
            readonly List<ulong> _oids;
            readonly IDBObject _owner;

            // ReSharper disable UnusedMember.Global
            public ListOfDBObject(IDBObject owner)
            // ReSharper restore UnusedMember.Global
            {
                _owner = owner;
                _oids = new List<ulong>();
            }

            public ListOfDBObject(IDBObject owner, List<ulong> oids)
            {
                _owner = owner;
                _oids = oids;
            }

            public IEnumerator<T> GetEnumerator()
            {
                foreach (var oid in _oids)
                {
                    yield return _owner.OwningTransaction.Get(oid) as T;
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public List<ulong> Oids
            {
                get { return _oids; }
            }

            void OnChange()
            {
                ((IObjectDBTransactionInternal)_owner.OwningTransaction).ObjectModified(_owner);
            }

            ulong GetOid(T item)
            {
                return ((IObjectDBTransactionInternal)_owner.OwningTransaction).GetOid(item);
            }

            public void Add(T item)
            {
                var oid = GetOid(item);
                OnChange();
                _oids.Add(oid);
            }

            public void Clear()
            {
                OnChange();
                _oids.Clear();
            }

            public bool Contains(T item)
            {
                return _oids.Contains(GetOid(item));
            }

            public void CopyTo(T[] array, int arrayIndex)
            {
                if (array == null) throw new ArgumentNullException("array");
                if (arrayIndex < 0) throw new ArgumentOutOfRangeException("arrayIndex");
                foreach (var oid in _oids)
                {
                    array[arrayIndex++] = _owner.OwningTransaction.Get(oid) as T;
                }
            }

            public bool Remove(T item)
            {
                return _oids.Remove(GetOid(item));
            }

            public int Count
            {
                get { return _oids.Count; }
            }

            public bool IsReadOnly
            {
                get { return false; }
            }

            public int IndexOf(T item)
            {
                return _oids.IndexOf(GetOid(item));
            }

            public void Insert(int index, T item)
            {
                var oid = GetOid(item);
                OnChange();
                _oids.Insert(index, oid);
            }

            public void RemoveAt(int index)
            {
                _oids.RemoveAt(index);
            }

            public T this[int index]
            {
                get { return _owner.OwningTransaction.Get(_oids[index]) as T; }
                set
                {
                    var oid = GetOid(value);
                    OnChange();
                    _oids[index] = oid;
                }
            }
        }

        public string Name
        {
            get { return "ListOfDBObject"; }
        }

        public byte[] Configuration
        {
            get { return null; }
        }

        public bool IsCompatibleWith(Type type)
        {
            return type.GetGenericTypeDefinition() == typeof(IList<>) &&
                   MemberHandler.IsCompatibleWith(type.GetGenericArguments()[0]);
        }

        // ReSharper disable UnusedMember.Global
        public static IList<T> LoaderImpl<T>(AbstractBufferedReader reader, IDBObject owner) where T : class
        // ReSharper restore UnusedMember.Global
        {
            var count = reader.ReadVUInt32();
            if (count == 0) return null;
            var oids = new List<ulong>((int)count);
            while (count-- > 0)
            {
                oids.Add(reader.ReadVUInt64());
            }
            return new ListOfDBObject<T>(owner, oids);
        }

        public bool LoadToSameHandler(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushThis, Type implType, string destFieldName)
        {
            var fieldInfo = implType.GetField("_FieldStorage_" + destFieldName);
            pushThis(ilGenerator);
            pushReader(ilGenerator);
            pushThis(ilGenerator);
            ilGenerator.Call(typeof(ListDBObjectFieldHandler).GetMethod("LoaderImpl").MakeGenericMethod(fieldInfo.FieldType.GetGenericArguments()));
            ilGenerator.Stfld(fieldInfo);
            return true;
        }

        public Type WillLoad()
        {
            return null;
        }

        public void LoadToWillLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            throw new InvalidOperationException();
        }

        // ReSharper disable MemberCanBePrivate.Global
        public static void SkipImpl(AbstractBufferedReader reader)
        // ReSharper restore MemberCanBePrivate.Global
        {
            var items = reader.ReadVUInt32();
            while (items-- > 0)
            {
                reader.SkipVUInt64();
            }
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(() => SkipImpl(null));
        }

        public void SaveFromWillLoad(ILGenerator ilGenerator, Action<ILGenerator> pushWriter, Action<ILGenerator> pushValue)
        {
            throw new InvalidOperationException();
        }

        public void CreateStorage(FieldHandlerCreateImpl ctx)
        {
            ctx.CreateSimpleStorage();
        }

        public void CreatePropertyGetter(FieldHandlerCreateImpl ctx)
        {
            var defaultFieldBuilder = ctx.DefaultFieldBuilder;
            var listImplType = typeof(ListOfDBObject<>).MakeGenericType(defaultFieldBuilder.FieldType.GetGenericArguments());
            var miGenericCompareExchange = typeof(Interlocked).GetMethods().Single(mi => mi.IsGenericMethod && mi.Name == "CompareExchange");
            var miCompareExchange = miGenericCompareExchange.MakeGenericMethod(defaultFieldBuilder.FieldType);
            var ilGenerator = ctx.Generator;
            var label = ilGenerator.DefineLabel();
            ilGenerator
                .Ldarg(0)
                .Ldfld(defaultFieldBuilder)
                .BrtrueS(label)
                .Ldarg(0)
                .Ldflda(defaultFieldBuilder)
                .Ldarg(0)
                .Newobj(listImplType.GetConstructor(new[] {typeof (IDBObject)}))
                .Castclass(defaultFieldBuilder.FieldType)
                .Ldnull()
                .Call(miCompareExchange)
                .Pop()
                .Mark(label)
                .Ldarg(0)
                .Ldfld(defaultFieldBuilder);
        }

        public void CreatePropertySetter(FieldHandlerCreateImpl ctx)
        {
            throw new BTDBException(string.Format("Property {0} in {1} must be read only", ctx.FieldName, ctx.TableName));
        }

        // ReSharper disable UnusedMember.Global
        public static void SaverImpl<T>(AbstractBufferedWriter writer, IList<T> ilist) where T : class
        // ReSharper restore UnusedMember.Global
        {
            if (ilist == null)
            {
                writer.WriteVUInt32(0);
                return;
            }
            var oids = ((ListOfDBObject<T>)ilist).Oids;
            writer.WriteVUInt32((uint)oids.Count);
            foreach (var oid in oids)
            {
                writer.WriteVUInt64(oid);
            }
        }

        public void CreateSaver(FieldHandlerCreateImpl ctx)
        {
            ctx.Generator
                .Ldloc(1)
                .Ldloc(0)
                .Ldfld(ctx.DefaultFieldBuilder)
                .Call(typeof(ListDBObjectFieldHandler).GetMethod("SaverImpl").MakeGenericMethod(ctx.DefaultFieldBuilder.FieldType.GetGenericArguments()));
        }
    }
}