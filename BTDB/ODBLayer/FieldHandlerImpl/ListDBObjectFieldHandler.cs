using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.KVDBLayer.Interface;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class ListDBObjectFieldHandler : IFieldHandler
    {
        static readonly IFieldHandler MemberHandler = new DBObjectFieldHandler();

        public class ListOfDBObject<T> : IList<T> where T : class
        {
            readonly List<ulong> _oids = new List<ulong>();
            readonly IDBObject _owner;

            public ListOfDBObject(IDBObject owner)
            {
                _owner = owner;
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

            void OnChange()
            {
                ((IObjectDBTransactionInternal)_owner.OwningTransaction).ObjectModified(_owner.Oid, _owner);
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

        public bool LoadToSameHandler(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushThis, Type implType, string destFieldName)
        {
            pushThis(ilGenerator);
            pushReader(ilGenerator);
            ilGenerator.Call(() => ((AbstractBufferedReader)null).ReadVUInt64());
            var fieldInfo = implType.GetField("_FieldStorage_" + destFieldName);
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
            var ilGenerator = ctx.Generator;
            ilGenerator
                .Ldarg(0)
                .Ldfld(ctx.FieldMidLevelDBTransaction)
                .Ldarg(0)
                .Ldfld(ctx.DefaultFieldBuilder)
                .Callvirt(() => ((IObjectDBTransactionInternal)null).Get(0));
            if (ctx.PropertyInfo.PropertyType != typeof(object))
            {
                ilGenerator.Isinst(ctx.PropertyInfo.PropertyType);
            }
        }

        public void CreatePropertySetter(FieldHandlerCreateImpl ctx)
        {
            throw new BTDBException(string.Format("Property {0} in {1} must be read only", ctx.FieldName, ctx.TableName));
            var ilGenerator = ctx.Generator;
            var fieldBuilder = ctx.DefaultFieldBuilder;
            var labelNoChange = ilGenerator.DefineLabel();
            ilGenerator.DeclareLocal(typeof(ulong));
            ilGenerator
                .Ldarg(0)
                .Ldfld(ctx.FieldMidLevelDBTransaction)
                .Ldarg(1)
                .Callvirt(() => ((IObjectDBTransactionInternal)null).GetOid(null))
                .Stloc(0);
            EmitHelpers.GenerateJumpIfEqual(ilGenerator, typeof(ulong), labelNoChange,
                                            g => g.Ldarg(0).Ldfld(fieldBuilder),
                                            g => g.Ldloc(0));
            ilGenerator
                .Ldarg(0)
                .Ldloc(0)
                .Stfld(fieldBuilder);
            ctx.CallObjectModified(ilGenerator);
            ilGenerator
                .Mark(labelNoChange);
        }

        public void CreateSaver(FieldHandlerCreateImpl ctx)
        {
            ctx.Generator
                .Ldloc(1)
                .Ldloc(0)
                .Ldfld(ctx.DefaultFieldBuilder)
                .Call(() => ((AbstractBufferedWriter)null).WriteVUInt64(0));
        }
    }
}