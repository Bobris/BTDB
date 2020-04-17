using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace BTDB.FieldHandler
{
    public class DBIndirect<T> : IIndirect<T> where T : class
    {
        IObjectDBTransaction _transaction;
        readonly ulong _oid;
        T _value;

        internal DBIndirect(IObjectDBTransaction transaction, ulong oid)
        {
            _transaction = transaction;
            _oid = oid;
        }

        public DBIndirect()
        {
        }

        public DBIndirect(T obj)
        {
            _value = obj;
        }

        public T Value
        {
            get
            {
                if (_transaction != null)
                {
                    _value = _transaction.Get(_oid) as T;
                    _transaction = null;
                }
                return _value;
            }
            set
            {
                _value = value;
                _transaction = null;
            }
        }

        [NotStored]
        public ulong Oid => _oid;

        [NotStored]
        public object ValueAsObject => _value;

        public static void SaveImpl(IWriterCtx writerCtx, object obj)
        {
            var ind = obj as DBIndirect<T>;
            if (ind != null)
            {
                if (ind._transaction != null)
                {
                    if (((IDBWriterCtx)writerCtx).GetTransaction() != ind._transaction)
                    {
                        throw new BTDBException("Transaction does not match when saving nonmaterialized IIndirect");
                    }
                    writerCtx.Writer().WriteVInt64((long)ind._oid);
                    return;
                }
            }
            var ind2 = obj as IIndirect<T>;
            if (ind2 != null)
            {
                writerCtx.WriteNativeObjectPreventInline(ind2.Value);
                return;
            }
            writerCtx.WriteNativeObjectPreventInline(obj);
        }

        public static IIndirect<T> LoadImpl(IReaderCtx readerCtx)
        {
            var oid = readerCtx.Reader().ReadVInt64();
            return new DBIndirect<T>(((IDBReaderCtx)readerCtx).GetTransaction(), (ulong)oid);
        }
    }
}
