using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class DBIndirect<T> : IIndirect<T> where T : class
{
    IObjectDBTransaction? _transaction;
    readonly ulong _oid;
    T? _value;

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

    public T? Value
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
    public object? ValueAsObject => _value;

    public static void SaveImpl(ref SpanWriter writer, IWriterCtx writerCtx, object obj)
    {
        if (obj is DBIndirect<T> ind)
        {
            if (ind._transaction != null)
            {
                if (((IDBWriterCtx)writerCtx).GetTransaction() != ind._transaction)
                {
                    throw new BTDBException("Transaction does not match when saving nonmaterialized IIndirect");
                }
                writer.WriteVInt64((long)ind._oid);
                return;
            }
        }

        if (obj is IIndirect<T> ind2)
        {
            writerCtx.WriteNativeObjectPreventInline(ref writer, ind2.Value);
            return;
        }
        writerCtx.WriteNativeObjectPreventInline(ref writer, obj);
    }

    public static IIndirect<T> LoadImpl(ref SpanReader reader, IReaderCtx readerCtx)
    {
        var oid = reader.ReadVInt64();
        return new DBIndirect<T>(((IDBReaderCtx)readerCtx).GetTransaction(), (ulong)oid);
    }
}
