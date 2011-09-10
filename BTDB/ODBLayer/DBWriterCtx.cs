using BTDB.KVDBLayer.ReaderWriters;

namespace BTDB.ODBLayer
{
    public class DBWriterCtx : IWriterCtx
    {
        readonly IObjectDBTransaction _transaction;
        readonly AbstractBufferedWriter _writer;

        public DBWriterCtx(IObjectDBTransaction transaction, AbstractBufferedWriter writer)
        {
            _transaction = transaction;
            _writer = writer;
        }

        public void WriteObject(object @object)
        {
            if (@object==null)
            {
                _writer.WriteVInt64(0);
                return;
            }
            var oid = _transaction.StoreIfUnknown(@object);
            _writer.WriteVInt64((long) oid);
        }

        public AbstractBufferedWriter Writer()
        {
            return _writer;
        }
    }
}