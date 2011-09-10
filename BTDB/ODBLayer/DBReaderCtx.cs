using BTDB.KVDBLayer.ReaderWriters;

namespace BTDB.ODBLayer
{
    public class DBReaderCtx : IReaderCtx
    {
        readonly IObjectDBTransaction _transaction;
        readonly AbstractBufferedReader _reader;

        public DBReaderCtx(IObjectDBTransaction transaction, AbstractBufferedReader reader)
        {
            _transaction = transaction;
            _reader = reader;
        }

        public object ReadObject()
        {
            var id = _reader.ReadVInt64();
            if (id == 0) return null;
            return _transaction.Get((ulong) id);
        }

        public void SkipObject()
        {
            _reader.SkipVInt64();
        }

        public AbstractBufferedReader Reader()
        {
            return _reader;
        }
    }
}