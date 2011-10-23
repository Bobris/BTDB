using System.Collections.Generic;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class DBWriterCtx : IDBWriterCtx
    {
        readonly IInternalObjectDBTransaction _transaction;
        readonly AbstractBufferedWriter _writer;
        Dictionary<object, int> _objectIdMap;
        int _lastId;

        public DBWriterCtx(IInternalObjectDBTransaction transaction, AbstractBufferedWriter writer)
        {
            _transaction = transaction;
            _writer = writer;
        }

        public bool WriteObject(object @object)
        {
            if (@object == null)
            {
                _writer.WriteVInt64(0);
                return false;
            }
            var oid = _transaction.StoreIfUnknownButTypeRegistered(@object);
            if (oid != ulong.MaxValue)
            {
                _writer.WriteVInt64((long)oid);
                return false;
            }
            if (_objectIdMap == null) _objectIdMap = new Dictionary<object, int>();
            int cid;
            if (_objectIdMap.TryGetValue(@object, out cid))
            {
                _writer.WriteVInt64(-cid);
                return false;
            }
            _lastId++;
            _objectIdMap.Add(@object, _lastId);
            _writer.WriteVInt64(-_lastId);
            return true;
        }

        public AbstractBufferedWriter Writer()
        {
            return _writer;
        }

        public int RegisterInstance(object content)
        {
            return ((IInstanceRegistry)_transaction.Owner).RegisterInstance(content);
        }

        public object FindInstance(int id)
        {
            return ((IInstanceRegistry)_transaction.Owner).FindInstance(id);
        }

        public IInternalObjectDBTransaction GetTransaction()
        {
            return _transaction;
        }
    }
}