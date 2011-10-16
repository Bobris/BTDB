using System.Collections.Generic;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public class ServiceWriterCtx : IWriterCtx
    {
        readonly AbstractBufferedWriter _writer;
        Dictionary<object, uint> _objectIdMap;
        uint _lastId;

        public ServiceWriterCtx(AbstractBufferedWriter writer)
        {
            _writer = writer;
        }

        public bool WriteObject(object @object)
        {
            if (@object == null)
            {
                _writer.WriteByteZero();
                return false;
            }
            if (_objectIdMap == null) _objectIdMap = new Dictionary<object, uint>();
            uint cid;
            if (_objectIdMap.TryGetValue(@object, out cid))
            {
                _writer.WriteVUInt32(cid);
                return false;
            }
            _lastId++;
            _objectIdMap.Add(@object, _lastId);
            _writer.WriteVUInt32(_lastId);
            return true;
        }

        public AbstractBufferedWriter Writer()
        {
            return _writer;
        }
    }
}