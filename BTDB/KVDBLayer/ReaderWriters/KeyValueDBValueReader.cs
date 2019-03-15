using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public class KeyValueDBValueReader : ByteBufferReader
    {
        readonly IKeyValueDBTransaction _transaction;

        public KeyValueDBValueReader(IKeyValueDBTransaction transaction):base(transaction.GetValue())
        {
            _transaction = transaction;
        }

        public void Restart()
        {
            Restart(_transaction.GetValue());
        }
    }
}