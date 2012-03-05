using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public class KeyValueDBKeyReader : ByteBufferReader
    {
        readonly IKeyValueDBTransaction _transaction;

        public KeyValueDBKeyReader(IKeyValueDBTransaction transaction)
            : base(transaction.GetKey())
        {
            _transaction = transaction;
        }

        public void Restart()
        {
            Restart(_transaction.GetKey());
        }
    }
}