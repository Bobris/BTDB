using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public class KeyValueDBValueReader : ByteBufferReader, ICanMemorizePosition
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

        public IMemorizedPosition MemorizeCurrentPosition()
        {
            return new MemorizedPosition(this);
        }

        class MemorizedPosition : IMemorizedPosition
        {
            readonly KeyValueDBValueReader _owner;
            readonly int _pos;

            public MemorizedPosition(KeyValueDBValueReader owner)
            {
                _owner = owner;
                _pos = owner.Pos;
            }

            public void Restore()
            {
                _owner.Pos = _pos;
            }
        }
    }
}