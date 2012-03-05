using System;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public class KeyValueDBValueWriter : ByteBufferWriter, IDisposable
    {
        readonly IKeyValueDBTransaction _transaction;

        public KeyValueDBValueWriter(IKeyValueDBTransaction transaction)
        {
            _transaction = transaction;
        }

        public virtual void Dispose()
        {
            _transaction.SetValue(Data);
        }
    }
}