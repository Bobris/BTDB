using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    class KeyValueDBValueProtectedWriter : KeyValueDBValueWriter
    {
        readonly KeyValueDBTransactionProtector _protector;

        internal KeyValueDBValueProtectedWriter(IKeyValueDBTransaction transaction, KeyValueDBTransactionProtector protector)
            : base(transaction)
        {
            _protector = protector;
        }

        public override void Dispose()
        {
            base.Dispose();
            _protector.Stop();
        }
    }
}