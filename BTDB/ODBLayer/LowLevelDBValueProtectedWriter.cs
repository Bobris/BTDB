namespace BTDB.ODBLayer
{
    internal class LowLevelDBValueProtectedWriter : LowLevelDBValueWriter
    {
        readonly LowLevelDBTransactionProtector _protector;

        internal LowLevelDBValueProtectedWriter(ILowLevelDBTransaction transaction, LowLevelDBTransactionProtector protector)
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