namespace BTDB.ODBLayer
{
    public class KeyValueDBTransactionProtector
    {
        long _protectionCounter;

        internal void Start()
        {
            _protectionCounter++;
        }

        internal bool WasInterupted(long lastCounter)
        {
            return lastCounter + 1 != _protectionCounter;
        }

        internal long ProtectionCounter => _protectionCounter;
    }
}