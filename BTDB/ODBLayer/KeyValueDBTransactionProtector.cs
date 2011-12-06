using System.Diagnostics;
using System.Threading;

namespace BTDB.ODBLayer
{
    public class KeyValueDBTransactionProtector
    {
        readonly object _lock = new object();
        long _protectionCounter;
        bool _lockTaken;

        internal void Start(ref bool taken)
        {
            Debug.Assert(_lockTaken == false);
            _lockTaken = false;
            taken = true;
            Monitor.Enter(_lock, ref _lockTaken);
            _protectionCounter++;
        }

        internal void Stop()
        {
            if (_lockTaken)
            {
                Monitor.Exit(_lock);
                _lockTaken = false;
            }
        }

        internal void Stop(ref bool taken)
        {
            if (_lockTaken)
            {
                Monitor.Exit(_lock);
                taken = false;
                _lockTaken = false;
            }
        }

        internal bool WasInterupted(long lastCounter)
        {
            return lastCounter + 1 != _protectionCounter;
        }

        internal long ProtectionCounter
        {
            get { return _protectionCounter; }
        }
    }
}