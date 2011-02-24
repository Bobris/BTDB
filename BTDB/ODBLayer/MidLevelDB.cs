using System;
using System.Threading.Tasks;

namespace BTDB.ODBLayer
{
    public class MidLevelDB : IMidLevelDB
    {
        ILowLevelDB _lowLevelDB;
        bool _dispose;

        public void Open(ILowLevelDB lowLevelDB, bool dispose)
        {
            if (lowLevelDB == null) throw new ArgumentNullException("lowLevelDB");
            _lowLevelDB = lowLevelDB;
            _dispose = dispose;
        }

        public IMidLevelDBTransaction StartTransaction()
        {
            return new MidLevelDBTransaction(this, _lowLevelDB.StartTransaction());
        }

        public Task<IMidLevelDBTransaction> StartWritingTransaction()
        {
            return _lowLevelDB.StartWritingTransaction()
                .ContinueWith<IMidLevelDBTransaction>(t => new MidLevelDBTransaction(this, t.Result), TaskContinuationOptions.ExecuteSynchronously);
        }

        public void Dispose()
        {
            if (_dispose)
            {
                _lowLevelDB.Dispose();
                _dispose = false;
            }
        }
    }
}