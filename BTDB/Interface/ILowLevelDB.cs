using System;

namespace BTDB
{
    public interface ILowLevelDB: IDisposable
    {
        bool Open(IStream stream, bool dispose);

        ILowLevelDBTransaction StartTransaction();
    }
}
