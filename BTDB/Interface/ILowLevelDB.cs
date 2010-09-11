using System;

namespace BTDB
{
    public interface ILowLevelDB: IDisposable
    {
        ITweaks Tweaks { get; set; }

        bool Open(IStream stream, bool dispose);

        ILowLevelDBTransaction StartTransaction();
    }
}
