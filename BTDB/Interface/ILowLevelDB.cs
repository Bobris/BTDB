using System;

namespace BTDB
{
    public interface ILowLevelDB: IDisposable
    {
        ITweaks Tweaks { get; set; }

        /// <summary>
        /// Default DurabilityPromise is NearlyDurable
        /// </summary>
        DurabilityPromiseType DurabilityPromise { get; set; }

        bool Open(IStream stream, bool dispose);

        ILowLevelDBTransaction StartTransaction();
    }
}
