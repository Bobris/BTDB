using System;
using System.Threading.Tasks;

namespace BTDB
{
    public interface ILowLevelDB: IDisposable
    {
        ITweaks Tweaks { get; set; }

        /// <summary>
        /// Default are durable, not corrupting commits (true)
        /// </summary>
        bool DurableTransactions { get; set; }

        bool Open(IStream stream, bool dispose);

        ILowLevelDBTransaction StartTransaction();

        Task<ILowLevelDBTransaction> StartWritingTransaction();
    }
}
