using System;
using System.Threading.Tasks;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer.Interface
{
    public interface IKeyValueDB: IDisposable
    {
        /// <summary>
        /// Default Cache Size is 10MB. Minimum is 1MB. Maximum is 1024MB.
        /// </summary>
        int CacheSizeInMB { get; set; }
        
        /// <summary>
        /// Default are durable, not corrupting commits (true)
        /// </summary>
        bool DurableTransactions { get; set; }

        bool Open(IPositionLessStream positionLessStream, bool dispose);

        IKeyValueDBTransaction StartTransaction();

        Task<IKeyValueDBTransaction> StartWritingTransaction();
    }
}
