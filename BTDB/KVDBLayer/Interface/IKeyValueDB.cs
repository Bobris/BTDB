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
        /// Default are durable, not corrupting commits (true). In case of false and crash of OS or computer, database could became corrupted, and unopennable.
        /// </summary>
        bool DurableTransactions { get; set; }

        bool Open(IPositionLessStream positionLessStream, bool dispose);

        /// <summary>
        /// it will be UTF-8 encoded. There is atleast 300 bytes to store your description. If you will try to set too long description you will get exception.
        /// DB must be in Open state to be able get or set description. It is not protected by transaction so don't be too much dependent on it.
        /// </summary>
        string HumanReadableDescriptionInHeader { get; set; }

        IKeyValueDBTransaction StartTransaction();

        Task<IKeyValueDBTransaction> StartWritingTransaction();
    }
}
