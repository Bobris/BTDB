using System;
using System.Threading.Tasks;

namespace BTDB
{
    public interface IKeyValueDB: IDisposable
    {
        IKeyValueDBTweaks KeyValueDBTweaks { get; set; }

        /// <summary>
        /// Default are durable, not corrupting commits (true)
        /// </summary>
        bool DurableTransactions { get; set; }

        bool Open(IPositionLessStream positionLessStream, bool dispose);

        IKeyValueDBTransaction StartTransaction();

        Task<IKeyValueDBTransaction> StartWritingTransaction();
    }
}
