using System;
using System.Threading.Tasks;

namespace BTDB.KVDBLayer
{
    public interface IKeyValueDB: IDisposable
    {
        // Default are durable, not corrupting commits (true). In case of false and crash of OS or computer, database could became corrupted, and unopennable.
        bool DurableTransactions { get; set; }

        IKeyValueDBTransaction StartTransaction();

        Task<IKeyValueDBTransaction> StartWritingTransaction();

        string CalcStats();
    }
}
