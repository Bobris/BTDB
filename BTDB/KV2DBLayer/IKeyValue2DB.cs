using System;
using System.Threading.Tasks;

namespace BTDB.KV2DBLayer
{
    public interface IKeyValue2DB : IDisposable
    {
        IKeyValue2DBTransaction StartTransaction();
        Task<IKeyValue2DBTransaction> StartWritingTransaction();
    }
}
