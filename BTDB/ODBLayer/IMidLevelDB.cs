using System;
using System.Threading.Tasks;

namespace BTDB.ODBLayer
{
    public interface IMidLevelDB : IDisposable 
    {
        void Open(ILowLevelDB lowLevelDB, bool dispose);

        IMidLevelDBTransaction StartTransaction();

        Task<IMidLevelDBTransaction> StartWritingTransaction();

        string RegisterType(Type type);

        string RegisterType(Type type, string withName);

        ITypeConvertorGenerator TypeConvertorGenerator { get; set; }

        IFieldHandlerFactory FieldHandlerFactory { get; set; }
    }
}
