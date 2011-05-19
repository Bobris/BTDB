using System;
using System.Threading.Tasks;
using BTDB.KVDBLayer.Interface;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer
{
    public interface IObjectDB : IDisposable 
    {
        void Open(IKeyValueDB keyValueDB, bool dispose);

        IObjectDBTransaction StartTransaction();

        Task<IObjectDBTransaction> StartWritingTransaction();

        string RegisterType(Type type);

        string RegisterType(Type type, string withName);

        ITypeConvertorGenerator TypeConvertorGenerator { get; set; }

        IFieldHandlerFactory FieldHandlerFactory { get; set; }
    }
}
