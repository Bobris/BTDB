using System;
using System.Threading.Tasks;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public interface IObjectDB : IFieldHandlerFactoryProvider, IDisposable 
    {
        void Open(IKeyValueDB keyValueDB, bool dispose);

        IObjectDBTransaction StartTransaction();

        Task<IObjectDBTransaction> StartWritingTransaction();

        string RegisterType(Type type);

        string RegisterType(Type type, string withName);

        Type TypeByName(string name);

        new ITypeConvertorGenerator TypeConvertorGenerator { get; set; }

        new IFieldHandlerFactory FieldHandlerFactory { get; set; }
    }
}
