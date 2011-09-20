using System;
using System.Threading.Tasks;
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

        ITypeConvertorGenerator TypeConvertorGenerator { get; set; }

        IFieldHandlerFactory FieldHandlerFactory { get; set; }
    }
}
