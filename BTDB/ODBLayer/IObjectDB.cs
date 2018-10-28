using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public interface IObjectDB : IFieldHandlerFactoryProvider, IDisposable
    {
        void Open(IKeyValueDB keyValueDB, bool dispose);

        void Open(IKeyValueDB keyValueDB, bool dispose, DBOptions options);

        IObjectDBTransaction StartTransaction();

        IObjectDBTransaction StartReadOnlyTransaction();

        Task<IObjectDBTransaction> StartWritingTransaction();

        string RegisterType(Type type);

        string RegisterType(Type type, string withName);

        Type TypeByName(string name);

        string RegisterPolymorphicType(Type type, Type baseType);

        bool IsPolymorphicType(Type baseType, out IEnumerable<Type> subTypes);

        new ITypeConvertorGenerator TypeConvertorGenerator { get; set; }

        new IFieldHandlerFactory FieldHandlerFactory { get; set; }

        DBOptions ActualOptions { get; }

        IObjectDBLogger Logger { get; set; }
    }
}
