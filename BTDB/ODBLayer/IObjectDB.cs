using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer;

public interface IObjectDB : IFieldHandlerFactoryProvider, IDisposable
{
    void Open(IKeyValueDB keyValueDB, bool dispose);

    void Open(IKeyValueDB keyValueDB, bool dispose, DBOptions options);

    IObjectDBTransaction StartTransaction();

    IObjectDBTransaction StartReadOnlyTransaction();

    ValueTask<IObjectDBTransaction> StartWritingTransaction();

    string RegisterType(Type type);

    string RegisterType(Type type, string withName);

    IEnumerable<Type> GetPolymorphicTypes(Type baseType);

    Type TypeByName(string name);

    new ITypeConvertorGenerator TypeConvertorGenerator { get; set; }

    new IFieldHandlerFactory FieldHandlerFactory { get; set; }

    DBOptions ActualOptions { get; }

    IObjectDBLogger? Logger { get; set; }

    ISymmetricCipher GetSymmetricCipher();

    void RegisterCustomRelation(Type type, Func<IObjectDBTransaction, IRelation> factory);

    bool AllowAutoRegistrationOfRelations { get; set; }

    bool AutoRegisterTypes { get; set; }
}
