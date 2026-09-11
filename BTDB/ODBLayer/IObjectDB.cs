using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.Serialization;

namespace BTDB.ODBLayer;

public interface IObjectDB : IFieldHandlerFactoryProvider, IDisposable
{
    void Open(IKeyValueDB keyValueDB, bool dispose);

    void Open(IKeyValueDB keyValueDB, bool dispose, DBOptions options);

    IObjectDBTransaction StartTransaction();

    IObjectDBTransaction StartReadOnlyTransaction();

    ValueTask<IObjectDBTransaction> StartWritingTransaction(bool inBatch = false);

    /// <summary>
    /// Finish the current batch immediately if idle, otherwise after the active writer commits or rolls back,
    /// before the next queued writer starts. Returns immediately without committing the active transaction.
    /// </summary>
    void FinishTransactionBatchAfterCurrentTransaction();

    LeakDetectionResult RunLeakDetection(CancellationToken cancellation = default);

    ValueTask<LeakRemovalResult> RunLeakRemovalAsync(CancellationToken cancellation = default);

    string RegisterType(Type type);

    string RegisterType(Type type, string withName);

    IEnumerable<Type> GetPolymorphicTypes(Type baseType);

    Type TypeByName(string name);

    new ITypeConvertorGenerator TypeConvertorGenerator { get; set; }
    new ITypeConverterFactory TypeConverterFactory { get; set; }

    new IFieldHandlerFactory FieldHandlerFactory { get; set; }

    DBOptions ActualOptions { get; }

    IObjectDBLogger? Logger { get; set; }

    ISymmetricCipher GetSymmetricCipher();

    void RegisterCustomRelation(Type type, Func<IObjectDBTransaction, IRelation> factory);

    bool AllowAutoRegistrationOfRelations { get; set; }

    bool AutoRegisterTypes { get; set; }
}
