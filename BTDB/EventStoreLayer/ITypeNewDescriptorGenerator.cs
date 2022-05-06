using System;
using BTDB.IL;

namespace BTDB.EventStoreLayer;

public interface ITypeNewDescriptorGenerator
{
    // ctx is IDescriptorSerializerLiteContext type
    void GenerateTypeIterator(IILGen ilGenerator, Action<IILGen> pushObj, Action<IILGen> pushCtx, Type type);
}
