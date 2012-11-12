using System;
using BTDB.IL;

namespace BTDB.EventStoreLayer
{
    public interface ITypeNewDescriptorGenerator
    {
        void GenerateTypeIterator(IILGen ilGenerator, Action<IILGen> pushCtx);
    }
}