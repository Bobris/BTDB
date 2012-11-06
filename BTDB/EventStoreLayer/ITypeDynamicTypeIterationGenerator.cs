using System;
using BTDB.IL;

namespace BTDB.EventStoreLayer
{
    public interface ITypeDynamicTypeIterationGenerator
    {
        void GenerateTypeIterator(IILGen ilGenerator, Action<IILGen> pushCtx);
    }
}