using System;
using BTDB.IL;

namespace BTDB.EventStoreLayer
{
    public interface ITypeBinaryDeserializerGenerator
    {
        bool LoadNeedsCtx();
        // ctx is ITypeBinaryDeserializerContext
        void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor);
    }
}