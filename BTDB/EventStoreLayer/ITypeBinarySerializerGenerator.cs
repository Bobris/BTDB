using System;
using BTDB.IL;

namespace BTDB.EventStoreLayer
{
    public interface ITypeBinarySerializerGenerator
    {
        bool SaveNeedsCtx();
        // ctx is ITypeBinarySerializerContext
        void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue, Type valueType);
    }
}