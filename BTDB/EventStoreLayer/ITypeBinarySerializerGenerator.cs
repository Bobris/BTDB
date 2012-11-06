using System;
using BTDB.IL;

namespace BTDB.EventStoreLayer
{
    public interface ITypeBinarySerializerGenerator
    {
        bool SaveNeedsCtx();
        void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue);
    }
}