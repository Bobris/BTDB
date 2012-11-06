using System;
using BTDB.IL;

namespace BTDB.EventStoreLayer
{
    public interface ITypeBinaryDeserializerGenerator
    {
        bool LoadNeedsCtx();
        void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx);
    }
}