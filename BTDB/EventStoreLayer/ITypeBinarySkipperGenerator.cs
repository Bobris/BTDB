using System;
using BTDB.IL;

namespace BTDB.EventStoreLayer
{
    public interface ITypeBinarySkipperGenerator
    {
        bool SkipNeedsCtx();
        void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx);
    }
}