using System;
using System.Reflection.Emit;

namespace BTDB.FieldHandler
{
    public interface IFieldHandler
    {
        string Name { get; }
        byte[] Configuration { get; }
        bool IsCompatibleWith(Type type);
        Type HandledType();
        bool NeedsCtx();
        void Load(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx);
        void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx);
        void Save(ILGenerator ilGenerator, Action<ILGenerator> pushWriterOrCtx, Action<ILGenerator> pushValue);
        void InformAboutDestinationHandler(IFieldHandler dstHandler);
    }
}
