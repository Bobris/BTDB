using System;
using System.Reflection.Emit;

namespace BTDB.ODBLayer.FieldHandlerIface
{
    public interface IFieldHandler
    {
        string Name { get; }
        byte[] Configuration { get; }
        bool IsCompatibleWith(Type type);
        Type HandledType();
        void Load(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushCtx);
        void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushCtx);
        void Save(ILGenerator ilGenerator, Action<ILGenerator> pushWriter, Action<ILGenerator> pushCtx, Action<ILGenerator> pushValue);
        void InformAboutDestinationHandler(IFieldHandler dstHandler);
    }
}
