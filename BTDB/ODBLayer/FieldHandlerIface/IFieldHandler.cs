using System;
using System.Reflection.Emit;

namespace BTDB.ODBLayer
{
    public interface IFieldHandler
    {
        string Name { get; }
        byte[] Configuration { get; }
        bool IsCompatibleWith(Type type);
        bool LoadToSameHandler(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushThis, Type implType, string destFieldName);
        Type WillLoad();
        void LoadToWillLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader);
        void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader);
        void CreateImpl(FieldHandlerCreateImpl ctx);
    }
}
