using System;
using System.Reflection.Emit;

namespace BTDB.ODBLayer.FieldHandlerIface
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
        void CreateStorage(FieldHandlerCreateImpl ctx);
        void CreatePropertyGetter(FieldHandlerCreateImpl ctx);
        void CreatePropertySetter(FieldHandlerCreateImpl ctx);
        void CreateSaver(FieldHandlerCreateImpl ctx);
    }
}
