using System;

namespace BTDB.ODBLayer
{
    public interface IFieldHandler
    {
        string Name { get; }
        byte[] Configuration { get; }
        bool IsCompatibleWith(Type type);
        void Load(FieldHandlerLoad ctx);
        void SkipLoad(FieldHandlerSkipLoad ctx);
        void CreateImpl(FieldHandlerCreateImpl ctx);
    }
}
