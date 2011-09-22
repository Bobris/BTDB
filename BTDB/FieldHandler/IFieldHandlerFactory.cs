using System;

namespace BTDB.FieldHandler
{
    public interface IFieldHandlerFactory
    {
        bool TypeSupported(Type type);
        IFieldHandler CreateFromType(Type type);
        IFieldHandler CreateFromName(string handlerName, byte[] configuration);
    }
}