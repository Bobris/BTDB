using System;

namespace BTDB.FieldHandler;

public interface IFieldHandlerFactory
{
    bool TypeSupported(Type type);
    IFieldHandler CreateFromType(Type type, FieldHandlerOptions options);
    IFieldHandler CreateFromName(string handlerName, byte[]? configuration, FieldHandlerOptions options);
}
