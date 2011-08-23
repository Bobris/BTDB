using System;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ServiceLayer
{
    public interface IServiceFieldHandlerFactory
    {
        bool TypeSupported(Type type);
        IFieldHandler CreateFromType(Type type);
        IFieldHandler CreateFromName(string handlerName, byte[] configuration);
    }
}