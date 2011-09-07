using System;

namespace BTDB.ODBLayer.FieldHandlerIface
{
    public interface IFieldHandlerFactory
    {
        IFieldHandler CreateFromType(Type type);
        IFieldHandler CreateFromName(string handlerName, byte[] configuration);
    }
}