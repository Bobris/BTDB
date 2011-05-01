using System;
using System.Reflection;

namespace BTDB.ODBLayer
{
    public interface IFieldHandlerFactory
    {
        IFieldHandler CreateFromProperty(string tableName, Type clientType, PropertyInfo property);
        IFieldHandler CreateFromName(string tableName, string fieldName, string handlerName, byte[] configuration);
    }
}