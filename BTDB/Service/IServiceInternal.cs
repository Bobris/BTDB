using System;
using BTDB.FieldHandler;

namespace BTDB.Service
{
    public interface IServiceInternal: IFieldHandlerFactoryProvider
    {
        string RegisterType(Type type);
        Type TypeByName(string name);
    }
}