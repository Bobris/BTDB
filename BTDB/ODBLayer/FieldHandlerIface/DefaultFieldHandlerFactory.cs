using System;
using System.Reflection;
using BTDB.ODBLayer.FieldHandlerImpl;

namespace BTDB.ODBLayer.FieldHandlerIface
{
    public class DefaultFieldHandlerFactory : IFieldHandlerFactory
    {
        static readonly IFieldHandler[] FieldHandlers = new IFieldHandler[]
            {
                new StringFieldHandler(),
                new SignedFieldHandler(),
                new UnsignedFieldHandler(),
                new BoolFieldHandler(),
                new DoubleFieldHandler(),
                new DecimalFieldHandler(),
                new DateTimeFieldHandler(),
                new GuidFieldHandler(),
                new DBObjectFieldHandler(),
            };

        public IFieldHandler CreateFromType(Type type)
        {
            if (EnumFieldHandler.IsCompatibleWith(type)) return new EnumFieldHandler(type);
            foreach (var fieldHandler in FieldHandlers)
            {
                if (fieldHandler.IsCompatibleWith(type)) return fieldHandler;
            }
            return null;
        }

        public IFieldHandler CreateFromName(string handlerName, byte[] configuration)
        {
            foreach (var fieldHandler in FieldHandlers)
            {
                if (fieldHandler.Name == handlerName) return fieldHandler;
            }
            if (handlerName == EnumFieldHandler.HandlerName) return new EnumFieldHandler(configuration);
            return null;
        }
    }
}