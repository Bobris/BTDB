using System;
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
            };

        public IFieldHandler CreateFromType(Type type)
        {
            if (EnumFieldHandler.IsCompatibleWith(type)) return new EnumFieldHandler(type);
            foreach (var fieldHandler in FieldHandlers)
            {
                if (fieldHandler.IsCompatibleWith(type)) return fieldHandler;
            }
            if (DBObjectFieldHandler.IsCompatibleWith(type)) return new DBObjectFieldHandler(type);
            return null;
        }

        public IFieldHandler CreateFromName(string handlerName, byte[] configuration)
        {
            foreach (var fieldHandler in FieldHandlers)
            {
                if (fieldHandler.Name == handlerName) return fieldHandler;
            }
            if (handlerName == DBObjectFieldHandler.HandlerName) return new DBObjectFieldHandler(configuration);
            if (handlerName == EnumFieldHandler.HandlerName) return new EnumFieldHandler(configuration);
            return null;
        }
    }
}