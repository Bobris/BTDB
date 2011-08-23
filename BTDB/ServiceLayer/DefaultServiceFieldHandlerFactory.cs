using System;
using BTDB.ODBLayer.FieldHandlerIface;
using BTDB.ODBLayer.FieldHandlerImpl;

namespace BTDB.ServiceLayer
{
    public class DefaultServiceFieldHandlerFactory: IServiceFieldHandlerFactory
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

        public bool TypeSupported(Type type)
        {
            if (EnumFieldHandler.IsCompatibleWith(type)) return true;
            foreach (var fieldHandler in FieldHandlers)
            {
                if (fieldHandler.IsCompatibleWith(type)) return true;
            }
            return false;
        }

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