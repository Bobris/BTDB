using System;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class DefaultFieldHandlerFactory: IFieldHandlerFactory
    {
        static readonly IFieldHandler[] FieldHandlers = new IFieldHandler[]
            {
                new StringFieldHandler(),
                new Uint8FieldHandler(), 
                new Int8FieldHandler(), 
                new SignedFieldHandler(),
                new UnsignedFieldHandler(),
                new BoolFieldHandler(),
                new DoubleFieldHandler(),
                new DecimalFieldHandler(),
                new DateTimeFieldHandler(),
                new GuidFieldHandler(),
                new ByteArrayFieldHandler(),
            };

        readonly IFieldHandlerFactoryProvider _provider;

        public DefaultFieldHandlerFactory(IFieldHandlerFactoryProvider provider)
        {
            _provider = provider;
        }

        public virtual bool TypeSupported(Type type)
        {
            if (EnumFieldHandler.IsCompatibleWith(type)) return true;
            foreach (var fieldHandler in FieldHandlers)
            {
                if (fieldHandler.IsCompatibleWith(type)) return true;
            }
            if (ListFieldHandler.IsCompatibleWith(type)) return true;
            return false;
        }

        public virtual IFieldHandler CreateFromType(Type type)
        {
            if (EnumFieldHandler.IsCompatibleWith(type)) return new EnumFieldHandler(type);
            foreach (var fieldHandler in FieldHandlers)
            {
                if (fieldHandler.IsCompatibleWith(type)) return fieldHandler;
            }
            if (ListFieldHandler.IsCompatibleWith(type)) return new ListFieldHandler(_provider.FieldHandlerFactory, _provider.TypeConvertorGenerator, type);
            return null;
        }

        public virtual IFieldHandler CreateFromName(string handlerName, byte[] configuration)
        {
            foreach (var fieldHandler in FieldHandlers)
            {
                if (fieldHandler.Name == handlerName) return fieldHandler;
            }
            if (handlerName == EnumFieldHandler.HandlerName) return new EnumFieldHandler(configuration);
            if (handlerName == ListFieldHandler.HandlerName) return new ListFieldHandler(_provider.FieldHandlerFactory, _provider.TypeConvertorGenerator, configuration);
            return null;
        }
    }
}