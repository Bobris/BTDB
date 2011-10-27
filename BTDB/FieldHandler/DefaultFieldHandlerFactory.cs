using System;

namespace BTDB.FieldHandler
{
    public class DefaultFieldHandlerFactory : IFieldHandlerFactory
    {
        static readonly IFieldHandler[] FieldHandlers = new IFieldHandler[]
            {
                new StringOrderableFieldHandler(), 
                new StringFieldHandler(),
                new Uint8FieldHandler(), 
                new Int8OrderableFieldHandler(), 
                new Int8FieldHandler(), 
                new SignedFieldHandler(),
                new UnsignedFieldHandler(),
                new BoolFieldHandler(),
                new DoubleFieldHandler(),
                new DecimalFieldHandler(),
                new DateTimeFieldHandler(),
                new TimeSpanFieldHandler(), 
                new GuidFieldHandler(),
                new ByteArrayLastFieldHandler(), 
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
                if (fieldHandler.IsCompatibleWith(type, FieldHandlerOptions.None)) return true;
            }
            if (ListFieldHandler.IsCompatibleWith(type)) return true;
            if (DictionaryFieldHandler.IsCompatibleWith(type)) return true;
            return false;
        }

        public virtual IFieldHandler CreateFromType(Type type, FieldHandlerOptions options)
        {
            if (EnumFieldHandler.IsCompatibleWith(type)) return new EnumFieldHandler(type);
            foreach (var fieldHandler in FieldHandlers)
            {
                if (fieldHandler.IsCompatibleWith(type, options)) return fieldHandler;
            }
            if (ListFieldHandler.IsCompatibleWith(type)) return new ListFieldHandler(_provider.FieldHandlerFactory, _provider.TypeConvertorGenerator, type);
            if (DictionaryFieldHandler.IsCompatibleWith(type)) return new DictionaryFieldHandler(_provider.FieldHandlerFactory, _provider.TypeConvertorGenerator, type);
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
            if (handlerName == DictionaryFieldHandler.HandlerName) return new DictionaryFieldHandler(_provider.FieldHandlerFactory, _provider.TypeConvertorGenerator, configuration);
            return null;
        }
    }
}
