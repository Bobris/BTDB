using System;

namespace BTDB.FieldHandler;

public class DefaultFieldHandlerFactory : IFieldHandlerFactory
{
    readonly IFieldHandlerFactoryProvider _provider;

    public DefaultFieldHandlerFactory(IFieldHandlerFactoryProvider provider)
    {
        _provider = provider;
    }

    public virtual bool TypeSupported(Type type)
    {
        if (EnumFieldHandler.IsCompatibleWith(type)) return true;
        foreach (var fieldHandler in BasicSerializersFactory.FieldHandlers)
        {
            if (fieldHandler.IsCompatibleWith(type, FieldHandlerOptions.None)) return true;
        }
        if (ListFieldHandler.IsCompatibleWith(type)) return true;
        if (DictionaryFieldHandler.IsCompatibleWith(type)) return true;
        if (NullableFieldHandler.IsCompatibleWith(type)) return true;
        if (TupleFieldHandler.IsCompatibleWith(type)) return true;
        return false;
    }

    public virtual IFieldHandler? CreateFromType(Type type, FieldHandlerOptions options)
    {
        if (EnumFieldHandler.IsCompatibleWith(type)) return new EnumFieldHandler(type);
        foreach (var fieldHandler in BasicSerializersFactory.FieldHandlers)
        {
            if (fieldHandler.IsCompatibleWith(type, options)) return fieldHandler;
        }
        if (ListFieldHandler.IsCompatibleWith(type)) return new ListFieldHandler(_provider.FieldHandlerFactory, _provider.TypeConvertorGenerator, type);
        if (DictionaryFieldHandler.IsCompatibleWith(type)) return new DictionaryFieldHandler(_provider.FieldHandlerFactory, _provider.TypeConvertorGenerator, type);
        if (NullableFieldHandler.IsCompatibleWith(type)) return new NullableFieldHandler(_provider.FieldHandlerFactory, _provider.TypeConvertorGenerator, type);
        if (TupleFieldHandler.IsCompatibleWith(type))
            return new TupleFieldHandler(_provider.FieldHandlerFactory, _provider.TypeConvertorGenerator, type,
                options);
        return null;
    }

    public virtual IFieldHandler? CreateFromName(string handlerName, byte[] configuration, FieldHandlerOptions options)
    {
        IFieldHandler fallbackFieldHandler = null;
        foreach (var fieldHandler in BasicSerializersFactory.FieldHandlers)
        {
            if (fieldHandler.Name == handlerName)
            {
                fallbackFieldHandler = fieldHandler;
                if (fieldHandler.IsCompatibleWith(fieldHandler.HandledType()!, options))
                    return fieldHandler;
            }
        }
        if (fallbackFieldHandler != null)
            return fallbackFieldHandler;
        if (handlerName == EnumFieldHandler.HandlerName) return new EnumFieldHandler(configuration);
        if (handlerName == ListFieldHandler.HandlerName) return new ListFieldHandler(_provider.FieldHandlerFactory, _provider.TypeConvertorGenerator, configuration);
        if (handlerName == DictionaryFieldHandler.HandlerName) return new DictionaryFieldHandler(_provider.FieldHandlerFactory, _provider.TypeConvertorGenerator, configuration);
        if (handlerName == NullableFieldHandler.HandlerName) return new NullableFieldHandler(_provider.FieldHandlerFactory, _provider.TypeConvertorGenerator, configuration);
        if (handlerName == TupleFieldHandler.HandlerName)
            return new TupleFieldHandler(_provider.FieldHandlerFactory, _provider.TypeConvertorGenerator,
                configuration, options);
        return null;
    }
}
