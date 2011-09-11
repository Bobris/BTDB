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
                new ByteArrayFieldHandler(), 
            };

        readonly IObjectDB _owner;

        public DefaultFieldHandlerFactory(IObjectDB owner)
        {
            _owner = owner;
        }

        public IFieldHandler CreateFromType(Type type)
        {
            if (EnumFieldHandler.IsCompatibleWith(type)) return new EnumFieldHandler(type);
            foreach (var fieldHandler in FieldHandlers)
            {
                if (fieldHandler.IsCompatibleWith(type)) return fieldHandler;
            }
            if (DBObjectFieldHandler.IsCompatibleWith(type)) return new DBObjectFieldHandler(_owner, type);
            if (ListFieldHandler.IsCompatibleWith(type)) return new ListFieldHandler(_owner.FieldHandlerFactory, _owner.TypeConvertorGenerator, type);
            return null;
        }

        public IFieldHandler CreateFromName(string handlerName, byte[] configuration)
        {
            foreach (var fieldHandler in FieldHandlers)
            {
                if (fieldHandler.Name == handlerName) return fieldHandler;
            }
            if (handlerName == DBObjectFieldHandler.HandlerName) return new DBObjectFieldHandler(_owner, configuration);
            if (handlerName == ListFieldHandler.HandlerName) return new ListFieldHandler(_owner.FieldHandlerFactory, _owner.TypeConvertorGenerator, configuration);
            if (handlerName == EnumFieldHandler.HandlerName) return new EnumFieldHandler(configuration);
            return null;
        }
    }
}