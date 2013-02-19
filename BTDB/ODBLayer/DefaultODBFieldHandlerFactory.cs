using System;
using BTDB.FieldHandler;

namespace BTDB.ODBLayer
{
    public class DefaultODBFieldHandlerFactory : DefaultFieldHandlerFactory
    {
        readonly IObjectDB _odb;

        public DefaultODBFieldHandlerFactory(IObjectDB odb)
            : base(odb)
        {
            _odb = odb;
        }

        public override bool TypeSupported(Type type)
        {
            if (ODBDictionaryFieldHandler.IsCompatibleWithStatic(type, FieldHandlerOptions.None)) return true;
            if (base.TypeSupported(type)) return true;
            if (DBObjectFieldHandler.IsCompatibleWith(type)) return true;
            return false;
        }

        public override IFieldHandler CreateFromType(Type type, FieldHandlerOptions options)
        {
            if (ODBDictionaryFieldHandler.IsCompatibleWithStatic(type, options)) return new ODBDictionaryFieldHandler(_odb, type);
            var result = base.CreateFromType(type, options);
            if (result != null) return result;
            if (DBObjectFieldHandler.IsCompatibleWith(type)) return new DBObjectFieldHandler(_odb, type);
            return null;
        }

        public override IFieldHandler CreateFromName(string handlerName, byte[] configuration, FieldHandlerOptions options)
        {
            if (handlerName == ODBDictionaryFieldHandler.HandlerName) return new ODBDictionaryFieldHandler(_odb, configuration);
            var result = base.CreateFromName(handlerName, configuration, options);
            if (result != null) return result;
            if (handlerName == DBObjectFieldHandler.HandlerName) return new DBObjectFieldHandler(_odb, configuration);
            return null;
        }
    }
}