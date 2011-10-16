using System;
using BTDB.FieldHandler;

namespace BTDB.Service
{
    public class DefaultServiceFieldHandlerFactory : DefaultFieldHandlerFactory
    {
        readonly IServiceInternal _service;

        public DefaultServiceFieldHandlerFactory(IServiceInternal service)
            : base(service)
        {
            _service = service;
        }

        public override bool TypeSupported(Type type)
        {
            if (base.TypeSupported(type)) return true;
            if (ServiceObjectFieldHandler.IsCompatibleWith(type)) return true;
            return false;
        }

        public override IFieldHandler CreateFromType(Type type)
        {
            var result = base.CreateFromType(type);
            if (result != null) return result;
            if (ServiceObjectFieldHandler.IsCompatibleWith(type)) return new ServiceObjectFieldHandler(_service, type);
            return null;
        }

        public override IFieldHandler CreateFromName(string handlerName, byte[] configuration)
        {
            var result = base.CreateFromName(handlerName, configuration);
            if (result != null) return result;
            if (handlerName == ServiceObjectFieldHandler.HandlerName) return new ServiceObjectFieldHandler(_service, configuration);
            return null;
        }
    }
}