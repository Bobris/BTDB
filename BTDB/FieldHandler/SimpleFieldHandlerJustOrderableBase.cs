using System;
using System.Reflection;

namespace BTDB.FieldHandler
{
    public abstract class SimpleFieldHandlerJustOrderableBase : SimpleFieldHandlerBase
    {
        protected SimpleFieldHandlerJustOrderableBase(MethodInfo loader, MethodInfo skipper, MethodInfo saver):base(loader,skipper,saver)
        {
        }

        public override bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            if (!options.HasFlag(FieldHandlerOptions.Orderable)) return false;
            return base.IsCompatibleWith(type, options);
        }
    }
}