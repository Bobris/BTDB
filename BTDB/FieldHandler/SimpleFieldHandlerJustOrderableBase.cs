using System;
using System.Reflection;

namespace BTDB.FieldHandler;

public class SimpleFieldHandlerJustOrderableBase : SimpleFieldHandlerBase
{
    public SimpleFieldHandlerJustOrderableBase(string name, MethodInfo loader, MethodInfo skipper, MethodInfo saver,
        SkipReaderCtxFunc skipReader)
        : base(name, loader, skipper, saver, skipReader)
    {
    }

    public override bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        if ((options & FieldHandlerOptions.Orderable) == 0) return false;
        return base.IsCompatibleWith(type, options);
    }
}
