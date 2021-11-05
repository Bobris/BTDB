using System;
using System.Linq;
using BTDB.IL;

namespace BTDB.FieldHandler;

public class DefaultFieldHandlerLogger : IFieldHandlerLogger
{
    readonly Action<string> _loggerFunction;

    public DefaultFieldHandlerLogger(Action<string>? loggerFunction = null)
    {
        _loggerFunction = loggerFunction ?? Console.WriteLine;
    }

    public void ReportTypeIncompatibility(Type? sourceType, IFieldHandler source, Type targetType,
        IFieldHandler? target)
    {
        if (target != null)
        {
            _loggerFunction.Invoke("Cannot load " + source + " into " + target);
        }
        else if (sourceType != null)
        {
            _loggerFunction.Invoke(
                "Cannot load " + sourceType.ToSimpleName().Split("__").First() + " into " +
                targetType.ToSimpleName().Split("__").First());
        }
        else
        {
            _loggerFunction.Invoke("Cannot load " + source + " into " + targetType.ToSimpleName());
        }
    }
}
