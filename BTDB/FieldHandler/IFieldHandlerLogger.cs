using System;

namespace BTDB.FieldHandler;

public interface IFieldHandlerLogger
{
    void ReportTypeIncompatibility(Type? sourceType, IFieldHandler source, Type targetType, IFieldHandler? target);
}
