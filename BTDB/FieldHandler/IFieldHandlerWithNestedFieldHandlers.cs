using System.Collections.Generic;

namespace BTDB.FieldHandler;

public interface IFieldHandlerWithNestedFieldHandlers
{
    IEnumerable<IFieldHandler> EnumerateNestedFieldHandlers();
}
