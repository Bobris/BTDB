using System;

namespace BTDB.FieldHandler;

[Flags]
public enum FieldHandlerOptions
{
    None = 0,
    Orderable = 1,
    AtEndOfStream = 2,
}
