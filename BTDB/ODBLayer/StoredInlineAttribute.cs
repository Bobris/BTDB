using System;

namespace BTDB.ODBLayer;

[Obsolete("It is now default so just delete StoredInline attribute")]
[AttributeUsage(AttributeTargets.Class)]
public class StoredInlineAttribute : Attribute
{
}
