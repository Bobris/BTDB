using System;

namespace BTDB.ODBLayer;

[Obsolete("It is now default so just delete StoredInline attribute")]
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public class StoredInlineAttribute : Attribute
{
}
