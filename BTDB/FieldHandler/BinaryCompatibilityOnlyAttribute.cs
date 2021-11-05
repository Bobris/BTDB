using System;

namespace BTDB.FieldHandler;

[Obsolete("It does need to be used anymore as it is default behaviour")]
[AttributeUsage(AttributeTargets.Enum)]
public class BinaryCompatibilityOnlyAttribute : Attribute
{
}
