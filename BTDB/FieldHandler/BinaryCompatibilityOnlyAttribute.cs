using System;

namespace BTDB.FieldHandler
{
    [AttributeUsage(AttributeTargets.Enum)]
    public class BinaryCompatibilityOnlyAttribute : Attribute
    {
    }
}