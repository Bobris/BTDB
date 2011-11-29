using System;

namespace BTDB.ODBLayer
{
    [AttributeUsage(AttributeTargets.Property)]
    public class NotStoredAttribute : Attribute
    {
    }
}
