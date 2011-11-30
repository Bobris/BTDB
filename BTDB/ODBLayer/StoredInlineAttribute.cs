using System;

namespace BTDB.ODBLayer
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
    public class StoredInlineAttribute : Attribute
    {
    }
}