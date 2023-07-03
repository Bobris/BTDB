using System;

namespace BTDB.ODBLayer;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public class OnBeforeRemoveAttribute : Attribute
{
    public OnBeforeRemoveAttribute()
    {
    }
}
