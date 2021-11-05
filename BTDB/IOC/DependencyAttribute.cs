using System;
using BTDB.ODBLayer;

namespace BTDB.IOC;

[AttributeUsage(AttributeTargets.Property)]
public class DependencyAttribute : NotStoredAttribute
{
    public readonly string? Name;

    public DependencyAttribute(string? name = null)
    {
        Name = name;
    }
}
