using System;

namespace BTDB.ODBLayer;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public class PrimaryKeyAttribute : Attribute
{
    public uint Order { get; set; }

    public PrimaryKeyAttribute(uint order = 0)
    {
        Order = order;
    }
}
