using System;

namespace BTDB.ODBLayer;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public class PrimaryKeyAttribute : Attribute
{
    public uint Order { get; set; }

    public bool InKeyValue { get; set; }

    public PrimaryKeyAttribute(uint order = 0, bool inKeyValue = false)
    {
        Order = order;
        InKeyValue = inKeyValue;
    }
}
