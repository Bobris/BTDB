using System;

namespace BTDB.ODBLayer;

[AttributeUsage(AttributeTargets.Property)]
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
