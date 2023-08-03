using System;

namespace BTDB.ODBLayer;

[AttributeUsage(AttributeTargets.Property)]
public class InKeyValueAttribute : Attribute
{
    public uint Order { get; set; }

    public InKeyValueAttribute(uint order = 0)
    {
        Order = order;
    }
}
