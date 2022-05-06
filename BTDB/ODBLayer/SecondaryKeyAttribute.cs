using System;

namespace BTDB.ODBLayer;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
public class SecondaryKeyAttribute : Attribute
{
    public string Name { get; set; }
    public uint Order { get; set; }
    public uint IncludePrimaryKeyOrder { get; set; }

    public SecondaryKeyAttribute(string name)
    {
        Name = name;
    }

    internal static bool Equal(SecondaryKeyAttribute a, SecondaryKeyAttribute b)
    {
        if (a.Name != b.Name) return false;
        if (a.Order != b.Order) return false;
        if (a.IncludePrimaryKeyOrder != b.IncludePrimaryKeyOrder) return false;
        return true;
    }
}
