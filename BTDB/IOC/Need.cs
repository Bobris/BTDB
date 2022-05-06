using System;
using System.Reflection;

namespace BTDB.IOC;

class Need : INeed
{
    public NeedKind Kind { get; set; }
    public Type? ClrType { get; set; }
    public bool Optional { get; set; }
    public bool ForcedKey { get; set; }
    public object? Key { get; set; }
    public Type? ParentType { get; set; }
    public object? OptionalValue { get; set; }
    public PropertyInfo? PropertyInfo { get; set; }

    public static readonly INeed ContainerNeed;

    static Need()
    {
        ContainerNeed = new Need
        {
            ClrType = typeof(IContainer),
            Optional = false,
            Kind = NeedKind.Internal,
            ParentType = null,
            ForcedKey = false,
            Key = null
        };
    }

}
