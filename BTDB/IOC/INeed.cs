using System;
using System.Reflection;

namespace BTDB.IOC;

interface INeed
{
    NeedKind Kind { get; }
    Type ClrType { get; }
    bool Optional { get; }
    object? OptionalValue { get; }
    bool ForcedKey { get; }
    object? Key { get; }
    PropertyInfo? PropertyInfo { get; }
}
