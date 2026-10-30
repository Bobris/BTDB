using System;

namespace BTDB.ODBLayer;

public interface IType2NameRegistry
{
    string RegisterType(Type type, string asName);

    /// Makes objects stored under <paramref name="alias"/>, such as by an earlier name of the type, load as the type,
    /// without storing it under that name. A type registered under the alias by <see cref="RegisterType"/> takes
    /// precedence. Returns false when another type has the alias already, or when the registry keeps no aliases.
    bool RegisterAlias(Type type, string alias) => false;

    Type? FindTypeByName(string name);
    string? FindNameByType(Type type);
}
