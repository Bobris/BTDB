using System;

namespace BTDB.ODBLayer;

public interface IType2NameRegistry
{
    string RegisterType(Type type, string asName);
    Type? FindTypeByName(string name);
    string? FindNameByType(Type type);
}
