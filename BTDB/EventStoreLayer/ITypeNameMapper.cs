using System;

namespace BTDB.EventStoreLayer;

public interface ITypeNameMapper
{
    string ToName(Type type);
    Type? ToType(string name);
}
