using System;
using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC;

public struct KeyAndType : IEquatable<KeyAndType>
{
    static readonly IEqualityComparer<object> KeyComparer = EqualityComparer<object>.Default;

    public KeyAndType(object? key, Type type)
    {
        Key = key;
        Type = type;
    }

    public readonly object? Key;
    public readonly Type Type;

    public bool Equals(KeyAndType other)
    {
        return Type == other.Type && KeyComparer.Equals(Key, other.Key);
    }

    public override int GetHashCode()
    {
        if (Key == null) return Type.GetHashCode();
        return Key.GetHashCode() * 33 + Type.GetHashCode();
    }

    public override string ToString()
    {
        return Type.ToSimpleName() + ((Key != null) ? " with key " + Key.ToString() : "");
    }
}
