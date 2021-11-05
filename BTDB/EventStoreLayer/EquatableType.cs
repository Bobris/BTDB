using System;

namespace BTDB.EventStoreLayer;

readonly struct EquatableType : IEquatable<EquatableType>
{
    public EquatableType(Type value)
    {
        Value = value;
    }

    public static implicit operator Type(EquatableType v) => v.Value;
    public static implicit operator EquatableType(Type v) => new EquatableType(v);

    public readonly Type Value;

    public bool Equals(EquatableType other)
    {
        return Value == other.Value;
    }

    public override bool Equals(object? obj)
    {
        return obj is EquatableType other && Equals(other);
    }

    public override int GetHashCode()
    {
        return Value.GetHashCode();
    }

    public static bool operator ==(EquatableType left, EquatableType right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(EquatableType left, EquatableType right)
    {
        return !left.Equals(right);
    }
}
