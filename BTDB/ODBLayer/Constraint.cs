using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Collections;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public delegate bool PredicateSpanChar(in ReadOnlySpan<char> value);

public interface IConstraint
{
    public enum MatchType
    {
        NoPrefix,
        Prefix,
        Exact
    }

    public enum MatchResult : uint
    {
        No = 0,
        Yes = 1,
        NoAfterLast = 2,
        YesSkipNext = 3 // This cannot be returned from MatchType.Exact
    }

    static bool IsYesLike(MatchResult result)
    {
        return ((uint)result & 1) != 0;
    }

    public bool IsAnyConstraint();
    public bool IsSimpleExact();
    public MatchType Prepare(ref MemWriter buffer);

    // Will be called only for Prefix and Exact MatchTypes, for Exact it have to write full part of key
    public void WritePrefix(ref MemWriter writer, in MemWriter buffer);

    // return true if match was successful and in ALL CASES SKIP reader after this field
    // when MatchType is Exact this method does not need to be called at all if part of key matches written prefix
    public MatchResult Match(ref MemReader reader, in MemWriter buffer);

    // return true if match was successful, reader does not need to be updated (that's why it must at least as fast as Match)
    // when MatchType is Exact this method does not need to be called at all if part of key matches written prefix
    public MatchResult MatchLast(ref MemReader reader, in MemWriter buffer);
}

public abstract class Constraint<T> : IConstraint
{
    public virtual bool IsAnyConstraint() => false;
    public abstract bool IsSimpleExact();

    public abstract IConstraint.MatchType Prepare(ref MemWriter buffer);
    public abstract void WritePrefix(ref MemWriter writer, in MemWriter buffer);
    public abstract IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer);

    public virtual IConstraint.MatchResult MatchLast(ref MemReader reader, in MemWriter buffer)
    {
        return Match(ref reader, buffer);
    }

    public static IConstraint.MatchResult AsMatchResult(bool value)
    {
        return value ? IConstraint.MatchResult.Yes : IConstraint.MatchResult.No;
    }

    public static readonly Constraint<T> Any;

    static Constraint()
    {
        if (typeof(T) == typeof(bool))
        {
            Any = Unsafe.As<Constraint<T>>(new ConstraintBoolAny());
        }
        else if (typeof(T).IsEnum)
        {
            if (SignedFieldHandler.IsCompatibleWith(typeof(T).GetEnumUnderlyingType()))
            {
                Any = new ConstraintSignedAny<T>();
            }
            else
            {
                Any = new ConstraintUnsignedAny<T>();
            }
        }
        else if (typeof(T) == typeof(string))
        {
            Any = Unsafe.As<Constraint<T>>(new ConstraintStringAny());
        }
        else if (typeof(T) == typeof(DateTime))
        {
            Any = Unsafe.As<Constraint<T>>(new ConstraintDateTimeAny());
        }
        else if (typeof(T) == typeof(Guid))
        {
            Any = Unsafe.As<Constraint<T>>(new ConstraintGuidAny());
        }
        else if (SignedFieldHandler.IsCompatibleWith(typeof(T)))
        {
            Any = new ConstraintSignedAny<T>();
        }
        else if (UnsignedFieldHandler.IsCompatibleWith(typeof(T)))
        {
            Any = new ConstraintUnsignedAny<T>();
        }
        else if (Nullable.GetUnderlyingType(typeof(T)) is { } ut)
        {
            if (ut.IsEnum)
            {
                if (SignedFieldHandler.IsCompatibleWith(ut.GetEnumUnderlyingType()))
                {
                    Any = new ConstraintNullableAny<T>(new ConstraintSignedAny<T>());
                }
                else
                {
                    Any = new ConstraintNullableAny<T>(new ConstraintUnsignedAny<T>());
                }
            }
            else if (ut == typeof(bool))
            {
                Any = new ConstraintNullableAny<T>(new ConstraintBoolAny());
            }
            else if (ut == typeof(DateTime))
            {
                Any = new ConstraintNullableAny<T>(new ConstraintDateTimeAny());
            }
            else if (ut == typeof(Guid))
            {
                Any = new ConstraintNullableAny<T>(new ConstraintGuidAny());
            }
            else if (SignedFieldHandler.IsCompatibleWith(ut))
            {
                Any = new ConstraintNullableAny<T>(new ConstraintSignedAny<T>());
            }
            else if (UnsignedFieldHandler.IsCompatibleWith(ut))
            {
                Any = new ConstraintNullableAny<T>(new ConstraintUnsignedAny<T>());
            }
            else
            {
                Any = new ConstraintNotImplemented<T>();
            }
        }
        else if (typeof(T) == typeof(List<string>) || typeof(T) == typeof(IList<string>))
        {
            Any = new ConstraintListStringAny<T>();
        }
        else
        {
            Any = new ConstraintNotImplemented<T>();
        }
    }
}

public class ConstraintListStringAny<T> : ConstraintAny<T>
{
    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        var count = reader.ReadVUInt32();
        for (var i = 0; i < count; i++)
        {
            reader.SkipString();
        }

        return IConstraint.MatchResult.Yes;
    }
}

public class ConstraintNullableAny<T> : Constraint<T>
{
    readonly IConstraint _anyT;

    public ConstraintNullableAny(IConstraint anyT)
    {
        Debug.Assert(anyT.IsAnyConstraint());
        _anyT = anyT;
    }

    public override bool IsAnyConstraint() => true;
    public override bool IsSimpleExact() => false;

    public override IConstraint.MatchType Prepare(ref MemWriter buffer)
    {
        return _anyT.Prepare(ref buffer);
    }

    public override void WritePrefix(ref MemWriter writer, in MemWriter buffer)
    {
    }

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        if (reader.ReadUInt8() != 0)
        {
            return _anyT.Match(ref reader, buffer);
        }

        return IConstraint.MatchResult.Yes;
    }

    public override IConstraint.MatchResult MatchLast(ref MemReader reader, in MemWriter buffer)
    {
        if (reader.ReadUInt8() != 0)
        {
            return _anyT.MatchLast(ref reader, buffer);
        }

        return IConstraint.MatchResult.Yes;
    }
}

public class ConstraintNotImplemented<T> : Constraint<T>
{
    public override bool IsSimpleExact()
    {
        throw new NotImplementedException();
    }

    public override IConstraint.MatchType Prepare(ref MemWriter buffer)
    {
        throw new NotImplementedException();
    }

    public override void WritePrefix(ref MemWriter writer, in MemWriter buffer)
    {
        throw new NotImplementedException();
    }

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        throw new NotImplementedException();
    }
}

public static partial class Constraint
{
    public static Constraint<T> First<T>(Constraint<T> of)
    {
        return new FirstConstraint<T>(of);
    }

    public static Constraint<T> Exact<T>(T value)
    {
        if (typeof(T) == typeof(bool))
            return Unsafe.As<Constraint<T>>(new ConstraintBoolExact(Unsafe.As<T, bool>(ref value)));
        if (typeof(T) == typeof(bool?))
        {
            ref var val = ref Unsafe.As<T, bool?>(ref value);
            if (val.HasValue) return new NullableExactValue<T>(new ConstraintBoolExact(val.Value));
            return new NullableExactNull<T>(new ConstraintBoolAny());
        }

        if (typeof(T) == typeof(string))
            return Unsafe.As<Constraint<T>>(new ConstraintStringExact(Unsafe.As<T, string>(ref value)));
        if (typeof(T) == typeof(System.Guid))
            return Unsafe.As<Constraint<T>>(new ConstraintGuidExact(Unsafe.As<T, System.Guid>(ref value)));
        if (typeof(T) == typeof(System.Guid?))
        {
            ref var val = ref Unsafe.As<T, System.Guid?>(ref value);
            if (val.HasValue) return new NullableExactValue<T>(new ConstraintGuidExact(val.Value));
            return new NullableExactNull<T>(new ConstraintGuidAny());
        }

        if (typeof(T) == typeof(System.DateTime))
            return Unsafe.As<Constraint<T>>(new ConstraintDateTimeExact(Unsafe.As<T, System.DateTime>(ref value)));
        if (typeof(T) == typeof(System.DateTime?))
        {
            ref var val = ref Unsafe.As<T, System.DateTime?>(ref value);
            if (val.HasValue) return new NullableExactValue<T>(new ConstraintDateTimeExact(val.Value));
            return new NullableExactNull<T>(new ConstraintDateTimeAny());
        }

        if (typeof(T) == typeof(sbyte))
            return Unsafe.As<Constraint<T>>(new ConstraintSignedExact(Unsafe.As<T, sbyte>(ref value)));
        if (typeof(T) == typeof(sbyte?))
        {
            ref var val = ref Unsafe.As<T, sbyte?>(ref value);
            if (val.HasValue) return new NullableExactValue<T>(new ConstraintSignedExact(val.Value));
            return new NullableExactNull<T>(new ConstraintSignedAny<sbyte>());
        }

        if (typeof(T) == typeof(short))
            return Unsafe.As<Constraint<T>>(new ConstraintSignedExact(Unsafe.As<T, short>(ref value)));
        if (typeof(T) == typeof(short?))
        {
            ref var val = ref Unsafe.As<T, short?>(ref value);
            if (val.HasValue) return new NullableExactValue<T>(new ConstraintSignedExact(val.Value));
            return new NullableExactNull<T>(new ConstraintSignedAny<short>());
        }

        if (typeof(T) == typeof(int))
            return Unsafe.As<Constraint<T>>(new ConstraintSignedExact(Unsafe.As<T, int>(ref value)));
        if (typeof(T) == typeof(int?))
        {
            ref var val = ref Unsafe.As<T, int?>(ref value);
            if (val.HasValue) return new NullableExactValue<T>(new ConstraintSignedExact(val.Value));
            return new NullableExactNull<T>(new ConstraintSignedAny<int>());
        }

        if (typeof(T) == typeof(long))
            return Unsafe.As<Constraint<T>>(new ConstraintSignedExact(Unsafe.As<T, long>(ref value)));
        if (typeof(T) == typeof(long?))
        {
            ref var val = ref Unsafe.As<T, long?>(ref value);
            if (val.HasValue) return new NullableExactValue<T>(new ConstraintSignedExact(val.Value));
            return new NullableExactNull<T>(new ConstraintSignedAny<long>());
        }

        if (typeof(T) == typeof(byte))
            return Unsafe.As<Constraint<T>>(new ConstraintUnsignedExact(Unsafe.As<T, byte>(ref value)));
        if (typeof(T) == typeof(byte?))
        {
            ref var val = ref Unsafe.As<T, byte?>(ref value);
            if (val.HasValue) return new NullableExactValue<T>(new ConstraintUnsignedExact(val.Value));
            return new NullableExactNull<T>(new ConstraintUnsignedAny<byte>());
        }

        if (typeof(T) == typeof(ushort))
            return Unsafe.As<Constraint<T>>(new ConstraintUnsignedExact(Unsafe.As<T, ushort>(ref value)));
        if (typeof(T) == typeof(ushort?))
        {
            ref var val = ref Unsafe.As<T, ushort?>(ref value);
            if (val.HasValue) return new NullableExactValue<T>(new ConstraintUnsignedExact(val.Value));
            return new NullableExactNull<T>(new ConstraintUnsignedAny<ushort>());
        }

        if (typeof(T) == typeof(uint))
            return Unsafe.As<Constraint<T>>(new ConstraintUnsignedExact(Unsafe.As<T, uint>(ref value)));
        if (typeof(T) == typeof(uint?))
        {
            ref var val = ref Unsafe.As<T, uint?>(ref value);
            if (val.HasValue) return new NullableExactValue<T>(new ConstraintUnsignedExact(val.Value));
            return new NullableExactNull<T>(new ConstraintUnsignedAny<uint>());
        }

        if (typeof(T) == typeof(ulong))
            return Unsafe.As<Constraint<T>>(new ConstraintUnsignedExact(Unsafe.As<T, ulong>(ref value)));
        if (typeof(T) == typeof(ulong?))
        {
            ref var val = ref Unsafe.As<T, ulong?>(ref value);
            if (val.HasValue) return new NullableExactValue<T>(new ConstraintUnsignedExact(val.Value));
            return new NullableExactNull<T>(new ConstraintUnsignedAny<ulong>());
        }

        if (typeof(T).IsEnum)
        {
            var et = typeof(T).GetEnumUnderlyingType();
            if (et == typeof(sbyte))
                return Unsafe.As<Constraint<T>>(new ConstraintSignedExact(Unsafe.As<T, sbyte>(ref value)));
            if (et == typeof(short))
                return Unsafe.As<Constraint<T>>(new ConstraintSignedExact(Unsafe.As<T, short>(ref value)));
            if (et == typeof(int))
                return Unsafe.As<Constraint<T>>(new ConstraintSignedExact(Unsafe.As<T, int>(ref value)));
            if (Enum.GetUnderlyingType(typeof(T)) == typeof(long))
                return Unsafe.As<Constraint<T>>(new ConstraintSignedExact(Unsafe.As<T, long>(ref value)));
            if (Enum.GetUnderlyingType(typeof(T)) == typeof(byte))
                return Unsafe.As<Constraint<T>>(new ConstraintUnsignedExact(Unsafe.As<T, byte>(ref value)));
            if (Enum.GetUnderlyingType(typeof(T)) == typeof(ushort))
                return Unsafe.As<Constraint<T>>(new ConstraintUnsignedExact(Unsafe.As<T, ushort>(ref value)));
            if (Enum.GetUnderlyingType(typeof(T)) == typeof(uint))
                return Unsafe.As<Constraint<T>>(new ConstraintUnsignedExact(Unsafe.As<T, uint>(ref value)));
            if (Enum.GetUnderlyingType(typeof(T)) == typeof(ulong))
                return Unsafe.As<Constraint<T>>(new ConstraintUnsignedExact(Unsafe.As<T, ulong>(ref value)));
            throw new NotSupportedException("Enum with underlying type " +
                                            Enum.GetUnderlyingType(typeof(T)).ToSimpleName() + " is not supported");
        }

        if (Nullable.GetUnderlyingType(typeof(T)) is { IsEnum : true } ut)
        {
            var et = ut.GetEnumUnderlyingType();
            if (et == typeof(sbyte))
            {
                ref var val = ref Unsafe.As<T, sbyte?>(ref value);
                if (val.HasValue) return new NullableExactValue<T>(new ConstraintSignedExact(val.Value));
                return new NullableExactNull<T>(new ConstraintSignedAny<sbyte>());
            }

            if (et == typeof(short))
            {
                ref var val = ref Unsafe.As<T, short?>(ref value);
                if (val.HasValue) return new NullableExactValue<T>(new ConstraintSignedExact(val.Value));
                return new NullableExactNull<T>(new ConstraintSignedAny<short>());
            }

            if (et == typeof(int))
            {
                ref var val = ref Unsafe.As<T, int?>(ref value);
                if (val.HasValue) return new NullableExactValue<T>(new ConstraintSignedExact(val.Value));
                return new NullableExactNull<T>(new ConstraintSignedAny<int>());
            }

            if (et == typeof(long))
            {
                ref var val = ref Unsafe.As<T, long?>(ref value);
                if (val.HasValue) return new NullableExactValue<T>(new ConstraintSignedExact(val.Value));
                return new NullableExactNull<T>(new ConstraintSignedAny<long>());
            }

            if (et == typeof(byte))
            {
                ref var val = ref Unsafe.As<T, byte?>(ref value);
                if (val.HasValue) return new NullableExactValue<T>(new ConstraintUnsignedExact(val.Value));
                return new NullableExactNull<T>(new ConstraintUnsignedAny<byte>());
            }

            if (et == typeof(ushort))
            {
                ref var val = ref Unsafe.As<T, ushort?>(ref value);
                if (val.HasValue) return new NullableExactValue<T>(new ConstraintUnsignedExact(val.Value));
                return new NullableExactNull<T>(new ConstraintUnsignedAny<ushort>());
            }

            if (et == typeof(uint))
            {
                ref var val = ref Unsafe.As<T, uint?>(ref value);
                if (val.HasValue) return new NullableExactValue<T>(new ConstraintUnsignedExact(val.Value));
                return new NullableExactNull<T>(new ConstraintUnsignedAny<uint>());
            }

            if (et == typeof(ulong))
            {
                ref var val = ref Unsafe.As<T, ulong?>(ref value);
                if (val.HasValue) return new NullableExactValue<T>(new ConstraintUnsignedExact(val.Value));
                return new NullableExactNull<T>(new ConstraintUnsignedAny<ulong>());
            }

            throw new NotSupportedException("Nullable enum with underlying type " +
                                            et.ToSimpleName() + " is not supported");
        }

        throw new NotSupportedException("Exact with type " + typeof(T).ToSimpleName() + " is not supported");
    }

    public static partial class Bool
    {
        public static Constraint<bool> Exact(bool value) => new ConstraintBoolExact(value);
        public static readonly Constraint<bool> Any = Constraint<bool>.Any;
    }

    public static partial class DateTime
    {
        public static Constraint<System.DateTime> Exact(System.DateTime value) =>
            new ConstraintDateTimeExact(ForbidUnspecifiedKind(value));

        public static Constraint<System.DateTime> Predicate(Predicate<System.DateTime> predicate) =>
            new ConstraintDateTimePredicate(predicate);

        public static Constraint<System.DateTime> UpTo(System.DateTime value, bool including = true) =>
            new ConstraintDateTimeUpTo(ForbidUnspecifiedKind(value), including);

        public static Constraint<System.DateTime>
            Range(System.DateTime from, System.DateTime to, bool including = true) =>
            new ConstraintDateTimeRange(ForbidUnspecifiedKind(from), ForbidUnspecifiedKind(to), including);

        public static readonly Constraint<System.DateTime> Any = Constraint<System.DateTime>.Any;

        public static System.DateTime ForbidUnspecifiedKind(System.DateTime value)
        {
            if (value.Kind == DateTimeKind.Unspecified)
            {
                if (value == System.DateTime.MinValue)
                    value = System.DateTime.MinValue.ToUniversalTime();
                else if (value == System.DateTime.MaxValue)
                    value = System.DateTime.MaxValue.ToUniversalTime();
                else
                    throw new ArgumentOutOfRangeException(nameof(value),
                        "DateTime.Kind cannot be Unspecified");
            }

            return value;
        }
    }

    public static partial class Unsigned
    {
        public static Constraint<ulong> Exact(ulong value) => new ConstraintUnsignedExact(value);

        public static Constraint<ulong> Predicate(Predicate<ulong> predicate) =>
            new ConstraintUnsignedPredicate(predicate);

        public static Constraint<ulong> UpTo(ulong value, bool including = true) =>
            new ConstraintUnsignedUpTo(value, including);

        public static readonly Constraint<ulong> Any = Constraint<ulong>.Any;
    }

    public static partial class Signed
    {
        public static Constraint<long> Exact(long value) => new ConstraintSignedExact(value);

        public static Constraint<long> Predicate(Predicate<long> predicate) =>
            new ConstraintSignedPredicate(predicate);

        public static Constraint<long> UpTo(long value, bool including = true) =>
            new ConstraintSignedUpTo(value, including);

        public static readonly Constraint<long> Any = Constraint<long>.Any;
    }

    public static partial class Enum<T> where T : Enum
    {
        public static readonly bool IsSigned = SignedFieldHandler.IsCompatibleWith(typeof(T).GetEnumUnderlyingType());
        public static readonly Func<T, long>? ToLong;
        public static readonly Func<long, T>? FromLong;
        public static readonly Func<T, ulong>? ToUlong;
        public static readonly Func<ulong, T>? FromUlong;

        static Enum()
        {
            if (IsSigned)
            {
                var b = ILBuilder.Instance.NewMethod<Func<T, long>>("ToLong" + typeof(T).FullName);
                b.Generator
                    .Ldarg(0)
                    .ConvI8()
                    .Ret();
                ToLong = b.Create();
                var b2 = ILBuilder.Instance.NewMethod<Func<long, T>>("FromLong" + typeof(T).FullName);
                b2.Generator.Ldarg(0);
                DefaultTypeConvertorGenerator.Instance.GenerateConversion(typeof(long),
                    typeof(T).GetEnumUnderlyingType())!(b2.Generator);
                b2.Generator.Ret();
                FromLong = b2.Create();
            }
            else
            {
                var b = ILBuilder.Instance.NewMethod<Func<T, ulong>>("ToUlong" + typeof(T).FullName);
                b.Generator
                    .Ldarg(0)
                    .ConvU8()
                    .Ret();
                ToUlong = b.Create();
                var b2 = ILBuilder.Instance.NewMethod<Func<ulong, T>>("FromUlong" + typeof(T).FullName);
                b2.Generator.Ldarg(0);
                DefaultTypeConvertorGenerator.Instance.GenerateConversion(typeof(ulong),
                    typeof(T).GetEnumUnderlyingType())!(b2.Generator);
                b2.Generator.Ret();
                FromUlong = b2.Create();
            }
        }

        public static Constraint<T> Exact(T value) =>
            IsSigned ? new ConstraintSignedEnumExact<T>(value) : new ConstraintUnsignedEnumExact<T>(value);

        public static Constraint<T> Predicate(Predicate<T> predicate) =>
            IsSigned
                ? new ConstraintSignedEnumPredicate<T>(predicate)
                : new ConstraintUnsignedEnumPredicate<T>(predicate);

        public static readonly Constraint<T> Any = Constraint<T>.Any;
    }

    public static partial class String
    {
        public static Constraint<string> StartsWith(string value) =>
            value == "" ? Any : new ConstraintStringStartsWith(value);

        public static Constraint<string> EndsWith(string value) =>
            value == "" ? Any : Predicate((in ReadOnlySpan<char> v) => v.EndsWith(value));

        public static Constraint<string> Contains(string value) =>
            value == "" ? Any : Predicate((in ReadOnlySpan<char> v) => v.Contains(value, StringComparison.Ordinal));

        public static Constraint<string> ContainsCaseInsensitive(string value) =>
            value == ""
                ? Any
                : Predicate((in ReadOnlySpan<char> v) => v.Contains(value, StringComparison.OrdinalIgnoreCase));

        public static Constraint<string> Predicate(PredicateSpanChar predicate) =>
            new ConstraintStringPredicate(predicate);

        public static Constraint<string> PredicateSlow(Predicate<string> predicate) =>
            new ConstraintStringPredicateSlow(predicate);

        public static Constraint<string> Exact(string value) => new ConstraintStringExact(value);

        public static Constraint<string> ExactCaseInsensitive(string value) => Predicate((in ReadOnlySpan<char> v) =>
            v.Equals(value, StringComparison.OrdinalIgnoreCase));

        public static Constraint<string> UpTo(string value, bool including = true) =>
            new ConstraintStringUpTo(value, including);

        public static readonly Constraint<string> Any = Constraint<string>.Any;
    }

    public static partial class ListString
    {
        public static Constraint<List<string>> Contains(string value) => new ConstraintListStringContains(value);
    }

    public static partial class Guid
    {
        public static Constraint<System.Guid> Exact(System.Guid value) => new ConstraintGuidExact(value);
    }
}

public class ConstraintListStringContains : Constraint<List<string>>
{
    readonly string _value;
    int _ofs;
    int _len;

    public ConstraintListStringContains(string value)
    {
        _value = value;
    }

    public override bool IsSimpleExact() => false;

    public override IConstraint.MatchType Prepare(ref MemWriter buffer)
    {
        _ofs = (int)buffer.GetCurrentPosition();
        buffer.WriteString(_value);
        _len = (int)buffer.GetCurrentPosition() - _ofs;
        return IConstraint.MatchType.NoPrefix;
    }

    public override void WritePrefix(ref MemWriter writer, in MemWriter buffer)
    {
    }

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        var res = false;
        var count = reader.ReadVUInt32();
        var val = buffer.AsReadOnlySpan(_ofs, _len);
        for (var i = 0; i < count; i++)
        {
            if (reader.CheckMagic(val))
                res = true;
            else
                reader.SkipString();
        }

        return res ? IConstraint.MatchResult.Yes : IConstraint.MatchResult.No;
    }
}

public class ConstraintDateTimeRange : Constraint<DateTime>
{
    readonly DateTime _from;
    readonly DateTime _to;
    readonly bool _including;

    public ConstraintDateTimeRange(DateTime from, DateTime to, bool including)
    {
        _from = from;
        _to = to;
        _including = including;
    }

    public override bool IsSimpleExact() => false;

    public override IConstraint.MatchType Prepare(ref MemWriter buffer)
    {
        return IConstraint.MatchType.NoPrefix;
    }

    public override void WritePrefix(ref MemWriter writer, in MemWriter buffer)
    {
    }

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        var value = reader.ReadDateTime();
        if (value < _from) return IConstraint.MatchResult.No;
        if (value > _to || !_including && value == _to) return IConstraint.MatchResult.NoAfterLast;
        return IConstraint.MatchResult.Yes;
    }
}

public class NullableExactValue<T> : Constraint<T>
{
    readonly IConstraint _exact;

    public NullableExactValue(IConstraint exact)
    {
        _exact = exact;
    }

    public override bool IsSimpleExact() => _exact.IsSimpleExact();

    public override IConstraint.MatchType Prepare(ref MemWriter buffer)
    {
        _exact.Prepare(ref buffer);
        return IConstraint.MatchType.Exact;
    }

    public override void WritePrefix(ref MemWriter writer, in MemWriter buffer)
    {
        writer.WriteUInt8(1);
        _exact.WritePrefix(ref writer, buffer);
    }

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        if (reader.ReadUInt8() == 1)
        {
            return _exact.Match(ref reader, buffer);
        }

        return IConstraint.MatchResult.No;
    }

    public override IConstraint.MatchResult MatchLast(ref MemReader reader, in MemWriter buffer)
    {
        if (reader.ReadUInt8() == 1)
        {
            return _exact.MatchLast(ref reader, buffer);
        }

        return IConstraint.MatchResult.No;
    }
}

public class NullableExactNull<T> : Constraint<T>
{
    readonly IConstraint _anyT;

    public NullableExactNull(IConstraint anyT)
    {
        _anyT = anyT;
    }

    public override bool IsSimpleExact() => true;

    public override IConstraint.MatchType Prepare(ref MemWriter buffer)
    {
        _anyT.Prepare(ref buffer);
        return IConstraint.MatchType.Exact;
    }

    public override void WritePrefix(ref MemWriter writer, in MemWriter buffer)
    {
        writer.WriteUInt8(0);
    }

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        if (reader.ReadUInt8() == 0)
        {
            return IConstraint.MatchResult.Yes;
        }

        _anyT.Match(ref reader, buffer);
        return IConstraint.MatchResult.NoAfterLast;
    }

    public override IConstraint.MatchResult MatchLast(ref MemReader reader, in MemWriter buffer)
    {
        if (reader.ReadUInt8() == 0)
        {
            return IConstraint.MatchResult.Yes;
        }

        return IConstraint.MatchResult.NoAfterLast;
    }
}

public class FirstConstraint<T> : Constraint<T>
{
    readonly Constraint<T> _of;

    public FirstConstraint(Constraint<T> of)
    {
        _of = of;
    }

    public override bool IsSimpleExact() => false;
    public override bool IsAnyConstraint() => false;

    public override IConstraint.MatchType Prepare(ref MemWriter buffer)
    {
        var res = _of.Prepare(ref buffer);
        if (res == IConstraint.MatchType.Exact) return IConstraint.MatchType.Prefix;
        return res;
    }

    public override void WritePrefix(ref MemWriter writer, in MemWriter buffer)
    {
        _of.WritePrefix(ref writer, buffer);
    }

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        var res = _of.Match(ref reader, buffer);
        if (res == IConstraint.MatchResult.Yes) return IConstraint.MatchResult.YesSkipNext;
        return res;
    }

    public override IConstraint.MatchResult MatchLast(ref MemReader reader, in MemWriter buffer)
    {
        var res = _of.MatchLast(ref reader, buffer);
        if (res == IConstraint.MatchResult.Yes) return IConstraint.MatchResult.YesSkipNext;
        return res;
    }
}

public class ConstraintStringPredicateSlow : ConstraintNoPrefix<string>
{
    readonly Predicate<string> _predicate;

    public ConstraintStringPredicateSlow(Predicate<string> predicate) => _predicate = predicate;

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer) =>
        AsMatchResult(_predicate(reader.ReadStringOrdered()));
}

public class ConstraintStringPredicate : ConstraintNoPrefix<string>
{
    readonly PredicateSpanChar _predicate;

    public ConstraintStringPredicate(PredicateSpanChar predicate) => _predicate = predicate;

    public override bool IsSimpleExact() => false;

    [SkipLocalsInit]
    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        Span<char> bufStr = stackalloc char[512];
        var realStr = reader.ReadStringOrderedAsSpan(ref MemoryMarshal.GetReference(bufStr), bufStr.Length);
        return AsMatchResult(_predicate(realStr));
    }
}

public abstract class ConstraintNoPrefix<T> : Constraint<T>
{
    public override bool IsSimpleExact() => false;

    public override IConstraint.MatchType Prepare(ref MemWriter buffer) => IConstraint.MatchType.NoPrefix;

    public override void WritePrefix(ref MemWriter writer, in MemWriter buffer)
    {
    }
}

public class ConstraintUnsignedPredicate : ConstraintNoPrefix<ulong>
{
    readonly Predicate<ulong> _predicate;

    public ConstraintUnsignedPredicate(Predicate<ulong> predicate) => _predicate = predicate;

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer) =>
        AsMatchResult(_predicate(reader.ReadVUInt64()));
}

public class ConstraintSignedPredicate : ConstraintNoPrefix<long>
{
    readonly Predicate<long> _predicate;

    public ConstraintSignedPredicate(Predicate<long> predicate) => _predicate = predicate;

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer) =>
        AsMatchResult(_predicate(reader.ReadVInt64()));
}

public class ConstraintDateTimePredicate : ConstraintNoPrefix<DateTime>
{
    readonly Predicate<DateTime> _predicate;

    public ConstraintDateTimePredicate(Predicate<DateTime> predicate) => _predicate = predicate;

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer) =>
        AsMatchResult(_predicate(reader.ReadDateTime()));
}

public abstract class ConstraintExact<T> : Constraint<T>
{
    int _ofs;
    int _len;

    public override bool IsSimpleExact() => true;

    public override IConstraint.MatchType Prepare(ref MemWriter buffer)
    {
        _ofs = (int)buffer.GetCurrentPosition();
        WriteExactValue(ref buffer);
        _len = (int)buffer.GetCurrentPosition() - _ofs;
        return IConstraint.MatchType.Exact;
    }

    protected abstract void WriteExactValue(ref MemWriter writer);

    public override void WritePrefix(ref MemWriter writer, in MemWriter buffer)
    {
        writer.WriteBlock(buffer.AsReadOnlySpan(_ofs, _len));
    }

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        if (reader.CheckMagic(buffer.AsReadOnlySpan(_ofs, _len))) return IConstraint.MatchResult.Yes;
        Skip(ref reader);
        return IConstraint.MatchResult.No;
    }

    public override IConstraint.MatchResult MatchLast(ref MemReader reader, in MemWriter buffer)
    {
        if (reader.PeekSpanTillEof().StartsWith(buffer.AsReadOnlySpan(_ofs, _len))) return IConstraint.MatchResult.Yes;
        return IConstraint.MatchResult.No;
    }

    protected abstract void Skip(ref MemReader reader);
}

public abstract class ConstraintUpTo<T> : Constraint<T>
{
    int _ofs;
    int _len;
    readonly bool _including;

    public override bool IsSimpleExact() => false;

    protected ConstraintUpTo(bool including)
    {
        _including = including;
    }

    public override IConstraint.MatchType Prepare(ref MemWriter buffer)
    {
        _ofs = (int)buffer.GetCurrentPosition();
        WriteUpToValue(ref buffer);
        _len = (int)buffer.GetCurrentPosition() - _ofs;
        return IConstraint.MatchType.NoPrefix;
    }

    protected abstract void WriteUpToValue(ref MemWriter writer);

    public override void WritePrefix(ref MemWriter writer, in MemWriter buffer)
    {
    }

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        var readerBuf = reader.PeekSpanTillEof();
        if (readerBuf.Length > _len) readerBuf = readerBuf.Slice(0, _len);
        var comp = readerBuf.SequenceCompareTo(buffer.AsReadOnlySpan(_ofs, _len));
        Skip(ref reader);
        if (comp < 0 || _including && comp == 0) return IConstraint.MatchResult.Yes;
        return IConstraint.MatchResult.NoAfterLast;
    }

    public override IConstraint.MatchResult MatchLast(ref MemReader reader, in MemWriter buffer)
    {
        var readerBuf = reader.PeekSpanTillEof();
        if (readerBuf.Length > _len) readerBuf = readerBuf.Slice(0, _len);
        var comp = readerBuf.SequenceCompareTo(buffer.AsReadOnlySpan(_ofs, _len));
        if (comp < 0 || _including && comp == 0) return IConstraint.MatchResult.Yes;
        return IConstraint.MatchResult.NoAfterLast;
    }

    protected abstract void Skip(ref MemReader reader);
}

public class ConstraintStringExact : ConstraintExact<string>
{
    readonly string _value;

    public ConstraintStringExact(string value)
    {
        _value = value;
    }

    protected override void WriteExactValue(ref MemWriter writer)
    {
        writer.WriteStringOrdered(_value);
    }

    protected override void Skip(ref MemReader reader)
    {
        reader.SkipStringOrdered();
    }
}

public class ConstraintStringUpTo : ConstraintUpTo<string>
{
    readonly string _value;

    public ConstraintStringUpTo(string value, bool including) : base(including) => _value = value;

    protected override void WriteUpToValue(ref MemWriter writer)
    {
        writer.WriteStringOrdered(_value);
    }

    protected override void Skip(ref MemReader reader)
    {
        reader.SkipStringOrdered();
    }
}

public abstract class ConstraintAny<T> : Constraint<T>
{
    public override bool IsAnyConstraint() => true;
    public override bool IsSimpleExact() => false;

    public override IConstraint.MatchType Prepare(ref MemWriter buffer)
    {
        return IConstraint.MatchType.NoPrefix;
    }

    public override IConstraint.MatchResult MatchLast(ref MemReader reader, in MemWriter buffer)
    {
        return IConstraint.MatchResult.Yes;
    }

    public override void WritePrefix(ref MemWriter writer, in MemWriter buffer)
    {
    }
}

public class ConstraintDateTimeAny : ConstraintAny<DateTime>
{
    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        reader.Skip8Bytes();
        return IConstraint.MatchResult.Yes;
    }
}

public class ConstraintGuidAny : ConstraintAny<Guid>
{
    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        reader.SkipGuid();
        return IConstraint.MatchResult.Yes;
    }
}

public class ConstraintGuidExact : ConstraintExact<Guid>
{
    readonly Guid _value;
    public ConstraintGuidExact(Guid value) => _value = value;

    protected override void WriteExactValue(ref MemWriter writer)
    {
        writer.WriteGuid(_value);
    }

    protected override void Skip(ref MemReader reader)
    {
        reader.SkipGuid();
    }
}

public class ConstraintStringAny : ConstraintAny<string>
{
    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        reader.SkipStringOrdered();
        return IConstraint.MatchResult.Yes;
    }
}

public class ConstraintBoolAny : ConstraintAny<bool>
{
    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        reader.Skip1Byte();
        return IConstraint.MatchResult.Yes;
    }
}

public class ConstraintStringStartsWith : Constraint<string>
{
    readonly string _value;
    int _ofs;
    int _len;

    public override bool IsSimpleExact() => false;

    public ConstraintStringStartsWith(string value)
    {
        _value = value;
    }

    public override IConstraint.MatchType Prepare(ref MemWriter buffer)
    {
        _ofs = (int)buffer.GetCurrentPosition();
        buffer.WriteStringOrderedPrefix(_value);
        _len = (int)buffer.GetCurrentPosition() - _ofs;
        return IConstraint.MatchType.Prefix;
    }

    public override void WritePrefix(ref MemWriter writer, in MemWriter buffer)
    {
        writer.WriteBlock(buffer.AsReadOnlySpan(_ofs, _len));
    }

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        if (reader.CheckMagic(buffer.AsReadOnlySpan(_ofs, _len)))
        {
            reader.SkipStringOrdered();
            return IConstraint.MatchResult.Yes;
        }

        reader.SkipStringOrdered();
        return IConstraint.MatchResult.No;
    }

    public override IConstraint.MatchResult MatchLast(ref MemReader reader, in MemWriter buffer)
    {
        if (reader.PeekSpanTillEof().StartsWith(buffer.AsReadOnlySpan(_ofs, _len)))
        {
            return IConstraint.MatchResult.Yes;
        }

        return IConstraint.MatchResult.No;
    }
}

public class ConstraintUnsignedExact : ConstraintExact<ulong>
{
    readonly ulong _value;

    public ConstraintUnsignedExact(ulong value) => _value = value;

    protected override void WriteExactValue(ref MemWriter writer)
    {
        writer.WriteVUInt64(_value);
    }

    protected override void Skip(ref MemReader reader)
    {
        reader.SkipVUInt64();
    }
}

public class ConstraintUnsignedUpTo : ConstraintUpTo<ulong>
{
    readonly ulong _value;

    public ConstraintUnsignedUpTo(ulong value, bool including) : base(including) => _value = value;

    protected override void WriteUpToValue(ref MemWriter writer)
    {
        writer.WriteVUInt64(_value);
    }

    protected override void Skip(ref MemReader reader)
    {
        reader.SkipVUInt64();
    }
}

public class ConstraintSignedExact : ConstraintExact<long>
{
    readonly long _value;

    public ConstraintSignedExact(long value) => _value = value;

    protected override void WriteExactValue(ref MemWriter writer)
    {
        writer.WriteVInt64(_value);
    }

    protected override void Skip(ref MemReader reader)
    {
        reader.SkipVInt64();
    }
}

public class ConstraintSignedUpTo : ConstraintUpTo<long>
{
    readonly long _value;

    public ConstraintSignedUpTo(long value, bool including) : base(including) => _value = value;

    protected override void WriteUpToValue(ref MemWriter writer)
    {
        writer.WriteVInt64(_value);
    }

    protected override void Skip(ref MemReader reader)
    {
        reader.SkipVInt64();
    }
}

public class ConstraintDateTimeExact : ConstraintExact<DateTime>
{
    readonly DateTime _value;

    public ConstraintDateTimeExact(DateTime value) => _value = value;

    protected override void WriteExactValue(ref MemWriter writer)
    {
        writer.WriteDateTimeForbidUnspecifiedKind(_value);
    }

    protected override void Skip(ref MemReader reader)
    {
        reader.Skip8Bytes();
    }
}

public class ConstraintDateTimeUpTo : ConstraintUpTo<DateTime>
{
    readonly DateTime _value;

    public ConstraintDateTimeUpTo(DateTime value, bool including) : base(including) => _value = value;

    protected override void WriteUpToValue(ref MemWriter writer)
    {
        writer.WriteDateTimeForbidUnspecifiedKind(_value);
    }

    protected override void Skip(ref MemReader reader)
    {
        reader.Skip8Bytes();
    }
}

public class ConstraintBoolExact : ConstraintExact<bool>
{
    readonly bool _value;

    public ConstraintBoolExact(bool value) => _value = value;

    protected override void WriteExactValue(ref MemWriter writer)
    {
        writer.WriteBool(_value);
    }

    protected override void Skip(ref MemReader reader)
    {
        reader.Skip1Byte();
    }
}

public class ConstraintSignedEnumExact<T> : ConstraintExact<T> where T : Enum
{
    readonly long _value;

    public ConstraintSignedEnumExact(T value) => _value = Constraint.Enum<T>.ToLong!(value);

    protected override void WriteExactValue(ref MemWriter writer)
    {
        writer.WriteVInt64(_value);
    }

    protected override void Skip(ref MemReader reader)
    {
        reader.SkipVInt64();
    }
}

public class ConstraintUnsignedEnumExact<T> : ConstraintExact<T> where T : Enum
{
    readonly ulong _value;

    public ConstraintUnsignedEnumExact(T value) => _value = Constraint.Enum<T>.ToUlong!(value);

    protected override void WriteExactValue(ref MemWriter writer)
    {
        writer.WriteVUInt64(_value);
    }

    protected override void Skip(ref MemReader reader)
    {
        reader.SkipVUInt64();
    }
}

public class ConstraintSignedAny<T> : ConstraintAny<T>
{
    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        reader.SkipVInt64();
        return IConstraint.MatchResult.Yes;
    }
}

public class ConstraintUnsignedAny<T> : ConstraintAny<T>
{
    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer)
    {
        reader.SkipVUInt64();
        return IConstraint.MatchResult.Yes;
    }
}

public class ConstraintSignedEnumPredicate<T> : ConstraintNoPrefix<T> where T : Enum
{
    readonly Predicate<T> _predicate;

    public ConstraintSignedEnumPredicate(Predicate<T> predicate) => _predicate = predicate;

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer) =>
        AsMatchResult(_predicate(Constraint.Enum<T>.FromLong!(reader.ReadVInt64())));
}

public class ConstraintUnsignedEnumPredicate<T> : ConstraintNoPrefix<T> where T : Enum
{
    readonly Predicate<T> _predicate;

    public ConstraintUnsignedEnumPredicate(Predicate<T> predicate) => _predicate = predicate;

    public override IConstraint.MatchResult Match(ref MemReader reader, in MemWriter buffer) =>
        AsMatchResult(_predicate(Constraint.Enum<T>.FromUlong!(reader.ReadVUInt64())));
}
