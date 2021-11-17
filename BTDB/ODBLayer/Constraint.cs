using System;
using System.Runtime.InteropServices;
using BTDB.Collections;
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

    public enum MatchResult
    {
        No = 0,
        Yes = 1,
        NoAfterLast = 2
    }

    public MatchType Prepare(ref StructList<byte> buffer);

    // Will be called only for Prefix and Exact MatchTypes, for Exact it have to write full part of key
    public void WritePrefix(ref SpanWriter writer, in StructList<byte> buffer);

    // return true if match was successful and in ALL CASES SKIP reader after this field
    // when MatchType is Exact this method does not need to be called at all if part of key matches written prefix
    public MatchResult Match(ref SpanReader reader, in StructList<byte> buffer);
}

public abstract class Constraint<T> : IConstraint
{
    public abstract IConstraint.MatchType Prepare(ref StructList<byte> buffer);
    public abstract void WritePrefix(ref SpanWriter writer, in StructList<byte> buffer);
    public abstract IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer);

    public static IConstraint.MatchResult AsMatchResult(bool value)
    {
        return value ? IConstraint.MatchResult.Yes : IConstraint.MatchResult.No;
    }
}

public static partial class Constraint
{
    public static partial class Bool
    {
        public static Constraint<bool> Exact(bool value) => new ConstraintBoolExact(value);
        public static readonly Constraint<bool> Any = new ConstraintBoolAny();
    }

    public static partial class DateTime
    {
        public static Constraint<System.DateTime> Exact(System.DateTime value) => new ConstraintDateTimeExact(value);
        public static Constraint<System.DateTime> Predicate(Predicate<System.DateTime> predicate) =>
            new ConstraintDateTimePredicate(predicate);
        public static Constraint<System.DateTime> UpTo(System.DateTime value, bool including = true) =>
            new ConstraintDateTimeUpTo(value, including);
        public static readonly Constraint<System.DateTime> Any = new ConstraintDateTimeAny();
    }

    public static partial class Unsigned
    {
        public static Constraint<ulong> Exact(ulong value) => new ConstraintUnsignedExact(value);
        public static Constraint<ulong> Predicate(Predicate<ulong> predicate) =>
            new ConstraintUnsignedPredicate(predicate);
        public static Constraint<ulong> UpTo(ulong value, bool including = true) =>
            new ConstraintUnsignedUpTo(value, including);
        public static readonly Constraint<ulong> Any = new ConstraintUnsignedAny();
    }

    public static partial class Signed
    {
        public static Constraint<long> Exact(long value) => new ConstraintSignedExact(value);
        public static Constraint<long> Predicate(Predicate<long> predicate) =>
            new ConstraintSignedPredicate(predicate);
        public static Constraint<long> UpTo(long value, bool including = true) =>
            new ConstraintSignedUpTo(value, including);
        public static readonly Constraint<long> Any = new ConstraintSignedAny();
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
            value == "" ? Any : Predicate((in ReadOnlySpan<char> v) => v.Contains(value, StringComparison.OrdinalIgnoreCase));

        public static Constraint<string> Predicate(PredicateSpanChar predicate) =>
            new ConstraintStringPredicate(predicate);

        public static Constraint<string> PredicateSlow(Predicate<string> predicate) =>
            new ConstraintStringPredicateSlow(predicate);

        public static Constraint<string> Exact(string value) => new ConstraintStringExact(value);
        public static Constraint<string> UpTo(string value, bool including = true) =>
            new ConstraintStringUpTo(value, including);
        public static readonly Constraint<string> Any = new ConstraintStringAny();
    }
}

public class ConstraintStringPredicateSlow : ConstraintNoPrefix<string>
{
    readonly Predicate<string> _predicate;

    public ConstraintStringPredicateSlow(Predicate<string> predicate) => _predicate = predicate;

    public override IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer) => AsMatchResult(_predicate(reader.ReadStringOrdered()));
}

public class ConstraintStringPredicate : ConstraintNoPrefix<string>
{
    readonly PredicateSpanChar _predicate;

    public ConstraintStringPredicate(PredicateSpanChar predicate) => _predicate = predicate;

    public override IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer)
    {
        Span<char> bufStr = stackalloc char[512];
        var realStr = reader.ReadStringOrderedAsSpan(ref MemoryMarshal.GetReference(bufStr), bufStr.Length);
        return AsMatchResult(_predicate(realStr));
    }
}

public abstract class ConstraintNoPrefix<T> : Constraint<T>
{
    public override IConstraint.MatchType Prepare(ref StructList<byte> buffer) => IConstraint.MatchType.NoPrefix;

    public override void WritePrefix(ref SpanWriter writer, in StructList<byte> buffer)
    {
    }
}

public class ConstraintUnsignedPredicate : ConstraintNoPrefix<ulong>
{
    readonly Predicate<ulong> _predicate;

    public ConstraintUnsignedPredicate(Predicate<ulong> predicate) => _predicate = predicate;

    public override IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer) => AsMatchResult(_predicate(reader.ReadVUInt64()));
}

public class ConstraintSignedPredicate : ConstraintNoPrefix<long>
{
    readonly Predicate<long> _predicate;

    public ConstraintSignedPredicate(Predicate<long> predicate) => _predicate = predicate;

    public override IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer) => AsMatchResult(_predicate(reader.ReadVInt64()));
}

public class ConstraintDateTimePredicate : ConstraintNoPrefix<DateTime>
{
    readonly Predicate<DateTime> _predicate;

    public ConstraintDateTimePredicate(Predicate<DateTime> predicate) => _predicate = predicate;

    public override IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer) => AsMatchResult(_predicate(reader.ReadDateTime()));
}

public abstract class ConstraintExact<T> : Constraint<T>
{
    int _ofs;
    int _len;

    public override IConstraint.MatchType Prepare(ref StructList<byte> buffer)
    {
        var structListWriter = new ContinuousMemoryBlockWriter(buffer);
        _ofs = (int)buffer.Count;
        var writer = new SpanWriter(structListWriter);
        WriteExactValue(ref writer);
        writer.Sync();
        buffer = structListWriter.GetStructList();
        _len = (int)buffer.Count - _ofs;
        return IConstraint.MatchType.Exact;
    }

    protected abstract void WriteExactValue(ref SpanWriter writer);

    public override void WritePrefix(ref SpanWriter writer, in StructList<byte> buffer)
    {
        writer.WriteBlock(buffer.AsReadOnlySpan(_ofs, _len));
    }

    public override IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer)
    {
        if (reader.CheckMagic(buffer.AsReadOnlySpan(_ofs, _len))) return IConstraint.MatchResult.Yes;
        Skip(ref reader);
        return IConstraint.MatchResult.No;
    }

    protected abstract void Skip(ref SpanReader reader);
}

public abstract class ConstraintUpTo<T> : Constraint<T>
{
    int _ofs;
    int _len;
    readonly bool _including;

    protected ConstraintUpTo(bool including)
    {
        _including = including;
    }

    public override IConstraint.MatchType Prepare(ref StructList<byte> buffer)
    {
        var structListWriter = new ContinuousMemoryBlockWriter(buffer);
        _ofs = (int)buffer.Count;
        var writer = new SpanWriter(structListWriter);
        WriteUpToValue(ref writer);
        writer.Sync();
        buffer = structListWriter.GetStructList();
        _len = (int)buffer.Count - _ofs;
        return IConstraint.MatchType.NoPrefix;
    }

    protected abstract void WriteUpToValue(ref SpanWriter writer);

    public override void WritePrefix(ref SpanWriter writer, in StructList<byte> buffer)
    {
    }

    public override IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer)
    {
        var readerBuf = reader.Buf;
        if (readerBuf.Length > _len) readerBuf = readerBuf.Slice(0, _len);
        var comp = readerBuf.SequenceCompareTo(buffer.AsReadOnlySpan(_ofs, _len));
        Skip(ref reader);
        if (comp < 0 || _including && comp == 0) return IConstraint.MatchResult.Yes;
        return IConstraint.MatchResult.NoAfterLast;
    }

    protected abstract void Skip(ref SpanReader reader);
}

public class ConstraintStringExact : ConstraintExact<string>
{
    readonly string _value;

    public ConstraintStringExact(string value)
    {
        _value = value;
    }

    protected override void WriteExactValue(ref SpanWriter writer)
    {
        writer.WriteStringOrdered(_value);
    }

    protected override void Skip(ref SpanReader reader)
    {
        reader.SkipStringOrdered();
    }
}

public class ConstraintStringUpTo : ConstraintUpTo<string>
{
    readonly string _value;

    public ConstraintStringUpTo(string value, bool including) : base(including) => _value = value;

    protected override void WriteUpToValue(ref SpanWriter writer)
    {
        writer.WriteStringOrdered(_value);
    }

    protected override void Skip(ref SpanReader reader)
    {
        reader.SkipStringOrdered();
    }
}

public abstract class ConstraintAny<T> : Constraint<T>
{
    public override IConstraint.MatchType Prepare(ref StructList<byte> buffer)
    {
        return IConstraint.MatchType.NoPrefix;
    }

    public override void WritePrefix(ref SpanWriter writer, in StructList<byte> buffer)
    {
    }
}

public class ConstraintUnsignedAny : ConstraintAny<ulong>
{
    public override IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer)
    {
        reader.SkipVUInt64();
        return IConstraint.MatchResult.Yes;
    }
}

public class ConstraintSignedAny : ConstraintAny<long>
{
    public override IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer)
    {
        reader.SkipVInt64();
        return IConstraint.MatchResult.Yes;
    }
}

public class ConstraintDateTimeAny : ConstraintAny<DateTime>
{
    public override IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer)
    {
        reader.SkipDateTime();
        return IConstraint.MatchResult.Yes;
    }
}

public class ConstraintStringAny : ConstraintAny<string>
{
    public override IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer)
    {
        reader.SkipStringOrdered();
        return IConstraint.MatchResult.Yes;
    }
}

public class ConstraintBoolAny : ConstraintAny<bool>
{
    public override IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer)
    {
        reader.SkipBool();
        return IConstraint.MatchResult.Yes;
    }
}

public class ConstraintStringStartsWith : Constraint<string>
{
    readonly string _value;
    int _ofs;
    int _len;

    public ConstraintStringStartsWith(string value)
    {
        _value = value;
    }

    public override IConstraint.MatchType Prepare(ref StructList<byte> buffer)
    {
        var structListWriter = new ContinuousMemoryBlockWriter(buffer);
        _ofs = (int)buffer.Count;
        var writer = new SpanWriter(structListWriter);
        writer.WriteStringOrderedPrefix(_value);
        writer.Sync();
        buffer = structListWriter.GetStructList();
        _len = (int)buffer.Count - _ofs;
        return IConstraint.MatchType.Prefix;
    }

    public override void WritePrefix(ref SpanWriter writer, in StructList<byte> buffer)
    {
        writer.WriteBlock(buffer.AsReadOnlySpan(_ofs, _len));
    }

    public override IConstraint.MatchResult Match(ref SpanReader reader, in StructList<byte> buffer)
    {
        if (reader.CheckMagic(buffer.AsReadOnlySpan(_ofs, _len)))
        {
            reader.SkipStringOrdered();
            return IConstraint.MatchResult.Yes;
        }

        reader.SkipStringOrdered();
        return IConstraint.MatchResult.No;
    }
}

public class ConstraintUnsignedExact : ConstraintExact<ulong>
{
    readonly ulong _value;

    public ConstraintUnsignedExact(ulong value) => _value = value;

    protected override void WriteExactValue(ref SpanWriter writer)
    {
        writer.WriteVUInt64(_value);
    }

    protected override void Skip(ref SpanReader reader)
    {
        reader.SkipVUInt64();
    }
}

public class ConstraintUnsignedUpTo : ConstraintUpTo<ulong>
{
    readonly ulong _value;

    public ConstraintUnsignedUpTo(ulong value, bool including) : base(including) => _value = value;

    protected override void WriteUpToValue(ref SpanWriter writer)
    {
        writer.WriteVUInt64(_value);
    }

    protected override void Skip(ref SpanReader reader)
    {
        reader.SkipVUInt64();
    }
}

public class ConstraintSignedExact : ConstraintExact<long>
{
    readonly long _value;

    public ConstraintSignedExact(long value) => _value = value;

    protected override void WriteExactValue(ref SpanWriter writer)
    {
        writer.WriteVInt64(_value);
    }

    protected override void Skip(ref SpanReader reader)
    {
        reader.SkipVInt64();
    }
}

public class ConstraintSignedUpTo : ConstraintUpTo<long>
{
    readonly long _value;

    public ConstraintSignedUpTo(long value, bool including) : base(including) => _value = value;

    protected override void WriteUpToValue(ref SpanWriter writer)
    {
        writer.WriteVInt64(_value);
    }

    protected override void Skip(ref SpanReader reader)
    {
        reader.SkipVInt64();
    }
}

public class ConstraintDateTimeExact : ConstraintExact<DateTime>
{
    readonly DateTime _value;

    public ConstraintDateTimeExact(DateTime value) => _value = value;

    protected override void WriteExactValue(ref SpanWriter writer)
    {
        writer.WriteDateTimeForbidUnspecifiedKind(_value);
    }

    protected override void Skip(ref SpanReader reader)
    {
        reader.SkipDateTime();
    }
}

public class ConstraintDateTimeUpTo : ConstraintUpTo<DateTime>
{
    readonly DateTime _value;

    public ConstraintDateTimeUpTo(DateTime value, bool including): base(including) => _value = value;

    protected override void WriteUpToValue(ref SpanWriter writer)
    {
        writer.WriteDateTimeForbidUnspecifiedKind(_value);
    }

    protected override void Skip(ref SpanReader reader)
    {
        reader.SkipDateTime();
    }
}

public class ConstraintBoolExact : ConstraintExact<bool>
{
    readonly bool _value;

    public ConstraintBoolExact(bool value) => _value = value;

    protected override void WriteExactValue(ref SpanWriter writer)
    {
        writer.WriteBool(_value);
    }

    protected override void Skip(ref SpanReader reader)
    {
        reader.SkipBool();
    }
}
