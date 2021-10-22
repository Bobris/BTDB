using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Collections;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public interface IConstraint
    {
        public enum MatchType
        {
            NoPrefix,
            Prefix,
            Exact
        }

        public MatchType Prepare(ref StructList<byte> buffer);

        // Will be called only for Prefix and Exact MatchTypes, for Exact it have to write full part of key
        public void WritePrefix(ref SpanWriter writer, in StructList<byte> buffer);

        // return true if match was successful and in ALL CASES SKIP reader after this field
        // when MatchType is Exact this method does not need to be called at all if part of key matches written prefix
        public bool Match(ref SpanReader reader, in StructList<byte> buffer);
    }

    public abstract class Constraint<T> : IConstraint
    {
        public abstract IConstraint.MatchType Prepare(ref StructList<byte> buffer);
        public abstract void WritePrefix(ref SpanWriter writer, in StructList<byte> buffer);
        public abstract bool Match(ref SpanReader reader, in StructList<byte> buffer);
    }

    public static partial class Constraint
    {
        public static partial class Unsigned
        {
            public static Constraint<ulong> Exact(ulong value) => new ConstraintUnsignedExact(value);
            public static Constraint<ulong> Predicate(Predicate<ulong> predicate) =>
                new ConstraintUnsignedPredicate(predicate);
            public static Constraint<ulong> Any = new ConstraintUnsignedAny();
        }

        public static partial class String
        {
            public static Constraint<string> StartsWith(string value) =>
                value == "" ? Any : new ConstraintStringStartsWith(value);

            public static Constraint<string> Contains(string value) =>
                value == "" ? Any : new ConstraintStringContains(value);

            public static Constraint<string> Exact(string value) => new ConstraintStringExact(value);
            public static Constraint<string> Any = new ConstraintStringAny();
        }
    }

    public abstract class ConstraintNoPrefix<T> : Constraint<T>
    {
        public override IConstraint.MatchType Prepare(ref StructList<byte> buffer)
        {
            return IConstraint.MatchType.NoPrefix;
        }

        public override void WritePrefix(ref SpanWriter writer, in StructList<byte> buffer)
        {
        }
    }

    public class ConstraintUnsignedPredicate : ConstraintNoPrefix<ulong>
    {
        readonly Predicate<ulong> _predicate;

        public ConstraintUnsignedPredicate(Predicate<ulong> predicate)
        {
            _predicate = predicate;
        }

        public override bool Match(ref SpanReader reader, in StructList<byte> buffer)
        {
            return _predicate(reader.ReadVUInt64());
        }
    }

    public abstract class ConstraintExact<T> : Constraint<T>
    {
        protected int Ofs;
        protected int Len;

        public override void WritePrefix(ref SpanWriter writer, in StructList<byte> buffer)
        {
            writer.WriteBlock(buffer.AsReadOnlySpan(Ofs, Len));
        }

        public override bool Match(ref SpanReader reader, in StructList<byte> buffer)
        {
            if (reader.CheckMagic(buffer.AsReadOnlySpan(Ofs, Len))) return true;
            Skip(ref reader);
            return false;
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

        public override IConstraint.MatchType Prepare(ref StructList<byte> buffer)
        {
            var structListWriter = new ContinuousMemoryBlockWriter(buffer);
            Ofs = (int)buffer.Count;
            var writer = new SpanWriter(structListWriter);
            writer.WriteStringOrdered(_value);
            writer.Sync();
            buffer = structListWriter.GetStructList();
            Len = (int)buffer.Count - Ofs;
            return IConstraint.MatchType.Exact;
        }

        protected override void Skip(ref SpanReader reader)
        {
            reader.SkipStringOrdered();
        }
    }

    public class ConstraintStringContains : Constraint<string>
    {
        readonly string _value;
        int _ofs;
        int _len;

        public ConstraintStringContains(string value)
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
            return IConstraint.MatchType.NoPrefix;
        }

        public override void WritePrefix(ref SpanWriter writer, in StructList<byte> buffer)
        {
        }

        public override bool Match(ref SpanReader reader, in StructList<byte> buffer)
        {
            var start = reader.Buf;
            reader.SkipStringOrdered();
            start = start.Slice(0,
                Unsafe.ByteOffset(ref MemoryMarshal.GetReference(start), ref MemoryMarshal.GetReference(reader.Buf))
                    .ToInt32() - 1);
            return start.IndexOf(buffer.AsReadOnlySpan(_ofs, _len)) >= 0;
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
        public override bool Match(ref SpanReader reader, in StructList<byte> buffer)
        {
            reader.SkipVUInt64();
            return true;
        }
    }

    public class ConstraintStringAny : ConstraintAny<string>
    {
        public override bool Match(ref SpanReader reader, in StructList<byte> buffer)
        {
            reader.SkipStringOrdered();
            return true;
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

        public override bool Match(ref SpanReader reader, in StructList<byte> buffer)
        {
            if (reader.CheckMagic(buffer.AsReadOnlySpan(_ofs, _len)))
            {
                reader.SkipStringOrdered();
                return true;
            }

            reader.SkipStringOrdered();
            return false;
        }
    }

    public class ConstraintUnsignedExact : ConstraintExact<ulong>
    {
        readonly ulong _value;

        public ConstraintUnsignedExact(ulong value)
        {
            _value = value;
        }

        public override IConstraint.MatchType Prepare(ref StructList<byte> buffer)
        {
            var structListWriter = new ContinuousMemoryBlockWriter(buffer);
            Ofs = (int)buffer.Count;
            var writer = new SpanWriter(structListWriter);
            writer.WriteVUInt64(_value);
            writer.Sync();
            buffer = structListWriter.GetStructList();
            Len = (int)buffer.Count - Ofs;
            return IConstraint.MatchType.Exact;
        }

        protected override void Skip(ref SpanReader reader)
        {
            reader.SkipVUInt64();
        }
    }
}
