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

        // return true if match was successful and also skip reader after this field
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
            public static Constraint<ulong> Exact(ulong value)
            {
                return new ConstraintUnsignedExact(value);
            }
        }
        public static partial class String
        {
            public static Constraint<string> StartsWith(string value)
            {
                return new ConstraintStringStartsWith(value);
            }
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

            return false;
        }
    }

    public class ConstraintUnsignedExact : Constraint<ulong>
    {
        readonly ulong _value;
        int _ofs;
        int _len;

        public ConstraintUnsignedExact(ulong value)
        {
            _value = value;
        }

        public override IConstraint.MatchType Prepare(ref StructList<byte> buffer)
        {
            var structListWriter = new ContinuousMemoryBlockWriter(buffer);
            _ofs = (int)buffer.Count;
            var writer = new SpanWriter(structListWriter);
            writer.WriteVUInt64(_value);
            writer.Sync();
            buffer = structListWriter.GetStructList();
            _len = (int)buffer.Count - _ofs;
            return IConstraint.MatchType.Exact;
        }

        public override void WritePrefix(ref SpanWriter writer, in StructList<byte> buffer)
        {
            writer.WriteBlock(buffer.AsReadOnlySpan(_ofs, _len));
        }

        public override bool Match(ref SpanReader reader, in StructList<byte> buffer)
        {
            return reader.CheckMagic(buffer.AsReadOnlySpan(_ofs, _len));
        }
    }
}
