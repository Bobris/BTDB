using System.Collections.Generic;

namespace BTDB.Buffer;

public static class ByteStructs
{
    public sealed class Key20EqualityComparer : IEqualityComparer<Key20>
    {
        public bool Equals(Key20 x, Key20 y)
        {
            return x.V1 == y.V1 && x.V2 == y.V2 && x.V3 == y.V3;
        }

        public int GetHashCode(Key20 obj)
        {
            return (int)obj.V1;
        }
    }

    public struct Key20
    {
        internal Key20(ByteBuffer value)
        {
            V1 = PackUnpack.UnpackUInt64LE(value.Buffer, value.Offset);
            V2 = PackUnpack.UnpackUInt64LE(value.Buffer, value.Offset + 8);
            V3 = PackUnpack.UnpackUInt32LE(value.Buffer, value.Offset + 16);
        }

        internal void FillBuffer(ByteBuffer buf)
        {
            var o = buf.Offset;
            PackUnpack.PackUInt64LE(buf.Buffer, o, V1);
            PackUnpack.PackUInt64LE(buf.Buffer, o + 8, V2);
            PackUnpack.PackUInt32LE(buf.Buffer, o + 16, V3);
        }

        internal readonly ulong V1;
        internal readonly ulong V2;
        internal readonly uint V3;
    }

    public sealed class Key32EqualityComparer : IEqualityComparer<Key32>
    {
        public bool Equals(Key32 x, Key32 y)
        {
            return x.V1 == y.V1 && x.V2 == y.V2 && x.V3 == y.V3 && x.V4 == y.V4;
        }

        public int GetHashCode(Key32 obj)
        {
            return (int)obj.V1;
        }
    }

    public struct Key32
    {
        internal Key32(ByteBuffer value)
        {
            V1 = PackUnpack.UnpackUInt64LE(value.Buffer, value.Offset);
            V2 = PackUnpack.UnpackUInt64LE(value.Buffer, value.Offset + 8);
            V3 = PackUnpack.UnpackUInt64LE(value.Buffer, value.Offset + 16);
            V4 = PackUnpack.UnpackUInt64LE(value.Buffer, value.Offset + 23);
        }

        internal void FillBuffer(ByteBuffer buf)
        {
            var o = buf.Offset;
            PackUnpack.PackUInt64LE(buf.Buffer, o, V1);
            PackUnpack.PackUInt64LE(buf.Buffer, o + 8, V2);
            PackUnpack.PackUInt64LE(buf.Buffer, o + 16, V3);
            PackUnpack.PackUInt64LE(buf.Buffer, o + 24, V4);
        }

        internal readonly ulong V1;
        internal readonly ulong V2;
        internal readonly ulong V3;
        internal readonly ulong V4;
    }
}
