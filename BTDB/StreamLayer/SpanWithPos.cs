using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;

namespace BTDB.StreamLayer;

public ref struct SpanWithPos
{
    public ref byte Buf;
    public uint Length;
    public uint Pos;

    public SpanWithPos(Span<byte> buf)
    {
        Buf = ref MemoryMarshal.GetReference(buf);
        Length = (uint)buf.Length;
        Pos = 0;
    }

    public SpanWithPos(ref byte buf, uint length)
    {
        Buf = ref buf;
        Length = length;
        Pos = 0;
    }

    public SpanWithPos()
    {
        Buf = ref Unsafe.NullRef<byte>();
        Length = 0;
        Pos = 0;
    }

    // Read and skip
    public byte ReadUInt8()
    {
        if (Pos >= Length) PackUnpack.ThrowEndOfStreamException();
        return Unsafe.AddByteOffset(ref Buf, Pos++);
    }

    public void SkipUInt8()
    {
        if (Pos >= Length) PackUnpack.ThrowEndOfStreamException();
        Pos++;
    }

    public bool ReadBool()
    {
        return ReadUInt8() != 0;
    }

    public void SkipBool()
    {
        SkipUInt8();
    }

    public sbyte ReadInt8()
    {
        return (sbyte)ReadUInt8();
    }

    public void SkipInt8()
    {
        SkipUInt8();
    }

    public ushort ReadUInt16LE()
    {
        if (Pos + 2 > Length) PackUnpack.ThrowEndOfStreamException();
        var res = PackUnpack.AsLittleEndian(
            Unsafe.As<byte, ushort>(ref Unsafe.AddByteOffset(ref Buf, Pos)));
        Pos += 2;
        return res;
    }

    public ushort ReadUInt16BE()
    {
        if (Pos + 2 > Length) PackUnpack.ThrowEndOfStreamException();
        var res = PackUnpack.AsBigEndian(
            Unsafe.As<byte, ushort>(ref Unsafe.AddByteOffset(ref Buf, Pos)));
        Pos += 2;
        return res;
    }


    public void SkipInt16()
    {
        if (Pos + 2 > Length) PackUnpack.ThrowEndOfStreamException();
        Pos += 2;
    }

    public short ReadInt16LE()
    {
        return (short)ReadUInt16LE();
    }

    public short ReadInt16BE()
    {
        return (short)ReadUInt16BE();
    }

    // Write

    public override string ToString()
    {
        if (Length < 100)
        {
            return Convert.ToHexString(MemoryMarshal.CreateSpan(ref Buf, (int)Pos)) + "|" +
                   Convert.ToHexString(MemoryMarshal.CreateSpan(ref Buf, (int)Length)[(int)Pos..]);
        }

        return (Pos < 50
                   ? Convert.ToHexString(MemoryMarshal.CreateSpan(ref Buf, (int)Pos))
                   : Convert.ToHexString(MemoryMarshal.CreateSpan(ref Buf, 20)) + "..." +
                     Convert.ToHexString(MemoryMarshal.CreateSpan(ref Buf, (int)Length).Slice((int)Pos - 20, 20))) +
               "|" +
               Convert.ToHexString(MemoryMarshal.CreateSpan(ref Buf, (int)Length)
                   .Slice((int)Pos, int.Min((int)(Length - Pos), 50)));
    }
}
