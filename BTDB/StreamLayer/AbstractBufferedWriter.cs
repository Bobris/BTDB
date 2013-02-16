using System;
using System.Net;
using System.Net.Sockets;
using BTDB.Buffer;

namespace BTDB.StreamLayer
{
    public abstract class AbstractBufferedWriter
    {
        protected AbstractBufferedWriter()
        {
            Buf = null;
            Pos = 0;
            End = 0;
        }
        protected byte[] Buf;
        protected int Pos;
        protected int End;

        public abstract void FlushBuffer();
        public abstract long GetCurrentPosition();

        public void WriteByteZero()
        {
            if (Pos >= End)
            {
                FlushBuffer();
            }
            Buf[Pos++] = 0;
        }

        public void WriteBool(bool value)
        {
            if (Pos >= End)
            {
                FlushBuffer();
            }
            Buf[Pos++] = (byte)(value ? 1 : 0);
        }

        public void WriteUInt8(byte value)
        {
            if (Pos >= End)
            {
                FlushBuffer();
            }
            Buf[Pos++] = value;
        }

        public void WriteInt8(sbyte value)
        {
            if (Pos >= End)
            {
                FlushBuffer();
            }
            Buf[Pos++] = (byte)value;
        }

        public void WriteInt8Ordered(sbyte value)
        {
            if (Pos >= End)
            {
                FlushBuffer();
            }
            Buf[Pos++] = (byte)(value + 128);
        }

        public void WriteVInt16(short value)
        {
            WriteVInt64(value);
        }

        public void WriteVUInt16(ushort value)
        {
            WriteVUInt64(value);
        }

        public void WriteVInt32(int value)
        {
            WriteVInt64(value);
        }

        public void WriteVUInt32(uint value)
        {
            WriteVUInt64(value);
        }

        public void WriteVInt64(long value)
        {
            var l = PackUnpack.LengthVInt(value);
            if (Pos + l > End)
            {
                FlushBuffer();
                if (Pos + l > End)
                {
                    var b = new byte[l];
                    int i = 0;
                    PackUnpack.PackVInt(b, ref i, value);
                    WriteBlock(b);
                    return;
                }
            }
            PackUnpack.PackVInt(Buf, ref Pos, value);
        }

        public void WriteVUInt64(ulong value)
        {
            var l = PackUnpack.LengthVUInt(value);
            if (Pos + l > End)
            {
                FlushBuffer();
                if (Pos + l > End)
                {
                    var b = new byte[l];
                    int i = 0;
                    PackUnpack.PackVUInt(b, ref i, value);
                    WriteBlock(b);
                    return;
                }
            }
            PackUnpack.PackVUInt(Buf, ref Pos, value);
        }

        public void WriteInt64(long value)
        {
            if (Pos + 8 > End)
            {
                FlushBuffer();
                if (Pos + 8 > End)
                {
                    var b = new byte[8];
                    PackUnpack.PackInt64BE(b, 0, value);
                    WriteBlock(b);
                    return;
                }
            }
            PackUnpack.PackInt64BE(Buf, Pos, value);
            Pos += 8;
        }

        public void WriteInt32(int value)
        {
            if (Pos + 4 > End)
            {
                FlushBuffer();
                if (Pos + 4 > End)
                {
                    var b = new byte[4];
                    PackUnpack.PackInt32BE(b, 0, value);
                    WriteBlock(b);
                    return;
                }
            }
            PackUnpack.PackInt32BE(Buf, Pos, value);
            Pos += 4;
        }

        public void WriteInt32LE(int value)
        {
            if (Pos + 4 > End)
            {
                FlushBuffer();
                if (Pos + 4 > End)
                {
                    var b = new byte[4];
                    PackUnpack.PackInt32LE(b, 0, value);
                    WriteBlock(b);
                    return;
                }
            }
            PackUnpack.PackInt32LE(Buf, Pos, value);
            Pos += 4;
        }

        public void WriteDateTime(DateTime value)
        {
            WriteInt64(value.ToBinary());
        }

        public void WriteDateTimeForbidUnspecifiedKind(DateTime value)
        {
            if (value.Kind == DateTimeKind.Unspecified)
                throw new ArgumentOutOfRangeException("value", "DateTime.Kind cannot be stored as Unspecified");
            WriteDateTime(value);
        }

        public void WriteTimeSpan(TimeSpan value)
        {
            WriteVInt64(value.Ticks);
        }

        public void WriteString(string value)
        {
            if (value == null)
            {
                WriteByteZero();
                return;
            }
            var l = value.Length;
            WriteVUInt64((ulong)l + 1);
            var i = 0;
            while (i < l)
            {
                var c = value[i];
                if (c < 0x80)
                {
                    if (Pos >= End)
                    {
                        FlushBuffer();
                    }
                    Buf[Pos++] = (byte)c;
                }
                else
                {
                    if (char.IsHighSurrogate(c) && i + 1 < l)
                    {
                        var c2 = value[i + 1];
                        if (char.IsLowSurrogate(c2))
                        {
                            WriteVUInt32((uint)((((c - 0xD800) * 0x400) + (c2 - 0xDC00)) + 0x10000));
                            i += 2;
                            continue;
                        }
                    }
                    WriteVUInt32(c);
                }
                i++;
            }
        }

        public void WriteStringOrdered(string value)
        {
            if (value == null)
            {
                WriteVUInt32(0x110001);
                return;
            }
            var l = value.Length;
            int i = 0;
            while (i < l)
            {
                var c = value[i];
                if (char.IsHighSurrogate(c) && i + 1 < l)
                {
                    var c2 = value[i + 1];
                    if (char.IsLowSurrogate(c2))
                    {
                        WriteVUInt32((uint)((((c - 0xD800) * 0x400) + (c2 - 0xDC00)) + 0x10000) + 1);
                        i += 2;
                        continue;
                    }
                }
                WriteVUInt32((uint)c + 1);
                i++;
            }
            WriteByteZero();
        }

        public virtual void WriteBlock(byte[] data, int offset, int length)
        {
            while (length > 0)
            {
                if (Pos >= End) FlushBuffer();
                var l = End - Pos;
                if (length < l) l = length;
                Array.Copy(data, offset, Buf, Pos, l);
                offset += l;
                length -= l;
                Pos += l;
            }
        }

        public void WriteBlock(byte[] data)
        {
            WriteBlock(data, 0, data.Length);
        }

        public void WriteGuid(Guid value)
        {
            WriteBlock(value.ToByteArray());
        }

        public void WriteSingle(float value)
        {
            WriteInt32(new Int32SingleUnion(value).AsInt32);
        }

        public void WriteDouble(double value)
        {
            WriteInt64(BitConverter.DoubleToInt64Bits(value));
        }

        public void WriteDecimal(decimal value)
        {
            var ints = decimal.GetBits(value);
            var header = (byte)((ints[3] >> 16) & 31);
            if (ints[3] < 0) header |= 128;
            var first = (uint)ints[0] + ((ulong)ints[1] << 32);
            if (ints[2] == 0)
            {
                if (first == 0)
                {
                    WriteUInt8(header);
                }
                else
                {
                    header |= 32;
                    WriteUInt8(header);
                    WriteVUInt64(first);
                }
            }
            else
            {
                if ((uint)ints[2] < 0x10000000)
                {
                    header |= 64;
                    WriteUInt8(header);
                    WriteVUInt32((uint)ints[2]);
                    WriteInt64((long)first);
                }
                else
                {
                    header |= 64 | 32;
                    WriteUInt8(header);
                    WriteInt32(ints[2]);
                    WriteInt64((long)first);
                }
            }
        }

        public void WriteByteArray(byte[] value)
        {
            if (value == null)
            {
                WriteVUInt32(0);
                return;
            }
            WriteVUInt32((uint)(value.Length + 1));
            WriteBlock(value);
        }

        public void WriteByteArrayRaw(byte[] value)
        {
            if (value == null) return;
            WriteBlock(value);
        }

        public void WriteBlock(ByteBuffer data)
        {
            WriteBlock(data.Buffer, data.Offset, data.Length);
        }

        public void WriteIPAddress(IPAddress value)
        {
            if (value.AddressFamily == AddressFamily.InterNetworkV6)
            {
                if (value.ScopeId != 0)
                {
                    WriteUInt8(2);
                    WriteBlock(value.GetAddressBytes());
                    WriteVUInt64((ulong)value.ScopeId);
                }
                else
                {
                    WriteUInt8(1);
                    WriteBlock(value.GetAddressBytes());
                }
            }
            else
            {
                WriteUInt8(0);
#pragma warning disable 612,618
                WriteInt32LE((int) value.Address);
#pragma warning restore 612,618
            }
        }
    }
}
