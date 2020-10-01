using System;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using Microsoft.Extensions.Primitives;

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

        protected byte[]? Buf;
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

            Buf[Pos++] = (byte) (value ? 1 : 0);
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

            Buf[Pos++] = (byte) value;
        }

        public void WriteInt8Ordered(sbyte value)
        {
            if (Pos >= End)
            {
                FlushBuffer();
            }

            Buf[Pos++] = (byte) (value + 128);
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
            var len = PackUnpack.LengthVInt(value);
            if (Pos + len > End)
            {
                FlushBuffer();
                if (Pos + len > End)
                {
                    Span<byte> buf = stackalloc byte[len];
                    PackUnpack.UnsafePackVInt(ref MemoryMarshal.GetReference(buf), value, len);
                    WriteBlock(buf);
                    return;
                }
            }

            PackUnpack.UnsafePackVInt(ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(Buf.AsSpan()), (IntPtr)Pos), value, len);
            Pos += len;
        }

        public void WriteVUInt64(ulong value)
        {
            var len = PackUnpack.LengthVUInt(value);
            if (Pos + len > End)
            {
                FlushBuffer();
                if (Pos + len > End)
                {
                    Span<byte> b = stackalloc byte[len];
                    PackUnpack.UnsafePackVUInt(ref MemoryMarshal.GetReference(b), value, len);
                    WriteBlock(b);
                    return;
                }
            }

            PackUnpack.UnsafePackVUInt(ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(Buf.AsSpan()), (IntPtr)Pos), value, len);
            Pos += len;
        }

        public void WriteInt64(long value)
        {
            if (Pos + 8 > End)
            {
                FlushBuffer();
                if (Pos + 8 > End)
                {
                    Span<byte> buf = stackalloc byte[8];
                    BinaryPrimitives.WriteInt64BigEndian(buf, value);
                    WriteBlock(buf);
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
                    Span<byte> buf = stackalloc byte[4];
                    BinaryPrimitives.WriteInt32BigEndian(buf, value);
                    WriteBlock(buf);
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
            {
                if (value == DateTime.MinValue)
                    value = DateTime.MinValue.ToUniversalTime();
                else if (value == DateTime.MaxValue)
                    value = DateTime.MaxValue.ToUniversalTime();
                else
                    throw new ArgumentOutOfRangeException(nameof(value),
                        "DateTime.Kind cannot be stored as Unspecified");
            }

            WriteDateTime(value);
        }

        public void WriteDateTimeOffset(DateTimeOffset value)
        {
            WriteVInt64(value.Ticks);
            WriteTimeSpan(value.Offset);
        }

        public void WriteTimeSpan(TimeSpan value)
        {
            WriteVInt64(value.Ticks);
        }

        public unsafe void WriteString(string? value)
        {
            if (value == null)
            {
                WriteByteZero();
                return;
            }

            var l = value.Length;
            if (l == 0)
            {
                WriteUInt8(1);
                return;
            }

            var buf = Buf;
            var pos = Pos;
            var end = End;
            fixed (char* strPtrStart = value)
            {
                var strPtr = strPtrStart;
                var strPtrEnd = strPtrStart + l;
                var toEncode = (uint) (l + 1);
                doEncode:
                var toEncodeLen = PackUnpack.LengthVUInt(toEncode);
                if (pos + toEncodeLen <= end)
                {
                    PackUnpack.UnsafePackVUInt(ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(buf.AsSpan()), (IntPtr)pos), toEncode, toEncodeLen);
                    pos += toEncodeLen;
                }
                else
                {
                    Pos = pos;
                    WriteVUInt32(toEncode);
                    buf = Buf;
                    pos = Pos;
                    end = End;
                }

                while (strPtr != strPtrEnd)
                {
                    var c = *strPtr++;
                    if (c < 0x80)
                    {
                        if (pos >= end)
                        {
                            Pos = pos;
                            FlushBuffer();
                            buf = Buf;
                            pos = Pos;
                            end = End;
                        }

                        buf[pos++] = (byte) c;
                    }
                    else
                    {
                        if (char.IsHighSurrogate(c) && strPtr != strPtrEnd)
                        {
                            var c2 = *strPtr;
                            if (char.IsLowSurrogate(c2))
                            {
                                toEncode = (uint) ((c - 0xD800) * 0x400 + (c2 - 0xDC00) + 0x10000);
                                strPtr++;
                                goto doEncode;
                            }
                        }

                        toEncode = c;
                        goto doEncode;
                    }
                }

                Pos = pos;
            }
        }

        public void WriteStringOrdered(string? value)
        {
            if (value == null)
            {
                WriteVUInt32(0x110001);
                return;
            }

            var l = value.Length;
            var i = 0;
            while (i < l)
            {
                var c = value[i];
                if (char.IsHighSurrogate(c) && i + 1 < l)
                {
                    var c2 = value[i + 1];
                    if (char.IsLowSurrogate(c2))
                    {
                        WriteVUInt32((uint) ((c - 0xD800) * 0x400 + (c2 - 0xDC00) + 0x10000) + 1);
                        i += 2;
                        continue;
                    }
                }

                WriteVUInt32((uint) c + 1);
                i++;
            }

            WriteByteZero();
        }

        public virtual void WriteBlock(ReadOnlySpan<byte> data)
        {
            while (data.Length > 0)
            {
                if (Pos >= End) FlushBuffer();
                var l = End - Pos;
                if (data.Length < l) l = data.Length;
                data.Slice(0, l).CopyTo(new Span<byte>(Buf, Pos, l));
                data = data.Slice(l);
                Pos += l;
            }
        }

        public void WriteBlock(byte[] buffer, int offset, int length)
        {
            WriteBlock(buffer.AsSpan(offset, length));
        }

        public unsafe void WriteBlock(IntPtr data, int length)
        {
            WriteBlock(new ReadOnlySpan<byte>(data.ToPointer(), length));
        }

        public void WriteBlock(byte[] data)
        {
            WriteBlock(data.AsSpan());
        }

        public unsafe void WriteGuid(Guid value)
        {
            WriteBlock(new ReadOnlySpan<byte>((byte*) &value, 16));
        }

        public void WriteSingle(float value)
        {
            WriteInt32(BitConverter.SingleToInt32Bits(value));
        }

        public void WriteDouble(double value)
        {
            WriteInt64(BitConverter.DoubleToInt64Bits(value));
        }

        public void WriteDecimal(decimal value)
        {
            var ints = decimal.GetBits(value);
            var header = (byte) ((ints[3] >> 16) & 31);
            if (ints[3] < 0) header |= 128;
            var first = (uint) ints[0] + ((ulong) ints[1] << 32);
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
                if ((uint) ints[2] < 0x10000000)
                {
                    header |= 64;
                    WriteUInt8(header);
                    WriteVUInt32((uint) ints[2]);
                    WriteInt64((long) first);
                }
                else
                {
                    header |= 64 | 32;
                    WriteUInt8(header);
                    WriteInt32(ints[2]);
                    WriteInt64((long) first);
                }
            }
        }

        public void WriteByteArray(byte[]? value)
        {
            if (value == null)
            {
                WriteVUInt32(0);
                return;
            }

            WriteVUInt32((uint) (value.Length + 1));
            WriteBlock(value);
        }

        public void WriteByteArray(ByteBuffer value)
        {
            WriteVUInt32((uint) (value.Length + 1));
            WriteBlock(value);
        }

        public void WriteByteArrayRaw(byte[]? value)
        {
            if (value == null) return;
            WriteBlock(value);
        }

        public void WriteBlock(ByteBuffer data)
        {
            WriteBlock(data.AsSyncReadOnlySpan());
        }

        public void WriteIPAddress(IPAddress? value)
        {
            if (value == null)
            {
                WriteUInt8(3);
                return;
            }

            if (value.AddressFamily == AddressFamily.InterNetworkV6)
            {
                Span<byte> buf = stackalloc byte[16];
                value.TryWriteBytes(buf, out _);
                if (value.ScopeId != 0)
                {
                    WriteUInt8(2);
                    WriteBlock(buf);
                    WriteVUInt64((ulong) value.ScopeId);
                }
                else
                {
                    WriteUInt8(1);
                    WriteBlock(buf);
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

        public void WriteVersion(Version? value)
        {
            if (value == null)
            {
                WriteUInt8(0);
                return;
            }

            WriteVUInt32((uint) value.Major + 1);
            WriteVUInt32((uint) value.Minor + 1);
            if (value.Minor == -1) return;
            WriteVUInt32((uint) value.Build + 1);
            if (value.Build == -1) return;
            WriteVUInt32((uint) value.Revision + 1);
        }

        public void WriteStringValues(StringValues value)
        {
            WriteVUInt32((uint)value.Count);
            foreach (var s in value)
            {
                WriteString(s);
            }
        }
    }
}
