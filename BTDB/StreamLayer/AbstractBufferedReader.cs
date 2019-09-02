using System;
using System.IO;
using System.Net;
using BTDB.Buffer;

namespace BTDB.StreamLayer
{
    public abstract class AbstractBufferedReader
    {
        protected AbstractBufferedReader()
        {
            Buf = null;
            Pos = 0;
            End = 0;
        }

        protected byte[] Buf;
        protected int Pos; // -1 for eof
        protected int End; // -1 for eof
        protected char[] CharBuf;

        protected abstract void FillBuffer();

        public abstract long GetCurrentPosition();

        public bool Eof
        {
            get
            {
                if (Pos != End) return false;
                FillBuffer();
                return End == -1;
            }
        }

        void NeedOneByteInBuffer()
        {
            if (Eof) throw new EndOfStreamException();
        }

        public bool ReadBool()
        {
            NeedOneByteInBuffer();
            return Buf[Pos++] != 0;
        }

        public void SkipBool()
        {
            SkipUInt8();
        }

        public byte ReadUInt8()
        {
            NeedOneByteInBuffer();
            return Buf[Pos++];
        }

        public void SkipUInt8()
        {
            NeedOneByteInBuffer();
            Pos++;
        }

        public sbyte ReadInt8()
        {
            NeedOneByteInBuffer();
            return (sbyte) Buf[Pos++];
        }

        public void SkipInt8()
        {
            SkipUInt8();
        }

        public sbyte ReadInt8Ordered()
        {
            NeedOneByteInBuffer();
            return (sbyte) (Buf[Pos++] - 128);
        }

        public void SkipInt8Ordered()
        {
            SkipUInt8();
        }

        public short ReadVInt16()
        {
            var res = ReadVInt64();
            if (res > short.MaxValue || res < short.MinValue)
                throw new InvalidDataException(
                    $"Reading VInt16 overflowed with {res}");
            return (short) res;
        }

        public void SkipVInt16()
        {
            var res = ReadVInt64();
            if (res > short.MaxValue || res < short.MinValue)
                throw new InvalidDataException(
                    $"Skipping VInt16 overflowed with {res}");
        }

        public ushort ReadVUInt16()
        {
            var res = ReadVUInt64();
            if (res > ushort.MaxValue) throw new InvalidDataException($"Reading VUInt16 overflowed with {res}");
            return (ushort) res;
        }

        public void SkipVUInt16()
        {
            var res = ReadVUInt64();
            if (res > ushort.MaxValue) throw new InvalidDataException($"Skipping VUInt16 overflowed with {res}");
        }

        public int ReadVInt32()
        {
            var res = ReadVInt64();
            if (res > int.MaxValue || res < int.MinValue)
                throw new InvalidDataException(
                    $"Reading VInt32 overflowed with {res}");
            return (int) res;
        }

        public void SkipVInt32()
        {
            var res = ReadVInt64();
            if (res > int.MaxValue || res < int.MinValue)
                throw new InvalidDataException(
                    $"Skipping VInt32 overflowed with {res}");
        }

        public uint ReadVUInt32()
        {
            var res = ReadVUInt64();
            if (res > uint.MaxValue) throw new InvalidDataException($"Reading VUInt32 overflowed with {res}");
            return (uint) res;
        }

        public void SkipVUInt32()
        {
            var res = ReadVUInt64();
            if (res > uint.MaxValue) throw new InvalidDataException($"Skipping VUInt32 overflowed with {res}");
        }

        public long ReadVInt64()
        {
            NeedOneByteInBuffer();
            var l = PackUnpack.LengthVInt(Buf, Pos);
            long res;
            if (Pos + l <= End)
            {
                res = PackUnpack.UnpackVInt(Buf, ref Pos);
            }
            else
            {
                res = (Buf[Pos] >= 0x80) ? 0 : -1;
                if (l < 8) res <<= 8 - l;
                res += Buf[Pos] & (0xff >> l);
                do
                {
                    Pos++;
                    res <<= 8;
                    NeedOneByteInBuffer();
                    res += Buf[Pos];
                    l--;
                } while (l > 1);

                Pos++;
            }

            return res;
        }

        public void SkipVInt64()
        {
            NeedOneByteInBuffer();
            var l = PackUnpack.LengthVInt(Buf, Pos);
            if (Pos + l <= End)
            {
                Pos += l;
            }
            else
            {
                do
                {
                    Pos++;
                    NeedOneByteInBuffer();
                    l--;
                } while (l > 1);

                Pos++;
            }
        }

        public ulong ReadVUInt64()
        {
            NeedOneByteInBuffer();
            var l = PackUnpack.LengthVUInt(Buf, Pos);
            ulong res;
            if (Pos + l <= End)
            {
                res = PackUnpack.UnpackVUInt(Buf, ref Pos);
            }
            else
            {
                res = (ulong) (Buf[Pos] & (0xff >> l));
                do
                {
                    Pos++;
                    res <<= 8;
                    NeedOneByteInBuffer();
                    res += Buf[Pos];
                    l--;
                } while (l > 1);

                Pos++;
            }

            return res;
        }

        public void SkipVUInt64()
        {
            NeedOneByteInBuffer();
            var l = PackUnpack.LengthVUInt(Buf, Pos);
            if (Pos + l <= End)
            {
                Pos += l;
            }
            else
            {
                do
                {
                    Pos++;
                    NeedOneByteInBuffer();
                    l--;
                } while (l > 1);

                Pos++;
            }
        }

        public long ReadInt64()
        {
            NeedOneByteInBuffer();
            long res = 0;
            if (Pos + 8 <= End)
            {
                res = PackUnpack.UnpackInt64BE(Buf, Pos);
                Pos += 8;
            }
            else
            {
                for (var i = 0; i < 8; i++)
                {
                    NeedOneByteInBuffer();
                    res <<= 8;
                    res += Buf[Pos];
                    Pos++;
                }
            }

            return res;
        }

        public void SkipInt64()
        {
            NeedOneByteInBuffer();
            if (Pos + 8 <= End)
            {
                Pos += 8;
            }
            else
            {
                for (var i = 0; i < 8; i++)
                {
                    NeedOneByteInBuffer();
                    Pos++;
                }
            }
        }

        public int ReadInt32()
        {
            NeedOneByteInBuffer();
            int res = 0;
            if (Pos + 4 <= End)
            {
                res = PackUnpack.UnpackInt32BE(Buf, Pos);
                Pos += 4;
            }
            else
            {
                for (var i = 0; i < 4; i++)
                {
                    NeedOneByteInBuffer();
                    res <<= 8;
                    res += Buf[Pos];
                    Pos++;
                }
            }

            return res;
        }

        public int ReadInt32LE()
        {
            NeedOneByteInBuffer();
            int res = 0;
            if (Pos + 4 <= End)
            {
                res = PackUnpack.UnpackInt32LE(Buf, Pos);
                Pos += 4;
            }
            else
            {
                for (var rot = 0; rot < 32; rot += 8)
                {
                    NeedOneByteInBuffer();
                    res += Buf[Pos] << rot;
                    Pos++;
                }
            }

            return res;
        }

        public void SkipInt32()
        {
            NeedOneByteInBuffer();
            if (Pos + 4 <= End)
            {
                Pos += 4;
            }
            else
            {
                for (int i = 0; i < 4; i++)
                {
                    NeedOneByteInBuffer();
                    Pos++;
                }
            }
        }

        public DateTime ReadDateTime()
        {
            return DateTime.FromBinary(ReadInt64());
        }

        public void SkipDateTime()
        {
            SkipInt64();
        }

        public TimeSpan ReadTimeSpan()
        {
            return new TimeSpan(ReadVInt64());
        }

        public void SkipTimeSpan()
        {
            SkipVInt64();
        }

        protected void ReserveCharBuf(int size)
        {
            if (CharBuf == null)
            {
                CharBuf = new char[size];
            }
            else
            {
                if (size <= CharBuf.Length) return;
                var newLen = Math.Max((int) Math.Min((long) CharBuf.Length * 2, 2147483591 / 2), size);
                Array.Resize(ref CharBuf, newLen);
            }
        }

        public string ReadString()
        {
            var len = ReadVUInt64();
            if (len == 0) return null;
            len--;
            if (len > int.MaxValue) throw new InvalidDataException($"Reading String length overflowed with {len}");
            var l = (int) len;
            if (l == 0) return "";
            ReserveCharBuf(l);
            var res = CharBuf;
            var i = 0;
            while (i < l)
            {
                if (Pos != End)
                {
                    var b = Buf[Pos];
                    if (b < 0x80)
                    {
                        res[i] = (char) b;
                        i++;
                        Pos++;
                        continue;
                    }
                }

                var c = ReadVUInt64();
                if (c > 0xffff)
                {
                    if (c > 0x10ffff)
                        throw new InvalidDataException($"Reading String unicode value overflowed with {c}");
                    c -= 0x10000;
                    res[i] = (char) ((c >> 10) + 0xD800);
                    i++;
                    res[i] = (char) ((c & 0x3FF) + 0xDC00);
                    i++;
                }
                else
                {
                    res[i] = (char) c;
                    i++;
                }
            }

            return new string(res, 0, l);
        }

        public string ReadStringOrdered()
        {
            var len = 0;
            while (true)
            {
                var c = ReadVUInt32();
                if (c == 0) break;
                c--;
                if (c > 0xffff)
                {
                    if (c > 0x10ffff)
                    {
                        if (len == 0 && c == 0x110000) return null;
                        throw new InvalidDataException($"Reading String unicode value overflowed with {c}");
                    }

                    c -= 0x10000;
                    ReserveCharBuf(len + 2);
                    CharBuf[len++] = (char) ((c >> 10) + 0xD800);
                    CharBuf[len++] = (char) ((c & 0x3FF) + 0xDC00);
                }
                else
                {
                    ReserveCharBuf(len + 1);
                    CharBuf[len++] = (char) c;
                }
            }

            if (len == 0) return "";
            return new string(CharBuf, 0, len);
        }

        public void SkipString()
        {
            var len = ReadVUInt64();
            if (len == 0) return;
            len--;
            if (len > int.MaxValue) throw new InvalidDataException($"Skipping String length overflowed with {len}");
            var l = (int) len;
            if (l == 0) return;
            int i = 0;
            while (i < l)
            {
                var c = ReadVUInt64();
                if (c > 0xffff)
                {
                    if (c > 0x10ffff)
                        throw new InvalidDataException(
                            $"Skipping String unicode value overflowed with {c}");
                    i += 2;
                }
                else
                {
                    i++;
                }
            }
        }

        public void SkipStringOrdered()
        {
            var c = ReadVUInt32();
            if (c == 0) return;
            c--;
            if (c > 0x10ffff)
            {
                if (c == 0x110000) return;
                throw new InvalidDataException($"Skipping String unicode value overflowed with {c}");
            }

            while (true)
            {
                c = ReadVUInt32();
                if (c == 0) break;
                c--;
                if (c > 0x10ffff) throw new InvalidDataException($"Skipping String unicode value overflowed with {c}");
            }
        }


        public virtual void ReadBlock(Span<byte> buffer)
        {
            while (buffer.Length > 0)
            {
                NeedOneByteInBuffer();
                if (Pos + buffer.Length <= End)
                {
                    Buf.AsSpan(Pos, buffer.Length).CopyTo(buffer);
                    Pos += buffer.Length;
                    return;
                }

                var l = End - Pos;
                Buf.AsSpan(Pos, l).CopyTo(buffer);
                buffer = buffer.Slice(l);
                Pos += l;
            }
        }

        public void ReadBlock(byte[] data, int offset, int length)
        {
            ReadBlock(data.AsSpan(offset, length));
        }

        public virtual void SkipBlock(int length)
        {
            while (length > 0)
            {
                NeedOneByteInBuffer();
                if (Pos + length <= End)
                {
                    Pos += length;
                    return;
                }

                var l = End - Pos;
                length -= l;
                Pos += l;
            }
        }

        public void SkipBlock(uint length)
        {
            while (length > int.MaxValue)
            {
                SkipBlock(int.MaxValue);
                length -= int.MaxValue;
            }

            SkipBlock((int) length);
        }

        public void ReadBlock(ByteBuffer buffer)
        {
            ReadBlock(buffer.AsSyncSpan());
        }

        public Guid ReadGuid()
        {
            Span<byte> res = stackalloc byte[16];
            ReadBlock(res);
            return new Guid(res);
        }

        public void SkipGuid()
        {
            SkipBlock(16);
        }

        public float ReadSingle()
        {
            return new Int32SingleUnion(ReadInt32()).AsSingle;
        }

        public void SkipSingle()
        {
            SkipInt32();
        }

        public double ReadDouble()
        {
            return BitConverter.Int64BitsToDouble(ReadInt64());
        }

        public void SkipDouble()
        {
            SkipInt64();
        }

        public decimal ReadDecimal()
        {
            var header = ReadUInt8();
            ulong first = 0;
            uint second = 0;
            switch (header >> 5 & 3)
            {
                case 0:
                    break;
                case 1:
                    first = ReadVUInt64();
                    break;
                case 2:
                    second = ReadVUInt32();
                    first = (ulong) ReadInt64();
                    break;
                case 3:
                    second = (uint) ReadInt32();
                    first = (ulong) ReadInt64();
                    break;
            }

            var res = new decimal((int) first, (int) (first >> 32), (int) second, (header & 128) != 0,
                (byte) (header & 31));
            return res;
        }

        public void SkipDecimal()
        {
            var header = ReadUInt8();
            switch (header >> 5 & 3)
            {
                case 0:
                    break;
                case 1:
                    SkipVUInt64();
                    break;
                case 2:
                    SkipVUInt32();
                    SkipInt64();
                    break;
                case 3:
                    SkipInt32();
                    SkipInt64();
                    break;
            }
        }

        public byte[] ReadByteArray()
        {
            var length = ReadVUInt32();
            if (length == 0) return null;
            var bytes = new byte[length - 1];
            ReadBlock(bytes);
            return bytes;
        }

        public void SkipByteArray()
        {
            var length = ReadVUInt32();
            if (length == 0) return;
            SkipBlock(length - 1);
        }

        public byte[] ReadByteArrayRawTillEof()
        {
            byte[] buffer = null;
            int length = 0;
            while (!Eof)
            {
                var l = End - Pos;
                Array.Resize(ref buffer, length + l);
                Array.Copy(Buf, Pos, buffer, length, l);
                length += l;
                Pos = End;
            }

            if (buffer == null) return Array.Empty<byte>();
            return buffer;
        }

        public byte[] ReadByteArrayRaw(int len)
        {
            var res = new byte[len];
            ReadBlock(res);
            return res;
        }

        public bool CheckMagic(byte[] magic)
        {
            try
            {
                Span<byte> buf = stackalloc byte[magic.Length];
                ReadBlock(buf);
                if (buf.SequenceEqual(magic))
                {
                    return true;
                }
            }
            catch (Exception)
            {
                return false;
            }

            return false;
        }

        public IPAddress ReadIPAddress()
        {
            switch (ReadUInt8())
            {
                case 0:
                    return new IPAddress((uint) ReadInt32LE());
                case 1:
                {
                    Span<byte> ip6Bytes = stackalloc byte[16];
                    ReadBlock(ip6Bytes);
                    return new IPAddress(ip6Bytes);
                }
                case 2:
                {
                    Span<byte> ip6Bytes = stackalloc byte[16];
                    ReadBlock(ip6Bytes);
                    var scopeid = (long) ReadVUInt64();
                    return new IPAddress(ip6Bytes, scopeid);
                }
                case 3:
                    return null;
                default: throw new InvalidDataException("Unknown type of IPAddress");
            }
        }

        public void SkipIPAddress()
        {
            switch (ReadUInt8())
            {
                case 0:
                    SkipInt32();
                    return;
                case 1:
                    SkipBlock(16);
                    return;
                case 2:
                    SkipBlock(16);
                    SkipVUInt64();
                    return;
                case 3:
                    return;
                default: throw new InvalidDataException("Unknown type of IPAddress");
            }
        }
    }
}
