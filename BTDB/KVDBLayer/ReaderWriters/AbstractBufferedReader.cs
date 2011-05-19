using System;
using System.IO;
using System.Text;
using BTDB.KVDBLayer.Helpers;

namespace BTDB.KVDBLayer.ReaderWriters
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
        protected int Pos; // 0 for eof
        protected int End; // -1 for eof

        protected abstract void FillBuffer();

        public bool Eof
        {
            get
            {
                if (Pos == End) FillBuffer();
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
            return Buf[Pos++]!=0;
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
            return (sbyte)Buf[Pos++];
        }

        public void SkipInt8()
        {
            SkipUInt8();
        }

        public int ReadVInt32()
        {
            var res = ReadVUInt64();
            if (res > uint.MaxValue) throw new InvalidDataException(string.Format("Reading VInt32 overflowed with {0}", res));
            if ((res & 1) == 0) return (int)(res >> 1);
            return -(int)(res >> 1) - 1;
        }

        public void SkipVInt32()
        {
            var res = ReadVUInt64();
            if (res > uint.MaxValue) throw new InvalidDataException(string.Format("Reading VInt32 overflowed with {0}", res));
        }

        public uint ReadVUInt32()
        {
            var res = ReadVUInt64();
            if (res > uint.MaxValue) throw new InvalidDataException(string.Format("Reading VUInt32 overflowed with {0}", res));
            return (uint)res;
        }

        public void SkipVUInt32()
        {
            var res = ReadVUInt64();
            if (res > uint.MaxValue) throw new InvalidDataException(string.Format("Reading VUInt32 overflowed with {0}", res));
        }

        public long ReadVInt64()
        {
            var res = ReadVUInt64();
            if ((res & 1) == 0) return (long)(res >> 1);
            return -(long)(res >> 1) - 1;
        }

        public void SkipVInt64()
        {
            SkipVUInt64();
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
                res = (ulong)(Buf[Pos] & (0xff >> l));
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
                for (int i = 0; i < 8; i++)
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
                for (int i = 0; i < 8; i++)
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
                for (int i = 0; i < 4; i++)
                {
                    NeedOneByteInBuffer();
                    res <<= 8;
                    res += Buf[Pos];
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

        public string ReadString()
        {
            var len = ReadVUInt64();
            if (len == 0) return null;
            len--;
            if (len > int.MaxValue) throw new InvalidDataException(string.Format("Reading String length overflowed with {0}", len));
            var l = (int)len;
            if (l == 0) return "";
            var res = new StringBuilder(l) { Length = l };
            int i = 0;
            while (i < l)
            {
                var c = ReadVUInt32();
                if (c > 0xffff)
                {
                    if (c > 0x10ffff) throw new InvalidDataException(string.Format("Reading String unicode value overflowed with {0}", c));
                    c -= 0x10000;
                    res[i] = (char)((c >> 10) + 0xD800);
                    i++;
                    res[i] = (char)((c & 0x3FF) + 0xDC00);
                    i++;
                }
                else
                {
                    res[i] = (char)c;
                    i++;
                }
            }
            return res.ToString();
        }

        public void SkipString()
        {
            var len = ReadVUInt64();
            if (len == 0) return;
            len--;
            if (len > int.MaxValue) throw new InvalidDataException(string.Format("Reading String length overflowed with {0}", len));
            var l = (int)len;
            if (l == 0) return;
            int i = 0;
            while (i < l)
            {
                var c = ReadVUInt32();
                if (c > 0xffff)
                {
                    if (c > 0x10ffff) throw new InvalidDataException(string.Format("Reading String unicode value overflowed with {0}", c));
                    i+=2;
                }
                else
                {
                    i++;
                }
            }
        }

        public void ReadBlock(byte[] data, int offset, int length)
        {
            while (length > 0)
            {
                NeedOneByteInBuffer();
                if (Pos + length <= End)
                {
                    Array.Copy(Buf, Pos, data, offset, length);
                    Pos += length;
                    return;
                }
                var l = End - Pos;
                Array.Copy(Buf, Pos, data, offset, l);
                offset += l;
                length -= l;
                Pos += l;
            }
        }

        public void SkipBlock(int length)
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
            while (length>int.MaxValue)
            {
                SkipBlock(int.MaxValue);
                length -= int.MaxValue;
            }
            SkipBlock((int)length);
        }

        public void ReadBlock(byte[] data)
        {
            ReadBlock(data, 0, data.Length);
        }

        public Guid ReadGuid()
        {
            var res = new byte[16];
            ReadBlock(res, 0, 16);
            return new Guid(res);
        }

        public void SkipGuid()
        {
            SkipBlock(16);
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
                    first = (ulong)ReadInt64();
                    break;
                case 3:
                    second = (uint)ReadInt32();
                    first = (ulong)ReadInt64();
                    break;
            }
            var res = new decimal((int)first, (int)(first >> 32), (int)second, (header & 128) != 0, (byte)(header & 31));
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
            var bytes = new byte[length-1];
            ReadBlock(bytes);
            return bytes;
        }

        public void SkipByteArray()
        {
            var length = ReadVUInt32();
            if (length == 0) return;
            SkipBlock(length-1);
        }
    }
}
