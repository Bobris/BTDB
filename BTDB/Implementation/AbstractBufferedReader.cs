using System;
using System.IO;
using System.Text;

namespace BTDB
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

        public abstract void FillBuffer();

        public bool Eof
        {
            get
            {
                if (Pos == End) FillBuffer();
                return End == -1;
            }
        }

        protected void NeedOneByteInBuffer()
        {
            if (Eof) throw new EndOfStreamException();
        }

        public int ReadVInt32()
        {
            var res = ReadVUInt64();
            if (res > uint.MaxValue) throw new InvalidDataException(string.Format("Reading VInt32 overflowed with {0}", res));
            if ((res & 1) == 0) return (int)(res >> 1);
            return -(int)(res >> 1) - 1;
        }

        public uint ReadVUInt32()
        {
            var res = ReadVUInt64();
            if (res > uint.MaxValue) throw new InvalidDataException(string.Format("Reading VUInt32 overflowed with {0}", res));
            return (uint)res;
        }

        public long ReadVInt64()
        {
            var res = ReadVUInt64();
            if ((res & 1) == 0) return (long)(res >> 1);
            return -(long)(res >> 1) - 1;
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
            }
            return res;
        }

        public string ReadString()
        {
            var len = ReadVUInt64();
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
                    if (c > 0x10ffff) throw new InvalidDataException(string.Format("Reading String unicode value overflowed with {0}", res));
                    c -= 0x10000;
                    res[i] = (char)((c >> 10) + 0xD800);
                    i++;
                    res[i] = (char)((c & 0x3FF) + 0xDC00);
                    i++;
                }
                else
                {
                    res[i] = (char) c;
                    i++;
                }
            }
            return res.ToString();
        }

        public void ReadBlock(byte[] data,int offset,int length)
        {
            while (length>0)
            {
                NeedOneByteInBuffer();
                if (Pos+length<=End)
                {
                    Array.Copy(Buf,Pos,data,offset,length);
                    Pos += length;
                    return;
                }
                var l = End - Pos;
                Array.Copy(Buf,Pos,data,offset,l);
                offset += l;
                length -= l;
                Pos += l;
            }
        }

        public Guid ReadGuid()
        {
            var res = new byte[16];
            ReadBlock(res, 0, 16);
            return new Guid(res);
        }
    }
}
