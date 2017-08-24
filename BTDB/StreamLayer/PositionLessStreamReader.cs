using System;
using System.IO;

namespace BTDB.StreamLayer
{
    public class PositionLessStreamReader : AbstractBufferedReader
    {
        readonly IPositionLessStream _stream;
        readonly ulong _valueSize;
        ulong _ofs;

        public PositionLessStreamReader(IPositionLessStream stream): this(stream, 8192)
        {
        }

        public PositionLessStreamReader(IPositionLessStream stream, int bufferSize)
        {
            if(bufferSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(bufferSize));

            _stream = stream;
            _valueSize = _stream.GetSize();
            _ofs = 0;
            Buf = new byte[bufferSize];
            FillBuffer();
        }

        protected sealed override void FillBuffer()
        {
            if (_ofs == _valueSize)
            {
                Pos = -1;
                End = -1;
                return;
            }
            End = _stream.Read(Buf, 0, Buf.Length, _ofs);
            _ofs += (ulong)End;
            Pos = 0;
        }

        public override void ReadBlock(byte[] data, int offset, int length)
        {
            if (length < Buf.Length)
            {
                base.ReadBlock(data, offset, length);
                return;
            }
            var l = End - Pos;
            Array.Copy(Buf, Pos, data, offset, l);
            offset += l;
            length -= l;
            Pos += l;

            while (length > 0)
            {
                var readed = _stream.Read(data, offset, length, _ofs);
                if (readed <= 0)
                {
                    _ofs = _valueSize;
                    Pos = -1;
                    End = -1;
                    throw new EndOfStreamException();
                }
                _ofs += (ulong)readed;
                offset += readed;
                length -= readed;
            }
        }

        public override void SkipBlock(int length)
        {
            if (length < Buf.Length)
            {
                base.SkipBlock(length);
                return;
            }
            if (GetCurrentPosition() + length > (long)_valueSize)
            {
                _ofs = _valueSize;
                Pos = -1;
                End = -1;
                throw new EndOfStreamException();
            }
            var l = End - Pos;
            Pos = End;
            length -= l;
            _ofs += (ulong)length;
        }

        public override long GetCurrentPosition()
        {
            return (long)_ofs - End + Pos;
        }
    }
}