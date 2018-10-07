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
            End = _stream.Read(Buf.AsSpan(), _ofs);
            _ofs += (ulong)End;
            Pos = 0;
        }

        public override void ReadBlock(Span<byte> data)
        {
            if (data.Length < Buf.Length)
            {
                base.ReadBlock(data);
                return;
            }
            var l = End - Pos;
            Buf.AsSpan(Pos, l).CopyTo(data);
            data = data.Slice(l);
            Pos += l;

            while (data.Length > 0)
            {
                var read = _stream.Read(data, _ofs);
                if (read <= 0)
                {
                    _ofs = _valueSize;
                    Pos = -1;
                    End = -1;
                    throw new EndOfStreamException();
                }
                _ofs += (ulong)read;
                data = data.Slice(read);
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
