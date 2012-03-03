using System;

namespace BTDB.StreamLayer
{
    public class PositionLessStreamWriter : AbstractBufferedWriter, IDisposable
    {
        readonly IPositionLessStream _stream;
        ulong _ofs;

        public PositionLessStreamWriter(IPositionLessStream stream, bool atEnd = false)
        {
            _stream = stream;
            Buf = new byte[8192];
            End = Buf.Length;
            if (atEnd)
            {
                _ofs = _stream.GetSize();
            }
            else
            {
                _ofs = 0;
                _stream.SetSize(0);
            }
        }

        public override void FlushBuffer()
        {
            _stream.Write(Buf, 0, Pos, _ofs);
            _ofs += (ulong)Pos;
            Pos = 0;
        }

        public override void WriteBlock(byte[] data, int offset, int length)
        {
            if (length < Buf.Length)
            {
                base.WriteBlock(data, offset, length);
                return;
            }
            if (Pos != 0) FlushBuffer();
            _stream.Write(data, offset, length, _ofs);
            _ofs += (ulong)length;
        }

        public override long GetCurrentPosition()
        {
            return (long)(_ofs + (ulong)Pos);
        }

        public void Dispose()
        {
            if (Pos != 0) FlushBuffer();
            _stream.Dispose();
        }
    }
}