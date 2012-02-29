namespace BTDB.StreamLayer
{
    public class PositionLessStreamReader : AbstractBufferedReader
    {
        readonly IPositionLessStream _stream;
        readonly ulong _valueSize;
        ulong _ofs;

        public PositionLessStreamReader(IPositionLessStream stream)
        {
            _stream = stream;
            _valueSize = _stream.GetSize();
            _ofs = 0;
            Buf = new byte[4096];
            FillBuffer();
        }

        protected override sealed void FillBuffer()
        {
            if (_ofs == _valueSize)
            {
                Pos = 0;
                End = -1;
                return;
            }
            End = _stream.Read(Buf, 0, Buf.Length, _ofs);
            _ofs += (ulong)End;
            Pos = 0;
        }

        public override long GetCurrentPosition()
        {
            return (long)_ofs - End + Pos;
        }
    }
}