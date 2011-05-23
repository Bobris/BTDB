using System;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer.ReaderWriters
{
    public class PositionLessStreamWriter : AbstractBufferedWriter, IDisposable
    {
        readonly IPositionLessStream _stream;
        ulong _ofs;

        public PositionLessStreamWriter(IPositionLessStream stream)
        {
            _stream = stream;
            Buf = new byte[4096];
            End = Buf.Length;
            _ofs = 0;
            _stream.SetSize(0);
        }

        public override void FlushBuffer()
        {
            _stream.Write(Buf,0,Pos,_ofs);
            _stream.Flush();
            _ofs += (ulong) Pos;
            Pos = 0;
        }

        public void Dispose()
        {
            if (Pos != 0) FlushBuffer();
            _stream.Dispose();
        }
    }
}