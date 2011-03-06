using System;

namespace BTDB
{
    public class LowLevelDBValueWriter : AbstractBufferedWriter, IDisposable
    {
        readonly ILowLevelDBTransaction _transaction;
        long _ofs;

        public LowLevelDBValueWriter(ILowLevelDBTransaction transaction)
        {
            _transaction = transaction;
            Buf = new byte[4096];
            End = Buf.Length;
            _ofs = 0;
        }

        public override void FlushBuffer()
        {
            _transaction.WriteValue(_ofs, Pos, Buf, 0);
            _ofs += Pos;
            Pos = 0;
        }

        public void Dispose()
        {
            if (Pos != 0) FlushBuffer();
            _transaction.SetValueSize(_ofs);
        }
    }
}