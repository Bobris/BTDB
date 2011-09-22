using System;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public class KeyValueDBValueWriter : AbstractBufferedWriter, IDisposable
    {
        readonly IKeyValueDBTransaction _transaction;
        long _ofs;

        public KeyValueDBValueWriter(IKeyValueDBTransaction transaction)
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

        public virtual void Dispose()
        {
            if (Pos != 0) FlushBuffer();
            _transaction.SetValueSize(_ofs);
        }
    }
}