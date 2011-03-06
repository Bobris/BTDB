using System.Diagnostics;

namespace BTDB
{
    public class LowLevelDBValueReader : AbstractBufferedReader
    {
        readonly ILowLevelDBTransaction _transaction;
        long _ofs;
        readonly long _valueSize;

        public LowLevelDBValueReader(ILowLevelDBTransaction transaction)
        {
            _transaction = transaction;
            _valueSize = _transaction.GetValueSize();
            _ofs = 0;
            FillBuffer();
        }

        protected override sealed void FillBuffer()
        {
            Debug.Assert(Pos == End);
            if (_ofs == _valueSize)
            {
                Pos = 0;
                End = -1;
            }
            _transaction.PeekValue(_ofs, out End, out Buf, out Pos);
            _ofs += End;
            End += Pos;
        }
    }
}