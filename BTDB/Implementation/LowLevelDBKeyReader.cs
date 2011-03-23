using System.Diagnostics;

namespace BTDB
{
    public class LowLevelDBKeyReader : AbstractBufferedReader
    {
        readonly ILowLevelDBTransaction _transaction;
        int _ofs;
        int _keySize;

        public LowLevelDBKeyReader(ILowLevelDBTransaction transaction)
        {
            _transaction = transaction;
            Restart();
        }

        public void Restart()
        {
            _keySize = _transaction.GetKeySize();
            if (_keySize < 0) _keySize = 0;
            _ofs = 0;
            FillBuffer();
        }

        protected override sealed void FillBuffer()
        {
            Debug.Assert(Pos == End);
            if (_ofs == _keySize)
            {
                Pos = 0;
                End = -1;
                return;
            }
            _transaction.PeekKey(_ofs, out End, out Buf, out Pos);
            _ofs += End;
            End += Pos;
        }
    }
}