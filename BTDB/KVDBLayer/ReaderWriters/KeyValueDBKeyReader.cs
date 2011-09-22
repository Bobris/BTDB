using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public class KeyValueDBKeyReader : AbstractBufferedReader
    {
        readonly IKeyValueDBTransaction _transaction;
        int _ofs;
        int _keySize;

        public KeyValueDBKeyReader(IKeyValueDBTransaction transaction)
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