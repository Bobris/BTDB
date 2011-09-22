using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public class KeyValueDBValueReader : AbstractBufferedReader
    {
        readonly IKeyValueDBTransaction _transaction;
        long _ofs;
        long _valueSize;

        public KeyValueDBValueReader(IKeyValueDBTransaction transaction)
        {
            _transaction = transaction;
            Restart();
        }

        public void Restart()
        {
            _valueSize = _transaction.GetValueSize();
            if (_valueSize < 0) _valueSize = 0;
            _ofs = 0;
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
            _transaction.PeekValue(_ofs, out End, out Buf, out Pos);
            _ofs += End;
            End += Pos;
        }
    }
}