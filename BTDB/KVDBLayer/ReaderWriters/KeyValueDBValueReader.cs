using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public class KeyValueDBValueReader : AbstractBufferedReader, ICanMemorizePosition
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

        public IMemorizedPosition MemorizeCurrentPosition()
        {
            return new MemorizedPosition(this);
        }

        class MemorizedPosition : IMemorizedPosition
        {
            readonly KeyValueDBValueReader _owner;
            readonly long _ofs;

            public MemorizedPosition(KeyValueDBValueReader owner)
            {
                _owner = owner;
                if (owner.End == -1) _ofs = owner._valueSize;
                else _ofs = owner._ofs - owner.End + owner.Pos;
            }

            public void Restore()
            {
                _owner._ofs = _ofs;
                _owner.FillBuffer();
            }
        }
    }
}