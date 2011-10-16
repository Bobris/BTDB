namespace BTDB.StreamLayer
{
    public class ByteArrayReader : AbstractBufferedReader, ICanMemorizePosition
    {
        public ByteArrayReader(byte[] data)
        {
            Buf = data;
            Pos = 0;
            End = data.Length;
        }

        protected override sealed void FillBuffer()
        {
            Pos = 0;
            End = -1;
        }

        public IMemorizedPosition MemorizeCurrentPosition()
        {
            return new MemorizedPosition(this, Pos, End);
        }

        class MemorizedPosition : IMemorizedPosition
        {
            readonly ByteArrayReader _owner;
            readonly int _pos;
            readonly int _end;

            internal MemorizedPosition(ByteArrayReader owner, int pos, int end)
            {
                _owner = owner;
                _pos = pos;
                _end = end;
            }

            public void Restore()
            {
                _owner.Pos = _pos;
                _owner.End = _end;
            }
        }
    }
}