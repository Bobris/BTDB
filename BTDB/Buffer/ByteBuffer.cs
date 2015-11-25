using System;

namespace BTDB.Buffer
{
    public struct ByteBuffer
    {
        byte[] _buffer;
        uint _offset;
        readonly int _length;

        public static ByteBuffer NewAsync(byte[] buffer)
        {
            return new ByteBuffer(buffer, 0, buffer.Length);
        }

        public static ByteBuffer NewAsync(byte[] buffer, int offset, int length)
        {
            return new ByteBuffer(buffer, (uint)offset, length);
        }

        public static ByteBuffer NewSync(byte[] buffer)
        {
            return new ByteBuffer(buffer, 0x80000000u, buffer.Length);
        }

        public static ByteBuffer NewSync(byte[] buffer, int offset, int length)
        {
            return new ByteBuffer(buffer, (((uint)offset) | 0x80000000u), length);
        }

        public static ByteBuffer NewEmpty()
        {
            return new ByteBuffer(BitArrayManipulation.EmptyByteArray, 0, 0);
        }

        ByteBuffer(byte[] buffer, uint offset, int length)
        {
            _buffer = buffer;
            _offset = offset;
            _length = length;
        }

        public byte[] Buffer => _buffer;
        public int Offset => (int)(_offset & 0x7fffffffu);
        public int Length => _length;
        public bool AsyncSafe => (_offset & 0x80000000u) == 0u;

        public byte this[int index]
        {
            get { return _buffer[Offset + index]; }
            set
            {
                _buffer[Offset + index] = value;
            }
        }

        public ByteBuffer SubBuffer(int offset)
        {
            return AsyncSafe ? NewAsync(Buffer, Offset + offset, Length - offset) : NewSync(Buffer, Offset + offset, Length - offset);
        }

        public ByteBuffer SubBuffer(int offset, int length)
        {
            return AsyncSafe ? NewAsync(Buffer, Offset + offset, length) : NewSync(Buffer, Offset + offset, length);
        }

        public ByteBuffer ToAsyncSafe()
        {
            if (AsyncSafe) return this;
            var copy = new byte[_length];
            Array.Copy(_buffer, Offset, copy, 0, _length);
            return NewAsync(copy);
        }

        public void MakeAsyncSafe()
        {
            if (AsyncSafe) return;
            var copy = new byte[_length];
            Array.Copy(_buffer, Offset, copy, 0, _length);
            _buffer = copy;
            _offset = 0;
        }

        public ArraySegment<byte> ToArraySegment()
        {
            return new ArraySegment<byte>(Buffer, Offset, Length);
        }

        public byte[] ToByteArray()
        {
            var safeSelf = ToAsyncSafe();
            var buf = safeSelf.Buffer ?? BitArrayManipulation.EmptyByteArray;
            if (safeSelf.Offset == 0 && safeSelf.Length == buf.Length)
            {
                return buf;
            }
            var copy = new byte[safeSelf.Length];
            Array.Copy(safeSelf.Buffer, safeSelf.Offset, copy, 0, safeSelf.Length);
            return copy;
        }

        public ByteBuffer ResizingAppend(ByteBuffer append)
        {
            if (AsyncSafe)
            {
                if (Offset + Length + append.Length <= Buffer.Length)
                {
                    Array.Copy(append.Buffer, append.Offset, Buffer, Offset + Length, append.Length);
                    return NewAsync(Buffer, Offset, Length + append.Length);
                }
            }
            var newCapacity = Math.Max(Length + append.Length, Length * 2);
            var newBuffer = new byte[newCapacity];
            Array.Copy(Buffer, Offset, newBuffer, 0, Length);
            Array.Copy(append.Buffer, append.Offset, newBuffer, Length, append.Length);
            return NewAsync(newBuffer, 0, Length + append.Length);
        }
    }
}
