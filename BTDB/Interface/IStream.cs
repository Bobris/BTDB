using System;

namespace BTDB
{
    public interface IStream : IDisposable
    {
        int Read(byte[] data, int offset, int size, ulong position);

        void Write(byte[] data, int offset, int size, ulong position);

        void Flush();

        ulong GetSize();

        void SetSize(ulong newSize);
    }
}
