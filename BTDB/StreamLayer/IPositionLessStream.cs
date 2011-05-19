using System;

namespace BTDB.StreamLayer
{
    public interface IPositionLessStream : IDisposable
    {
        int Read(byte[] data, int offset, int size, ulong position);

        void Write(byte[] data, int offset, int size, ulong position);

        void Flush();

        void HardFlush();

        ulong GetSize();

        void SetSize(ulong newSize);
    }
}
