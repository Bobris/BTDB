using System;

namespace BTDB.StreamLayer;

public interface IPositionLessStream : IDisposable
{
    int Read(Span<byte> data, ulong position);

    void Write(ReadOnlySpan<byte> data, ulong position);

    void Flush();

    void HardFlush();

    ulong GetSize();

    void SetSize(ulong newSize);
}
