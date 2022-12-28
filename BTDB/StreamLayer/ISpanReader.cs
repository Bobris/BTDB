using System;

namespace BTDB.StreamLayer;

public interface ISpanReader
{
    /// <summary>
    /// Fill Buf at creation of SpanReader.
    /// </summary>
    /// <param name="spanReader">owning SpanReader</param>
    void Init(ref SpanReader spanReader);
    /// <summary>
    /// Fill Buf at least 1 byte of data or return true if there at end of stream.
    /// </summary>
    /// <param name="spanReader">owning SpanReader</param>
    /// <returns>true if at end of stream (spanReader.Buf must be empty)</returns>
    bool FillBufAndCheckForEof(ref SpanReader spanReader);
    /// <summary>
    /// Calculate current position with help of spanReader.Buf.
    /// </summary>
    /// <param name="spanReader">owning SpanReader</param>
    /// <returns>byte offset from start</returns>
    long GetCurrentPosition(in SpanReader spanReader);
    /// <summary>
    /// Reads data into buffer. Called only when spanReader.Buf is empty.
    /// </summary>
    /// <param name="spanReader">owning SpanReader</param>
    /// <param name="buffer">reference to first byte of buffer to fill with length bytes</param>
    /// <param name="length">size of data to read</param>
    /// <returns>true if not enough data to fill buffer</returns>
    bool ReadBlock(ref SpanReader spanReader, ref byte buffer, uint length);
    /// <summary>
    /// Skip data of length. Called only when spanReader.Buf is empty.
    /// </summary>
    /// <param name="spanReader">owning SpanReader</param>
    /// <param name="length">how much to skip</param>
    /// <returns>true if not enough data to fill buffer</returns>
    bool SkipBlock(ref SpanReader spanReader, uint length);
    /// <summary>
    /// Seek to position in SpanReader.
    /// </summary>
    /// <param name="spanReader">owning SpanReader</param>
    /// <param name="position">new position to set</param>
    void SetCurrentPosition(ref SpanReader spanReader, long position);

    /// <summary>
    /// Called from SpanReader.Sync() to signal that current position needs to be remembered in this object.
    /// </summary>
    /// <param name="spanReader">owning SpanReader</param>
    void Sync(ref SpanReader spanReader);

    /// <summary>
    /// Try to return original continuous block of memory from storage
    /// </summary>
    /// <param name="spanReader">owning SpanReader</param>
    /// <param name="length">how many bytes to read</param>
    /// <param name="result">resulting ReadOnlyMemory</param>
    /// <returns>true if possible</returns>
    bool TryReadBlockAsMemory(ref SpanReader spanReader, uint length, out ReadOnlyMemory<byte> result)
    {
        result = new();
        return false;
    }
}
