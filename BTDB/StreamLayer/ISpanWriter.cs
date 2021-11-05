namespace BTDB.StreamLayer;

public interface ISpanWriter
{
    /// <summary>
    /// Restore/Prepare writing buffer.
    /// </summary>
    /// <param name="spanWriter">owning SpanWriter</param>
    void Init(ref SpanWriter spanWriter);
    /// <summary>
    /// Remember actual position from _buf into controller. SpanWriter will go out of scope.
    /// </summary>
    /// <param name="spanWriter">owning SpanWriter</param>
    void Sync(ref SpanWriter spanWriter);
    /// <summary>
    /// Flush writing buffer and prepare another one. After calling this there must be at least 16 bytes free in _buf.
    /// </summary>
    /// <returns>false if client must use spanWriter.Buf completely before calling Flush, it must return true if spanWriter.Buf is empty</returns>
    /// <param name="spanWriter">owning SpanWriter</param>
    bool Flush(ref SpanWriter spanWriter);
    /// <summary>
    /// Calculate current position with help of spanWriter._buf.
    /// </summary>
    /// <param name="spanWriter">owning SpanWriter</param>
    /// <returns>byte offset from start</returns>
    long GetCurrentPosition(in SpanWriter spanWriter);
    /// <summary>
    /// Get current position stored in this controller.
    /// </summary>
    /// <returns>byte offset from start</returns>
    long GetCurrentPositionWithoutWriter();
    /// <summary>
    /// Writes data from buffer. Called only in case when Flush does not creates big enough buffer.
    /// </summary>
    /// <param name="spanWriter">owning SpanWriter</param>
    /// <param name="buffer">reference to first byte of buffer to write with length bytes</param>
    /// <param name="length">size of data to write</param>
    void WriteBlock(ref SpanWriter spanWriter, ref byte buffer, uint length);
    /// <summary>
    /// Writes data from buffer. Called only when SpanWriter does not exists.
    /// </summary>
    /// <param name="buffer">reference to first byte of buffer to write with length bytes</param>
    /// <param name="length">size of data to write</param>
    void WriteBlockWithoutWriter(ref byte buffer, uint length);
    /// <summary>
    /// Seek to position in SpanWriter.
    /// </summary>
    /// <param name="spanWriter">owning SpanWriter</param>
    /// <param name="position">new position to set</param>
    void SetCurrentPosition(ref SpanWriter spanWriter, long position);
}
