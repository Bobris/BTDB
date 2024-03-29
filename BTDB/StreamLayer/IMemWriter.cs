namespace BTDB.StreamLayer;

public interface IMemWriter
{
    /// <summary>
    /// Restore/Prepare writing buffer.
    /// </summary>
    /// <param name="memWriter">owning MemWriter</param>
    void Init(ref MemWriter memWriter);

    /// <summary>
    /// Flush writing buffer and prepare another one.
    /// </summary>
    /// <param name="memWriter">owning MemWriter</param>
    /// <param name="spaceNeeded">if possible reserve this amount of bytes, 0 is special value to flush buffer to storage and does not need to prepare at least 1 byte</param>
    void Flush(ref MemWriter memWriter, uint spaceNeeded);

    /// <summary>
    /// Calculate current position with help of memWriter.Current.
    /// </summary>
    /// <param name="memWriter">owning MemWriter</param>
    /// <returns>byte offset from start</returns>
    long GetCurrentPosition(in MemWriter memWriter);

    /// <summary>
    /// Writes data from buffer. Called only in case when free space till end of buffer is zero. Usually it needs to start by flushing buffer.
    /// </summary>
    /// <param name="memWriter">owning MemWriter</param>
    /// <param name="buffer">reference to first byte of buffer to write with length bytes</param>
    /// <param name="length">size of data to write</param>
    void WriteBlock(ref MemWriter memWriter, ref byte buffer, nuint length);

    /// <summary>
    /// Seek to position in MemWriter.
    /// </summary>
    /// <param name="memWriter">owning MemWriter</param>
    /// <param name="position">new position to set</param>
    void SetCurrentPosition(ref MemWriter memWriter, long position);
}
