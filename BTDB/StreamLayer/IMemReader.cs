using System;
using BTDB.IL;
using BTDB.KVDBLayer;

namespace BTDB.StreamLayer;

public interface IMemReader
{
    /// <summary>
    /// Fill Buf at creation of MemReader.
    /// </summary>
    /// <param name="memReader">owning MemReader</param>
    void Init(ref MemReader memReader);

    /// <summary>
    /// Fill Buf with advisePrefetchLength bytes of data. Postcondition is that memReader.Current+advisePrefetchLength&lt;=memReader.End.
    /// </summary>
    /// <param name="memReader">owning MemReader</param>
    void FillBuf(ref MemReader memReader, nuint advisePrefetchLength);

    /// <summary>
    /// Calculate current position with help of memReader.Buf.
    /// </summary>
    /// <param name="memReader">owning MemReader</param>
    /// <returns>byte offset from start</returns>
    long GetCurrentPosition(in MemReader memReader);

    /// <summary>
    /// Reads data into buffer. Called only when memReader.Current==memReader.End.
    /// </summary>
    /// <param name="memReader">owning MemReader</param>
    /// <param name="buffer">reference to first byte of buffer to fill with length bytes</param>
    /// <param name="length">size of data to read</param>
    /// <returns>true if not enough data to fill buffer</returns>
    bool ReadBlock(ref MemReader memReader, ref byte buffer, nuint length);

    /// <summary>
    /// Skip data of length. Called only when memReader.Current==memReader.End.
    /// </summary>
    /// <param name="memReader">owning MemReader</param>
    /// <param name="length">how much to skip</param>
    /// <returns>true if not enough data to fill buffer</returns>
    bool SkipBlock(ref MemReader memReader, nuint length);

    /// <summary>
    /// Seek to position in MemReader.
    /// </summary>
    /// <param name="memReader">owning MemReader</param>
    /// <param name="position">new position to set</param>
    void SetCurrentPosition(ref MemReader memReader, long position);

    /// <summary>
    /// Try to return original continuous block of memory from storage
    /// </summary>
    /// <param name="memReader">owning MemReader</param>
    /// <param name="length">how many bytes to read</param>
    /// <param name="result">resulting ReadOnlyMemory</param>
    /// <returns>true if possible</returns>
    bool TryReadBlockAsMemory(ref MemReader memReader, uint length, out ReadOnlyMemory<byte> result)
    {
        result = new();
        return false;
    }

    /// <summary>
    /// Test for end of file.
    /// </summary>
    /// <param name="memReader">owning MemReader</param>
    /// <returns>true is end of file</returns>
    bool Eof(ref MemReader memReader);

    /// <summary>
    /// Override this by method returning true if your MemReader spans over whole file.
    /// </summary>
    bool ThrowIfNotSimpleReader()
    {
        throw new BTDBException("Used MemReader method which cannot work with this Controller " +
                                GetType().ToSimpleName());
    }
}
