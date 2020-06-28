namespace BTDB.StreamLayer
{
    public interface ISpanReader
    {
        /// <summary>
        /// Initialize initial data. spanReader._buf and spanReader._initialData are empty spans before calling this function.
        /// </summary>
        /// <param name="spanReader">owning SpanReader</param>
        void Init(ref SpanReader spanReader)
        {
            FillBufAndCheckForEof(ref spanReader, 0);
        }
        /// <summary>
        /// Read at least 1 byte of data or return true if there is not enough data.
        /// </summary>
        /// <param name="spanReader">owning SpanReader</param>
        /// <returns>true if there is not enough data in source</returns>
        bool FillBufAndCheckForEof(ref SpanReader spanReader)
        {
            return FillBufAndCheckForEof(ref spanReader, 1);
        }
        /// <summary>
        /// Read at least size bytes of data or return true if there is not enough data.
        /// </summary>
        /// <param name="spanReader">owning SpanReader</param>
        /// <param name="size">how much bytes are needed to be in spanReader._buf</param>
        /// <returns>true if there is not enough data in source</returns>
        bool FillBufAndCheckForEof(ref SpanReader spanReader, int size);
        /// <summary>
        /// Calculate current position with help of spanReader._buf.
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
        bool ReadBlock(ref SpanReader spanReader, ref byte buffer, int length);
        /// <summary>
        /// Skip data of length. Called only when spanReader.Buf is empty.
        /// </summary>
        /// <param name="spanReader">owning SpanReader</param>
        /// <param name="length">how much to skip</param>
        /// <returns>true if not enough data to fill buffer</returns>
        bool SkipBlock(ref SpanReader spanReader, int length);
        /// <summary>
        /// Seek to position in SpanReader.
        /// </summary>
        /// <param name="spanReader">owning SpanReader</param>
        /// <param name="position">new position to set</param>
        void SetCurrentPosition(ref SpanReader spanReader, long position);
    }
}
