using System;

namespace BTDB
{
    public enum FindKeyStrategy
    {
        Create,
        ExactMatch,
        PreferPrevious,
        PreferNext,
        OnlyPrevious,
        OnlyNext
    }

    public enum FindKeyResult
    {
        NotFound,
        FoundExact,
        FoundPrevious,
        FoundNext,
        Created
    }

    public enum EraseStrategy
    {
        JustErase,
        MovePrevious,
        MoveNext,
    }

    public interface ILowLevelDBTransaction : IDisposable
    {
        /// <summary>
        /// Move actual key pointer to previus key from current Position
        /// </summary>
        /// <returns>true if there was such previous key, else Position will not move</returns>
        bool FindPreviousKey();
        /// <summary>
        /// Move actual key pointer to next key from current Position
        /// </summary>
        /// <returns>true if there was such previous key, else Position will not move</returns>
        bool FindNextKey();
        /// <summary>
        /// Main function for seeking to keys or even creating
        /// </summary>
        /// <param name="keyBuf">Key Data in this buffer</param>
        /// <param name="keyOfs">Key Data starts on this offset in buffer</param>
        /// <param name="keyLen">Key has this Length</param>
        /// <param name="strategy">What should happen</param>
        /// <returns>What really happend</returns>
        FindKeyResult FindKey(byte[] keyBuf, int keyOfs, int keyLen, FindKeyStrategy strategy);
        /// <summary>
        /// Find out current key Length.
        /// </summary>
        /// <returns>-1 if current key does not exist</returns>
        int GetKeySize();
        /// <summary>
        /// Find out current value Length.
        /// </summary>
        /// <returns>-1 if current key does not exist</returns>
        long GetValueSize();
        long CountRange(byte[] key1Buf, int key1Ofs, int key1Len, bool key1Open, byte[] key2Buf, int key2Ofs, int key2Len, bool key2Open);
        long CountPrefix(byte[] prefix, int prefixOfs, int prefixLen);
        /// <summary>
        /// Sligtly lowlevel function to read Data of keys without need to allocate your own buffer.
        /// </summary>
        /// <param name="ofs">Byte offset into value Data where you want to start reading</param>
        /// <param name="len">How many bytes available to read</param>
        /// <param name="buf">Read Data from this byte array</param>
        /// <param name="bufOfs">From this Position of buf</param>
        void PeekKey(int ofs, out int len, out byte[] buf, out int bufOfs);
        /// <summary>
        /// Read key content into provided byte array. Throws exception if not enough bytes available.
        /// </summary>
        /// <param name="ofs">Byte offset into key Data where you want to start reading</param>
        /// <param name="len">How many bytes to read</param>
        /// <param name="buf">Into which byte array</param>
        /// <param name="bufOfs">Start filling of buf from this Position</param>
        void ReadKey(int ofs, int len, byte[] buf, int bufOfs);
        /// <summary>
        /// Sligtly lowlevel function to read Data of values without need to allocate your own buffer.
        /// </summary>
        /// <param name="ofs">Byte offset into value Data where you want to start reading</param>
        /// <param name="len">How many bytes available to read</param>
        /// <param name="buf">Read Data from this byte array</param>
        /// <param name="bufOfs">From this Position of buf</param>
        void PeekValue(long ofs, out int len, out byte[] buf, out int bufOfs);
        /// <summary>
        /// Read value content into provided byte array. Throws exception if not enough bytes available.
        /// </summary>
        /// <param name="ofs">Byte offset into value Data where you want to start reading</param>
        /// <param name="len">How many bytes to read</param>
        /// <param name="buf">Into which byte array</param>
        /// <param name="bufOfs">Start filling of buf from this Position</param>
        void ReadValue(long ofs, int len, byte[] buf, int bufOfs);
        /// <summary>
        /// Write value content from provided byte array. Automaticaly expanding Length of value. Filling new empty space with zeros if needed.
        /// </summary>
        /// <param name="ofs">Byte offset into value Data where you want to start writing</param>
        /// <param name="len">How many bytes to write</param>
        /// <param name="buf">From which byte array</param>
        /// <param name="bufOfs">Start reading of buf from this Position</param>
        void WriteValue(long ofs, int len, byte[] buf, int bufOfs);
        /// <summary>
        /// Set Length of current content. Filling new empty space with zeroes is needed.
        /// </summary>
        /// <param name="newSize">New value size</param>
        void SetValueSize(long newSize);
        /// <summary>
        /// Remove current key and value. Current key will be invalidated.
        /// </summary>
        void EraseCurrent();
        void EraseRange(byte[] key1Buf, int key1Ofs, int key1Len, bool key1Open, byte[] key2Buf, int key2Ofs, int key2Len, bool key2Open);
        void ErasePrefix(byte[] prefix, int prefixOfs, int prefixLen);
        /// <summary>
        /// You should call this as last method in using scope if you don't want to rollback transaction.
        /// </summary>
        void Commit();
    }

    public interface ILowLevelDB: IDisposable
    {
        bool Open(IStream stream, bool dispose);
        ILowLevelDBTransaction StartTransaction();
    }
}
