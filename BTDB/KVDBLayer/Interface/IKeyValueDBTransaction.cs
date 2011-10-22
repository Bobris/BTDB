using System;

namespace BTDB.KVDBLayer
{
    public interface IKeyValueDBTransaction : IDisposable
    {
        /// <summary>
        /// Check writting status of this transaction.
        /// </summary>
        /// <returns>true when this transaction is writting one</returns>
        bool IsWritting();
        
        /// <summary>
        /// It sets automatic key prefix, all funtions then works relatively to this prefix, it also invalidates current key
        /// </summary>
        /// <param name="prefix">Prefix data</param>
        /// <param name="prefixOfs">Offset to data where actual prefix starts</param>
        /// <param name="prefixLen">Length of prefix</param>
        void SetKeyPrefix(byte[] prefix, int prefixOfs, int prefixLen);

        /// <summary>
        /// Actual key pointer will be set to invalid state. It is good for soonner releasing memory.
        /// </summary>
        void InvalidateCurrentKey();

        /// <summary>
        /// Move actual key pointer to first key in current prefix.
        /// </summary>
        /// <returns>true if there is such key, false if there are no keys in current prefix</returns>
        bool FindFirstKey();

        /// <summary>
        /// Move actual key pointer to last key in current prefix.
        /// </summary>
        /// <returns>true if there is such key, false if there are no keys in current prefix</returns>
        bool FindLastKey();

        /// <summary>
        /// Move actual key pointer to previus key from current Position
        /// </summary>
        /// <returns>true if there was such previous key in curent prefix, else Position will not move</returns>
        bool FindPreviousKey();

        /// <summary>
        /// Move actual key pointer to next key from current Position
        /// </summary>
        /// <returns>true if there was such previous key, else Position will not move</returns>
        bool FindNextKey();

        /// <summary>
        /// Main function for seeking to keys or even creating. It automaticaly preppend current prefix to key.
        /// </summary>
        /// <param name="keyBuf">Key Data in this buffer</param>
        /// <param name="keyOfs">Key Data starts on this offset in buffer</param>
        /// <param name="keyLen">Key has this length</param>
        /// <param name="strategy">What should happen</param>
        /// <returns>What really happend</returns>
        FindKeyResult FindKey(byte[] keyBuf, int keyOfs, int keyLen, FindKeyStrategy strategy);

        /// <summary>
        /// All in one function for creating and updating key value pair. If Key does not exists it is created and value is always replaced. It automaticaly preppend current prefix to key.
        /// </summary>
        /// <param name="keyBuf">Key Data in this buffer</param>
        /// <param name="keyOfs">Key Data starts on this offset in buffer</param>
        /// <param name="keyLen">Key has this length</param>
        /// <param name="valueBuf">Value Data in this buffer</param>
        /// <param name="valueOfs">Value Data starts on this offset in buffer</param>
        /// <param name="valueLen">Value has this length</param>
        /// <returns>true for Create, false for Update</returns>
        bool CreateOrUpdateKeyValue(byte[] keyBuf, int keyOfs, int keyLen, byte[] valueBuf, int valueOfs, int valueLen);

        /// <summary>
        /// In current prefix will calculate number of key value pairs
        /// </summary>
        /// <returns>count</returns>
        long GetKeyValueCount();

        /// <summary>
        /// Gets index of current key in current prefix
        /// </summary>
        /// <returns>-1 if current Key is invalid</returns>
        long GetKeyIndex();

        /// <summary>
        /// Sets index of current key in current prefix
        /// </summary>
        /// <returns>true if such index exists</returns>
        bool SetKeyIndex(long index);

        /// <summary>
        /// Find out current key Length minus length of current prefix
        /// </summary>
        /// <returns>-1 if current key does not exist</returns>
        int GetKeySize();

        /// <summary>
        /// Find out current value Length.
        /// </summary>
        /// <returns>-1 if current key does not exist</returns>
        long GetValueSize();

        /// <summary>
        /// Sligtly lowlevel function to read Data of keys without need to allocate your own buffer. It skips current key prefix.
        /// </summary>
        /// <param name="ofs">Byte offset into value Data where you want to start reading</param>
        /// <param name="len">How many bytes available to read</param>
        /// <param name="buf">Read Data from this byte array</param>
        /// <param name="bufOfs">From this Position of buf</param>
        void PeekKey(int ofs, out int len, out byte[] buf, out int bufOfs);

        /// <summary>
        /// Read key content into provided byte array. Throws exception if not enough bytes available. It skips current key prefix.
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
        /// Same as SetValueSize(len); WriteValue(0,len,buf,bufOfs);, but faster
        /// </summary>
        /// <param name="buf">Buffer with new value data</param>
        /// <param name="bufOfs">Offset there actual data starts in buf</param>
        /// <param name="len">New value size</param>
        void SetValue(byte[] buf, int bufOfs, int len);

        /// <summary>
        /// Remove current key and value. Current key will be invalidated.
        /// It is same as calling EraseRange(GetKeyIndex(),GetKeyIndex()).
        /// </summary>
        void EraseCurrent();

        /// <summary>
        /// Remove all keys in current prefix.
        /// It is same as calling EraseRange(0,long.MaxValue).
        /// </summary>
        void EraseAll();

        /// <summary>
        /// This will remove keys in range of key indexes. It will erase only keys in current prefix, even you specify indexes outside of range. Nothing will be removed if lastKeyIndex is less than firstKeyIndex.
        /// </summary>
        /// <param name="firstKeyIndex">zero based index relative to current prefix where to start erase (inclusive)</param>
        /// <param name="lastKeyIndex">zero based index relative to current prefix where to finish erase (inclusive)</param>
        void EraseRange(long firstKeyIndex, long lastKeyIndex);

        /// <summary>
        /// You should call this as last method in using scope if you don't want to rollback transaction. After this method only Dispose() is allowed.
        /// </summary>
        void Commit();

        /// <summary>
        /// Calculates statistics. It is global, not relative to current prefix.
        /// </summary>
        /// <returns>DTO with usefull statistic about current Transaction and KeyValueDB</returns>
        KeyValueDBStats CalculateStats();
    }
}
