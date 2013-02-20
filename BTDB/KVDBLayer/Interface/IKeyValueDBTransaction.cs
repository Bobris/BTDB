using System;
using BTDB.Buffer;

namespace BTDB.KVDBLayer
{
    public interface IKeyValueDBTransaction : IDisposable
    {
        /// <summary>
        /// It sets automatic key prefix, all funtions then works relatively to this prefix, it also invalidates current key
        /// </summary>
        void SetKeyPrefix(ByteBuffer prefix);

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
        /// Try to find key exactly, then try previous, then try next, then return NotFound
        /// </summary>
        FindResult Find(ByteBuffer key);

        /// <summary>
        /// All in one function for creating and updating key value pair. If Key does not exists it is created and value is always replaced. It automaticaly preppend current prefix to key.
        /// </summary>
        /// <returns>true for Create, false for Update</returns>
        bool CreateOrUpdateKeyValue(ByteBuffer key, ByteBuffer value);

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
        /// Actual key pointer will be set to invalid state.
        /// </summary>
        void InvalidateCurrentKey();

        /// <summary>
        /// returns true if current key is valid
        /// </summary>
        bool IsValidKey();

        /// <summary>
        /// Return current key.
        /// </summary>
        ByteBuffer GetKey();

        /// <summary>
        /// Return current value.
        /// </summary>
        ByteBuffer GetValue();

        /// <summary>
        /// Overwrite current value with new content.
        /// </summary>
        void SetValue(ByteBuffer value);

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
        /// Check writting status of this transaction.
        /// </summary>
        /// <returns>true when this transaction is writting one</returns>
        bool IsWritting();

        /// <summary>
        /// You should call this as last method in using scope if you don't want to rollback transaction. After this method only Dispose() is allowed.
        /// </summary>
        void Commit();

        /// <summary>
        /// Global unique increasing number of actually running transaction
        /// </summary>
        long GetTransactionNumber();
    }
}
