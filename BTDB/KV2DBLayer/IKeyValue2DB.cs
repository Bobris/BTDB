using System;
using System.Threading.Tasks;

namespace BTDB.KV2DBLayer
{
    public interface IKeyValue2DB : IDisposable
    {
        IKeyValue2DBTransaction StartTransaction();
        Task<IKeyValue2DBTransaction> StartWritingTransaction();
    }

    public interface IKeyValue2DBTransaction
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

        bool FindExactOrPrevious(byte[] key);

        /// <summary>
        /// All in one function for creating and updating key value pair. If Key does not exists it is created and value is always replaced. It automaticaly preppend current prefix to key.
        /// </summary>
        /// <returns>true for Create, false for Update</returns>
        bool CreateOrUpdateKeyValue(byte[] key, byte[] value);

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

        byte[] GetKey();
        byte[] GetValue();
        void SetValue(byte[] value);

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
    }
}
