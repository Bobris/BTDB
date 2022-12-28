using System;
using System.Collections.Generic;
using BTDB.Collections;

namespace BTDB.KVDBLayer;

public interface IKeyValueDBTransaction : IDisposable
{
    IKeyValueDB Owner { get; }

    DateTime CreatedTime { get; }

    /// <summary>
    /// Set for with some description for this transaction purpose to find reason for Transaction leak
    /// </summary>
    string? DescriptionForLeaks { get; set; }

    /// <summary>
    /// Move actual key pointer to first key matching provided prefix.
    /// </summary>
    /// <returns>true if there is such key, false if there are no such keys</returns>
    bool FindFirstKey(in ReadOnlySpan<byte> prefix);

    /// <summary>
    /// Move actual key pointer to last key matching provided prefix.
    /// </summary>
    /// <returns>true if there is such key, false if there are no such keys</returns>
    bool FindLastKey(in ReadOnlySpan<byte> prefix);

    /// <summary>
    /// Move actual key pointer to previous key from current Position only if still matches provided prefix
    /// </summary>
    /// <returns>true if there was such previous key, else Position will not move</returns>
    bool FindPreviousKey(in ReadOnlySpan<byte> prefix);

    /// <summary>
    /// Move actual key pointer to next key from current Position only if still matches provided prefix
    /// </summary>
    /// <returns>true if there was such next key, else Position will not move</returns>
    bool FindNextKey(in ReadOnlySpan<byte> prefix);

    /// <summary>
    /// Try to find key exactly, then try previous, then try next, then return NotFound. It has to match at least prefixLen
    /// </summary>
    FindResult Find(in ReadOnlySpan<byte> key, uint prefixLen);

    /// <summary>
    /// All in one function for creating and updating key value pair. If Key does not exists it is created and value is always replaced.
    /// </summary>
    /// <returns>true for Create, false for Update</returns>
    bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value);

    /// <summary>
    /// In current prefix will calculate number of key value pairs
    /// </summary>
    /// <returns>count</returns>
    long GetKeyValueCount();

    /// <summary>
    /// Gets index of current key
    /// </summary>
    /// <returns>-1 if current Key is invalid</returns>
    long GetKeyIndex();

    /// <summary>
    /// Sets index of current key relative to prefix
    /// </summary>
    /// <returns>true if such index exists</returns>
    bool SetKeyIndex(in ReadOnlySpan<byte> prefix, long index);

    /// <summary>
    /// Sets index of current key
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
    ReadOnlySpan<byte> GetKey();

    /// <summary>
    /// Return current key. It is always new Array.
    /// </summary>
    byte[] GetKeyToArray();

    /// <summary>
    /// Return current key. Can use prepared buffer.
    /// </summary>
    ReadOnlySpan<byte> GetKey(ref byte buffer, int bufferLength);

    /// <summary>
    /// Return current value into fresh memory or provided buffer if it fits.
    /// </summary>
    ReadOnlySpan<byte> GetClonedValue(ref byte buffer, int bufferLength);

    /// <summary>
    /// Return current value.
    /// </summary>
    ReadOnlySpan<byte> GetValue();

    /// <summary>
    /// Return current value.
    /// </summary>
    ReadOnlyMemory<byte> GetValueAsMemory();

    /// <summary>
    /// Returns true if GetValue would throw exception because missing or incomplete value file or transaction file containing current value.
    /// </summary>
    bool IsValueCorrupted();
    /// <summary>
    /// Overwrite current value with new content.
    /// </summary>
    void SetValue(in ReadOnlySpan<byte> value);

    /// <summary>
    /// Remove current key and value. Current key will be invalidated.
    /// It is same as calling EraseRange(GetKeyIndex(),GetKeyIndex()).
    /// </summary>
    void EraseCurrent();

    /// <summary>
    /// Remove key and value by exact key match. Current key will be invalidated.
    /// </summary>
    /// <returns>true if found and erased</returns>
    bool EraseCurrent(in ReadOnlySpan<byte> exactKey);

    /// <summary>
    /// Remove key and value by exact key match. Current key will be invalidated.
    /// Before erase read value into prepared buffer, if it is not big enough new memory will be allocated,
    /// but for sure it is safe to read it even after any DB modification.
    /// </summary>
    /// <returns>true if found and erased</returns>
    bool EraseCurrent(in ReadOnlySpan<byte> exactKey, ref byte buffer, int bufferLength, out ReadOnlySpan<byte> value);

    /// <summary>
    /// Remove all keys in DB.
    /// It is same as calling EraseRange(0,long.MaxValue).
    /// </summary>
    void EraseAll();

    /// <summary>
    /// This will remove keys in range of key indexes. Nothing will be removed if lastKeyIndex is less than firstKeyIndex.
    /// </summary>
    /// <param name="firstKeyIndex">absolute zero based index where to Start erase (inclusive)</param>
    /// <param name="lastKeyIndex">absolute zero based index where to finish erase (inclusive)</param>
    void EraseRange(long firstKeyIndex, long lastKeyIndex);

    /// <summary>
    /// Check writing status of this transaction.
    /// </summary>
    /// <returns>true when this transaction is writing one</returns>
    bool IsWriting();

    bool IsReadOnly();

    bool IsDisposed();

    /// <summary>
    /// Each KeyValueDB has special ulong value which could be modified - it is much faster than regular key
    /// </summary>
    /// <returns>its value</returns>
    ulong GetCommitUlong();

    /// <summary>
    /// Each KeyValueDB has special ulong value which could be modified - it is much faster than regular key
    /// </summary>
    void SetCommitUlong(ulong value);

    /// <summary>
    /// Each KeyValueDB has additional special ulong values which could be modified - it is much faster than regular key
    /// </summary>
    /// <returns>number of used ulongs</returns>
    uint GetUlongCount();

    /// <summary>
    /// Each KeyValueDB has additional special ulong values which could be modified - it is much faster than regular key
    /// </summary>
    /// <param name="idx">Index of ulong. These ulongs are lazily allocated</param>
    /// <returns>its value</returns>
    ulong GetUlong(uint idx);

    /// <summary>
    /// Each KeyValueDB has additional special ulong values which could be modified - it is much faster than regular key
    /// Optimized for small grow per transaction. Use mostly small indexes, unused idx waste memory.
    /// </summary>
    void SetUlong(uint idx, ulong value);

    /// <summary>
    /// This creates safe checkpoint for next open in transaction log
    /// </summary>
    void NextCommitTemporaryCloseTransactionLog();

    /// <summary>
    /// You should call this as last method in using scope if you don't want to rollback transaction. After this method only Dispose() is allowed.
    /// This will preserve previous value of CommitUlong
    /// </summary>
    void Commit();

    /// <summary>
    /// Global unique increasing number of actually running transaction
    /// </summary>
    long GetTransactionNumber();

    /// <summary>
    /// Returns current value of internal transaction counter which is incremented everytime current key position is moved.
    /// </summary>
    long CursorMovedCounter { get; }

    /// <summary>
    /// Useful for finding what takes most storage in your DB
    /// </summary>
    /// <returns>Size of key and value (possibly even compressed size)</returns>
    KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey();

    /// <summary>
    /// This is just storage for boolean, add could store here that it does not want to commit transaction, it is up to infrastructure code around if it will listen this advice.
    /// </summary>
    bool RollbackAdvised { get; set; }

    Dictionary<(uint Depth, uint Children), uint> CalcBTreeStats();
}
