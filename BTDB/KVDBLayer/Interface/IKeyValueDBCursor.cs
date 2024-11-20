using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace BTDB.KVDBLayer;

public interface IKeyValueDBCursorInternal : IKeyValueDBCursor
{
    void NotifyRemove(ulong startIndex, ulong endIndex);
    void PreNotifyUpsert();
    void NotifyInsert(ulong index);
    IKeyValueDBCursorInternal? PrevCursor { get; set; }
    IKeyValueDBCursorInternal? NextCursor { get; set; }
    void NotifyWritableTransaction();
}

public interface IKeyValueDBCursor : IDisposable
{
    IKeyValueDBTransaction Transaction { get; }

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
    FindResult Find(scoped in ReadOnlySpan<byte> key, uint prefixLen);

    /// <summary>
    /// Gets index of current key
    /// </summary>
    /// <returns>-1 if current Key is invalid</returns>
    long GetKeyIndex();

    /// <summary>
    /// Sets index of current key relative to prefix
    /// </summary>
    /// <returns>true if such index exists</returns>
    bool FindKeyIndex(in ReadOnlySpan<byte> prefix, long index);

    /// <summary>
    /// Sets index of current key
    /// </summary>
    /// <returns>true if such index exists</returns>
    bool FindKeyIndex(long index);

    /// <summary>
    /// Actual key pointer will be set to invalid state.
    /// </summary>
    void Invalidate();

    /// <summary>
    /// returns true if current key is valid
    /// </summary>
    bool IsValid();

    /// <summary>
    /// Useful for finding what takes most storage in your DB
    /// </summary>
    /// <returns>Size of key and value (possibly even compressed size)</returns>
    KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey();

    /// <summary>
    /// Get key of current position. It can use provided buffer if it fits or allocate new memory and update buffer.
    /// </summary>
    /// <param name="buffer">It will try to use provided buffer if not big enough it will allocate new memory and update buffer for next time</param>
    /// <param name="copy">It forbids to return to mutable memory in database</param>
    /// <returns>memory of key</returns>
    ReadOnlyMemory<byte> GetKeyMemory(ref Memory<byte> buffer, bool copy = false);

    ReadOnlySpan<byte> GetKeySpan(scoped ref Span<byte> buffer, bool copy = false);

    ReadOnlySpan<byte> GetKeySpan(Span<byte> buffer, bool copy = false);

    /// <summary>
    /// Returns true if GetValue would throw exception because missing or incomplete value file or transaction file containing current value.
    /// </summary>
    bool IsValueCorrupted();

    /// <summary>
    /// Get value of current position. It can use provided buffer if it fits or allocate new memory and update buffer.
    /// </summary>
    /// <param name="buffer">It will try to use provided buffer if not big enough it will allocate new memory and update buffer for next time</param>
    /// <param name="copy">It forbids to return to mutable memory in database</param>
    /// <returns>memory of value</returns>
    ReadOnlyMemory<byte> GetValueMemory(ref Memory<byte> buffer, bool copy = false);

    /// <summary>
    /// Get value of current position. It can use provided buffer if it fits or allocate new memory and update buffer.
    /// </summary>
    /// <param name="buffer">It will try to use provided buffer if not big enough it will allocate new memory and update buffer for next time</param>
    /// <param name="copy">It forbids to return to mutable memory in database</param>
    /// <returns>span of value</returns>
    ReadOnlySpan<byte> GetValueSpan(scoped ref Span<byte> buffer, bool copy = false);

    /// <summary>
    /// Overwrite current value with new content.
    /// </summary>
    void SetValue(in ReadOnlySpan<byte> value);

    /// <summary>
    /// Remove current key and value. Current key will be fuzzy invalidated, you cannot get it, but you can use FindNextKey or FindPreviousKey to logically move to siblings.
    /// </summary>
    void EraseCurrent();

    /// <summary>
    /// Remove all keys between current key and provided cursor. Both cursors will be fuzzy invalidated, you cannot get it, but you can use FindNextKey or FindPreviousKey to logically move to siblings.
    /// If provided cursor is before current key, nothing will be removed.
    /// </summary>
    /// <param name="to">Valid cursor which points to last key which should be removed. It must be from same transaction.</param>
    /// <returns>Number of rows removed</returns>
    long EraseUpTo(IKeyValueDBCursor to);

    /// <summary>
    /// All in one function for creating and updating key value pair. If Key does not exist, it is created and value is always replaced.
    /// This cursor will be positioned on the key.
    /// </summary>
    /// <returns>true for Create, false for Update</returns>
    bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value);

    /// <summary>
    /// Updates key with preserving value.
    /// </summary>
    /// <returns>
    /// If multiple keys of prefixLen of key exists it will return NotUniquePrefix.
    /// If prefix is not even found it will return NotFound.
    /// If Key is already equal then it will return NothingToDo.
    /// Else it will update key and return Updated.
    /// </returns>
    UpdateKeySuffixResult UpdateKeySuffix(in ReadOnlySpan<byte> key, uint prefixLen);
}
