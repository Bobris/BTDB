using System;
using System.Collections.Generic;
using BTDB.Collections;
using BTDB.StreamLayer;

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
    /// Creates new cursor after using it Dispose must be called.
    /// </summary>
    /// <returns>Fresh cursor bound to this transaction</returns>
    IKeyValueDBCursor CreateCursor();

    /// <summary>
    /// Calculate number of key value pairs
    /// </summary>
    /// <returns>count</returns>
    long GetKeyValueCount();

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
    /// This is just storage for boolean, add could store here that it does not want to commit transaction, it is up to infrastructure code around if it will listen this advice.
    /// </summary>
    bool RollbackAdvised { get; set; }

    Dictionary<(uint Depth, uint Children), uint> CalcBTreeStats();

    IKeyValueDBCursor? FirstCursor { get; set; }
    IKeyValueDBCursor? LastCursor { get; set; }
}
