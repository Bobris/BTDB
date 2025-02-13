using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer.BTreeMem;

namespace BTDB.KVDBLayer;

public class InMemoryKeyValueDB : IKeyValueDB
{
    IBTreeRootNode _lastCommited;
    InMemoryKeyValueDBTransaction? _writingTransaction;

    readonly Queue<TaskCompletionSource<IKeyValueDBTransaction>> _writeWaitingQueue =
        new Queue<TaskCompletionSource<IKeyValueDBTransaction>>();

    readonly object _writeLock = new object();
    bool _disposed = false;

    public InMemoryKeyValueDB()
    {
        _lastCommited = new BTreeRoot(0);
    }

    public void Dispose()
    {
        lock (_writeLock)
        {
            if (_writingTransaction != null)
                throw new BTDBException("Cannot dispose KeyValueDB when writing transaction still running");
            while (_writeWaitingQueue.Count > 0)
            {
                _writeWaitingQueue.Dequeue().TrySetCanceled();
            }
        }

        if (_transactions.Count > 0)
            throw new BTDBException("Cannot dispose KeyValueDB when transactions still running");

        _disposed = true;
    }

    public bool DurableTransactions { get; set; }

    internal IBTreeRootNode LastCommited => _lastCommited;

    public IKeyValueDBTransaction StartTransaction()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tr = new InMemoryKeyValueDBTransaction(this, LastCommited, false, false);
        _transactions.TryAdd(tr, false);
        return tr;
    }

    public IKeyValueDBTransaction StartReadOnlyTransaction()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tr = new InMemoryKeyValueDBTransaction(this, LastCommited, false, true);
        _transactions.TryAdd(tr, false);
        return tr;
    }

    public ValueTask<IKeyValueDBTransaction> StartWritingTransaction()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_writeLock)
        {
            if (_writingTransaction == null)
            {
                var tr = NewWritingTransactionUnsafe();
                _transactions.TryAdd(tr, false);
                return new(tr);
            }

            var tcs = new TaskCompletionSource<IKeyValueDBTransaction>();
            _writeWaitingQueue.Enqueue(tcs);

            return new(tcs.Task);
        }
    }

    public string CalcStats()
    {
        return "KeyValueCount:" + LastCommited.CalcKeyCount() + Environment.NewLine;
    }

    public (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetNativeMemoryStats()
    {
        return (0, 0, 0, 0);
    }

    public ValueTask<bool> Compact(CancellationToken cancellation)
    {
        return ValueTask.FromResult(false);
    }

    public void CreateKvi(CancellationToken cancellation)
    {
    }

    public IKeyValueDBLogger Logger { get; set; }

    public uint CompactorRamLimitInMb { get; set; }
    public long MaxTrLogFileSize { get; set; }

    public IEnumerable<IKeyValueDBTransaction> Transactions()
    {
        foreach (var keyValuePair in _transactions.Keys)
        {
            yield return keyValuePair;
        }
    }

    public ulong CompactorReadBytesPerSecondLimit { get; set; }
    public ulong CompactorWriteBytesPerSecondLimit { get; set; }

    readonly ConcurrentDictionary<IKeyValueDBTransaction, bool> _transactions = new();

    public ulong? PreserveHistoryUpToCommitUlong
    {
        get { return null; }
        set
        {
            /* ignore */
        }
    }

    internal IBTreeRootNode MakeWritableTransaction(InMemoryKeyValueDBTransaction keyValueDBTransaction,
        IBTreeRootNode btreeRoot)
    {
        lock (_writeLock)
        {
            if (_writingTransaction != null)
                throw new BTDBTransactionRetryException("Another writing transaction already running");
            if (LastCommited != btreeRoot)
                throw new BTDBTransactionRetryException("Another writing transaction already finished");
            _writingTransaction = keyValueDBTransaction;
            return btreeRoot.NewTransactionRoot();
        }
    }

    internal void CommitWritingTransaction(IBTreeRootNode btreeRoot)
    {
        lock (_writeLock)
        {
            _writingTransaction = null;
            _lastCommited = btreeRoot;
            TryDequeWaiterForWritingTransaction();
        }
    }

    void TryDequeWaiterForWritingTransaction()
    {
        if (_writeWaitingQueue.Count == 0) return;
        var tcs = _writeWaitingQueue.Dequeue();
        var tr = NewWritingTransactionUnsafe();
        _transactions.TryAdd(tr, false);
        tcs.SetResult(tr);
    }

    InMemoryKeyValueDBTransaction NewWritingTransactionUnsafe()
    {
        var newTransactionRoot = LastCommited.NewTransactionRoot();
        var tr = new InMemoryKeyValueDBTransaction(this, newTransactionRoot, true, false);
        _writingTransaction = tr;
        return tr;
    }

    internal void RevertWritingTransaction()
    {
        lock (_writeLock)
        {
            _writingTransaction = null;
            TryDequeWaiterForWritingTransaction();
        }
    }

    public void TransactionDisposed(IKeyValueDBTransaction transaction)
    {
        _transactions.TryRemove(transaction, out _);
    }
}
