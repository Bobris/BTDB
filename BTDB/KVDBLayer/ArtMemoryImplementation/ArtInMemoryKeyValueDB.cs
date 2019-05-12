using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.ARTLib;

namespace BTDB.KVDBLayer
{
    public class ArtInMemoryKeyValueDB : IKeyValueDB
    {
        IRootNode _lastCommited;
        ArtInMemoryKeyValueDBTransaction _writingTransaction;

        readonly Queue<TaskCompletionSource<IKeyValueDBTransaction>> _writeWaitingQueue =
            new Queue<TaskCompletionSource<IKeyValueDBTransaction>>();

        readonly object _writeLock = new object();
        readonly ConcurrentBag<IRootNode> _waitingToDispose = new ConcurrentBag<IRootNode>();

        public ArtInMemoryKeyValueDB(IOffHeapAllocator allocator)
        {
            _lastCommited = ARTImpl12.CreateEmptyRoot(allocator, false);
            _lastCommited.Commit();
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

                DereferenceRoot(_lastCommited);
                FreeWaitingToDispose();
            }
        }

        public bool DurableTransactions { get; set; }

        public IKeyValueDBTransaction StartTransaction()
        {
            while (true)
            {
                var node = _lastCommited;
                // Memory barrier inside next statement
                if (!node.Reference())
                    return new ArtInMemoryKeyValueDBTransaction(this, node, false, false);
            }
        }

        public IKeyValueDBTransaction StartReadOnlyTransaction()
        {
            while (true)
            {
                var node = _lastCommited;
                // Memory barrier inside next statement
                if (!node.Reference())
                    return new ArtInMemoryKeyValueDBTransaction(this, node, false, true);
            }
        }

        public Task<IKeyValueDBTransaction> StartWritingTransaction()
        {
            lock (_writeLock)
            {
                var tcs = new TaskCompletionSource<IKeyValueDBTransaction>();
                if (_writingTransaction == null)
                {
                    NewWritingTransactionUnsafe(tcs);
                }
                else
                {
                    _writeWaitingQueue.Enqueue(tcs);
                }

                return tcs.Task;
            }
        }

        public string CalcStats()
        {
            return "KeyValueCount:" + _lastCommited.GetCount() + Environment.NewLine;
        }

        public bool Compact(CancellationToken cancellation)
        {
            return false;
        }

        public IKeyValueDBLogger Logger { get; set; }

        public uint CompactorRamLimitInMb { get; set; }

        public ulong? PreserveHistoryUpToCommitUlong
        {
            get { return null; }
            set
            {
                /* ignore */
            }
        }

        internal IRootNode MakeWritableTransaction(ArtInMemoryKeyValueDBTransaction keyValueDBTransaction,
            IRootNode artRoot)
        {
            lock (_writeLock)
            {
                if (_writingTransaction != null)
                    throw new BTDBTransactionRetryException("Another writing transaction already running");
                if (_lastCommited != artRoot)
                    throw new BTDBTransactionRetryException("Another writing transaction already finished");
                _writingTransaction = keyValueDBTransaction;
                var result = _lastCommited.CreateWritableTransaction();
                DereferenceRoot(artRoot);
                return result;
            }
        }

        internal void CommitWritingTransaction(IRootNode artRoot)
        {
            lock (_writeLock)
            {
                _writingTransaction = null;
                if (_lastCommited.Dereference())
                {
                    _lastCommited.Dispose();
                }

                _lastCommited = artRoot;
                _lastCommited.Commit();
                TryDequeWaiterForWritingTransaction();
            }
        }

        void TryDequeWaiterForWritingTransaction()
        {
            FreeWaitingToDispose();
            if (_writeWaitingQueue.Count == 0) return;
            var tcs = _writeWaitingQueue.Dequeue();
            NewWritingTransactionUnsafe(tcs);
        }

        void NewWritingTransactionUnsafe(TaskCompletionSource<IKeyValueDBTransaction> tcs)
        {
            FreeWaitingToDispose();
            var newTransactionRoot = _lastCommited.CreateWritableTransaction();
            try
            {
                _writingTransaction = new ArtInMemoryKeyValueDBTransaction(this, newTransactionRoot, true, false);
            }
            catch
            {
                newTransactionRoot.Dispose();
                throw;
            }

            tcs.TrySetResult(_writingTransaction);
        }

        void FreeWaitingToDispose()
        {
            while (_waitingToDispose.TryTake(out var node))
            {
                node.Dispose();
            }
        }

        internal void RevertWritingTransaction(IRootNode currentArtRoot)
        {
            lock (_writeLock)
            {
                currentArtRoot.Dispose();
                _writingTransaction = null;
                TryDequeWaiterForWritingTransaction();
            }
        }

        internal void DereferenceRoot(IRootNode currentArtRoot)
        {
            if (currentArtRoot.Dereference())
            {
                _waitingToDispose.Add(currentArtRoot);
            }
        }
    }
}
