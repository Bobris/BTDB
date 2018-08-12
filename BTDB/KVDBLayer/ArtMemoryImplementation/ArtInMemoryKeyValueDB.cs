using System;
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
        readonly Queue<TaskCompletionSource<IKeyValueDBTransaction>> _writeWaitingQueue = new Queue<TaskCompletionSource<IKeyValueDBTransaction>>();
        readonly object _writeLock = new object();
        
        public ArtInMemoryKeyValueDB(IOffHeapAllocator allocator)
        {
            _lastCommited = ARTImpl.CreateEmptyRoot(allocator, false);
        }

        public void Dispose()
        {
            lock (_writeLock)
            {
                if (_writingTransaction != null) throw new BTDBException("Cannot dispose KeyValueDB when writting transaction still running");
                while (_writeWaitingQueue.Count > 0)
                {
                    _writeWaitingQueue.Dequeue().TrySetCanceled();
                }
            }
        }

        public bool DurableTransactions { get; set; }

        internal IRootNode LastCommited => _lastCommited;

        public IKeyValueDBTransaction StartTransaction()
        {
            return new ArtInMemoryKeyValueDBTransaction(this, LastCommited, false, false);
        }

        public IKeyValueDBTransaction StartReadOnlyTransaction()
        {
            return new ArtInMemoryKeyValueDBTransaction(this, LastCommited, false, true);
        }

        public Task<IKeyValueDBTransaction> StartWritingTransaction()
        {
            lock (_writeLock)
            {
                var tcs = new TaskCompletionSource<IKeyValueDBTransaction>();
                if (_writingTransaction == null)
                {
                    NewWrittingTransactionUnsafe(tcs);
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
            return "KeyValueCount:" + LastCommited.GetCount() + Environment.NewLine;
        }

        public bool Compact(CancellationToken cancellation)
        {
            return false;
        }

        public IKeyValueDBLogger Logger { get; set; }

        public ulong? PreserveHistoryUpToCommitUlong {
            get { return null; }
            set { /* ignore */ }
        }

        internal IRootNode MakeWrittableTransaction(ArtInMemoryKeyValueDBTransaction keyValueDBTransaction, IRootNode artRoot)
        {
            lock (_writeLock)
            {
                if (_writingTransaction != null) throw new BTDBTransactionRetryException("Another writting transaction already running");
                if (LastCommited != artRoot) throw new BTDBTransactionRetryException("Another writting transaction already finished");
                _writingTransaction = keyValueDBTransaction;
                var result = _lastCommited;
                _lastCommited = result.Snapshot();
                return result;
            }
        }

        internal void CommitWrittingTransaction(IRootNode artRoot)
        {
            lock (_writeLock)
            {
                _writingTransaction = null;
                _lastCommited = artRoot;
                TryDequeWaiterForWrittingTransaction();
            }
        }

        void TryDequeWaiterForWrittingTransaction()
        {
            if (_writeWaitingQueue.Count == 0) return;
            var tcs = _writeWaitingQueue.Dequeue();
            NewWrittingTransactionUnsafe(tcs);
        }

        void NewWrittingTransactionUnsafe(TaskCompletionSource<IKeyValueDBTransaction> tcs)
        {
            var newTransactionRoot = LastCommited;
            _lastCommited = newTransactionRoot.Snapshot();
            _writingTransaction = new ArtInMemoryKeyValueDBTransaction(this, newTransactionRoot, true, false);
            tcs.TrySetResult(_writingTransaction);
        }

        internal void RevertWrittingTransaction()
        {
            lock (_writeLock)
            {
                _writingTransaction = null;
                TryDequeWaiterForWrittingTransaction();
            }
        }
    }
}