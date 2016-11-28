using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer.BTreeMem;

namespace BTDB.KVDBLayer
{
    public class InMemoryKeyValueDB : IKeyValueDB
    {
        IBTreeRootNode _lastCommited;
        InMemoryKeyValueDBTransaction _writingTransaction;
        readonly Queue<TaskCompletionSource<IKeyValueDBTransaction>> _writeWaitingQueue = new Queue<TaskCompletionSource<IKeyValueDBTransaction>>();
        readonly object _writeLock = new object();
        
        public InMemoryKeyValueDB()
        {
            _lastCommited = new BTreeRoot(0);
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

        internal IBTreeRootNode LastCommited => _lastCommited;

        public IKeyValueDBTransaction StartTransaction()
        {
            return new InMemoryKeyValueDBTransaction(this, LastCommited, false, false);
        }

        public IKeyValueDBTransaction StartReadOnlyTransaction()
        {
            return new InMemoryKeyValueDBTransaction(this, LastCommited, false, true);
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
            return "KeyValueCount:" + LastCommited.CalcKeyCount() + Environment.NewLine;
        }

        public bool Compact(CancellationToken cancellation)
        {
            return false;
        }

        public IKeyValueDBLogger Logger { get; set; }

        internal IBTreeRootNode MakeWrittableTransaction(InMemoryKeyValueDBTransaction keyValueDBTransaction, IBTreeRootNode btreeRoot)
        {
            lock (_writeLock)
            {
                if (_writingTransaction != null) throw new BTDBTransactionRetryException("Another writting transaction already running");
                if (LastCommited != btreeRoot) throw new BTDBTransactionRetryException("Another writting transaction already finished");
                _writingTransaction = keyValueDBTransaction;
                return btreeRoot.NewTransactionRoot();
            }
        }

        internal void CommitWrittingTransaction(IBTreeRootNode btreeRoot)
        {
            lock (_writeLock)
            {
                _writingTransaction = null;
                _lastCommited = btreeRoot;
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
            var newTransactionRoot = LastCommited.NewTransactionRoot();
            _writingTransaction = new InMemoryKeyValueDBTransaction(this, newTransactionRoot, true, false);
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