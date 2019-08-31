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
                if (_writingTransaction != null) throw new BTDBException("Cannot dispose KeyValueDB when writing transaction still running");
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

        public ValueTask<IKeyValueDBTransaction> StartWritingTransaction()
        {
            lock (_writeLock)
            {

                if (_writingTransaction == null)
                {
                    var tr = NewWritingTransactionUnsafe();
                    return new ValueTask<IKeyValueDBTransaction>(tr);
                }

                var tcs = new TaskCompletionSource<IKeyValueDBTransaction>();
                _writeWaitingQueue.Enqueue(tcs);

                return new ValueTask<IKeyValueDBTransaction>(tcs.Task);
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

        public uint CompactorRamLimitInMb { get; set; }

        public ulong? PreserveHistoryUpToCommitUlong {
            get { return null; }
            set { /* ignore */ }
        }

        internal IBTreeRootNode MakeWritableTransaction(InMemoryKeyValueDBTransaction keyValueDBTransaction, IBTreeRootNode btreeRoot)
        {
            lock (_writeLock)
            {
                if (_writingTransaction != null) throw new BTDBTransactionRetryException("Another writing transaction already running");
                if (LastCommited != btreeRoot) throw new BTDBTransactionRetryException("Another writing transaction already finished");
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
            tcs.SetResult(NewWritingTransactionUnsafe());
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
    }
}
