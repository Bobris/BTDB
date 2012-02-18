using System.Collections.Generic;
using System.Threading.Tasks;
using BTDB.KV2DBLayer.BTree;
using BTDB.KVDBLayer;

namespace BTDB.KV2DBLayer
{
    public class KeyValue2DB : IKeyValue2DB
    {
        readonly IFileCollection _fileCollection;
        IBTreeRootNode _lastCommited;
        KeyValue2DBTransaction _writingTransaction;
        readonly Queue<TaskCompletionSource<IKeyValue2DBTransaction>> _writeWaitingQueue = new Queue<TaskCompletionSource<IKeyValue2DBTransaction>>();
        readonly object _writeLock = new object();

        public KeyValue2DB(IFileCollection fileCollection)
        {
            _fileCollection = fileCollection;
            _lastCommited = new BTreeRoot(0);
        }

        public void Dispose()
        {
            lock (_writeLock)
            {
                if (_writingTransaction != null) throw new BTDBException("Cannot dispose KeyValue2DB when writting transaction still running");
                while (_writeWaitingQueue.Count > 0)
                {
                    _writeWaitingQueue.Dequeue().TrySetCanceled();
                }
            }
        }

        public IKeyValue2DBTransaction StartTransaction()
        {
            return new KeyValue2DBTransaction(this, _lastCommited, false);
        }

        public Task<IKeyValue2DBTransaction> StartWritingTransaction()
        {
            lock (_writeLock)
            {
                var tcs = new TaskCompletionSource<IKeyValue2DBTransaction>();
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

        internal IBTreeRootNode MakeWrittableTransaction(KeyValue2DBTransaction keyValue2DBTransaction, IBTreeRootNode btreeRoot)
        {
            lock (_writeLock)
            {
                if (_writingTransaction != null) throw new BTDBTransactionRetryException("Another writting transaction already running");
                if (_lastCommited != btreeRoot) throw new BTDBTransactionRetryException("Another writting transaction already finished");
                _writingTransaction = keyValue2DBTransaction;
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
            if (_writeWaitingQueue.Count > 0)
            {
                var tcs = _writeWaitingQueue.Dequeue();
                NewWrittingTransactionUnsafe(tcs);
            }
        }

        void NewWrittingTransactionUnsafe(TaskCompletionSource<IKeyValue2DBTransaction> tcs)
        {
            _writingTransaction = new KeyValue2DBTransaction(this, _lastCommited.NewTransactionRoot(), true);
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
