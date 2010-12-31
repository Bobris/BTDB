using System;
using System.Collections.Concurrent;
using System.Threading;

namespace BTDB
{
    public static class ExtensionMethods
    {
        public static bool CreateKey(this ILowLevelDBTransaction transaction, byte[] keyBuf)
        {
            return transaction.FindKey(keyBuf, 0, keyBuf.Length, FindKeyStrategy.Create)==FindKeyResult.Created;
        }

        public static bool FindExactKey(this ILowLevelDBTransaction transaction, byte[] keyBuf)
        {
            return transaction.FindKey(keyBuf, 0, keyBuf.Length, FindKeyStrategy.ExactMatch)==FindKeyResult.FoundExact;
        }

        public static bool CreateOrUpdateKeyValue(this ILowLevelDBTransaction transaction, byte[] keyBuf, byte[] valueBuf)
        {
            return transaction.CreateOrUpdateKeyValue(keyBuf, 0, keyBuf.Length, valueBuf, 0, valueBuf.Length);
        }

        public static void SetValue(this ILowLevelDBTransaction transaction, byte[] valueBuf)
        {
            transaction.SetValue(valueBuf, 0, valueBuf.Length);
        }

        public static byte[] ReadKey(this ILowLevelDBTransaction transaction)
        {
            int keySize = transaction.GetKeySize();
            if (keySize < 0) return null;
            var result = new byte[keySize];
            transaction.ReadKey(0, keySize, result, 0);
            return result;
        }

        public static byte[] ReadValue(this ILowLevelDBTransaction transaction)
        {
            long valueSize = transaction.GetValueSize();
            if (valueSize < 0) return null;
            if ((int)valueSize!=valueSize) throw new BTDBException("Value is bigger then 2GB does not fit in byte[]");
            var result = new byte[valueSize];
            transaction.ReadValue(0, (int)valueSize, result, 0);
            return result;
        }

        public static bool TryRemove<TKey,TValue>(this ConcurrentDictionary<TKey,TValue> dict, TKey key)
        {
            TValue val;
            return dict.TryRemove(key, out val);
        }

        public static Lazy<T> Force<T>(this Lazy<T> lazy)
        {
#pragma warning disable 168
            var ignored = lazy.Value;
#pragma warning restore 168
            return lazy;
        }

        public static ReadLockHelper ReadLock(this ReaderWriterLockSlim readerWriterLock)
        {
            return new ReadLockHelper(readerWriterLock);
        }

        public static UpgradeableReadLockHelper UpgradableReadLock(this ReaderWriterLockSlim readerWriterLock)
        {
            return new UpgradeableReadLockHelper(readerWriterLock);
        }

        public static WriteLockHelper WriteLock(this ReaderWriterLockSlim readerWriterLock)
        {
            return new WriteLockHelper(readerWriterLock);
        }

        public struct ReadLockHelper : IDisposable
        {
            readonly ReaderWriterLockSlim _readerWriterLock;

            public ReadLockHelper(ReaderWriterLockSlim readerWriterLock)
            {
                readerWriterLock.EnterReadLock();
                _readerWriterLock = readerWriterLock;
            }

            public void Dispose()
            {
                _readerWriterLock.ExitReadLock();
            }
        }

        public struct UpgradeableReadLockHelper : IDisposable
        {
            readonly ReaderWriterLockSlim _readerWriterLock;

            public UpgradeableReadLockHelper(ReaderWriterLockSlim readerWriterLock)
            {
                _readerWriterLock = readerWriterLock;
                readerWriterLock.EnterUpgradeableReadLock();
            }

            public void Dispose()
            {
                _readerWriterLock.ExitUpgradeableReadLock();
            }
        }

        public struct WriteLockHelper : IDisposable
        {
            readonly ReaderWriterLockSlim _readerWriterLock;

            public WriteLockHelper(ReaderWriterLockSlim readerWriterLock)
            {
                _readerWriterLock = readerWriterLock;
                readerWriterLock.EnterWriteLock();
            }

            public void Dispose()
            {
                _readerWriterLock.ExitWriteLock();
            }
        }

    }
}
