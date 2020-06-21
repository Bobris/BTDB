using System;
using System.Collections.Concurrent;
using System.Threading;
using BTDB.Buffer;

namespace BTDB.KVDBLayer
{
    public static class ExtensionMethods
    {
        public static bool CreateKey(this IKeyValueDBTransaction transaction, byte[] keyBuf)
        {
            if (FindExactKey(transaction, keyBuf)) return false;
            return transaction.CreateOrUpdateKeyValue(ByteBuffer.NewSync(keyBuf), ByteBuffer.NewEmpty());
        }

        public static bool FindExactKey(this IKeyValueDBTransaction transaction, byte[] keyBuf)
        {
            return transaction.Find(ByteBuffer.NewSync(keyBuf)) == FindResult.Exact;
        }

        public static bool CreateOrUpdateKeyValueUnsafe(this IKeyValueDBTransaction transaction, byte[] keyBuf, byte[] valueBuf)
        {
            return transaction.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(keyBuf), ByteBuffer.NewAsync(valueBuf));
        }

        public static bool CreateOrUpdateKeyValue(this IKeyValueDBTransaction transaction, byte[] keyBuf, byte[] valueBuf)
        {
            return transaction.CreateOrUpdateKeyValue(ByteBuffer.NewSync(keyBuf), ByteBuffer.NewSync(valueBuf));
        }

        public static void SetValue(this IKeyValueDBTransaction transaction, byte[] valueBuf)
        {
            transaction.SetValue(ByteBuffer.NewSync(valueBuf));
        }

        public static byte[] GetKeyAsByteArray(this IKeyValueDBTransaction transaction)
        {
            return transaction.GetKey().ToByteArray();
        }

        public static byte[] GetValueAsByteArray(this IKeyValueDBTransaction transaction)
        {
            return transaction.GetValue().ToByteArray();
        }

        public static void SetKeyPrefixUnsafe(this IKeyValueDBTransaction transaction, byte[]? prefix)
        {
            transaction.SetKeyPrefix(prefix == null ? ByteBuffer.NewEmpty() : ByteBuffer.NewAsync(prefix));
        }

        public static void SetKeyPrefix(this IKeyValueDBTransaction transaction, byte[]? prefix)
        {
            transaction.SetKeyPrefix(prefix == null ? ByteBuffer.NewEmpty() : ByteBuffer.NewSync(prefix));
        }

        public static bool TryRemove<TKey, TValue>(this ConcurrentDictionary<TKey, TValue> dict, TKey key)
        {
            return dict.TryRemove(key, out _);
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
