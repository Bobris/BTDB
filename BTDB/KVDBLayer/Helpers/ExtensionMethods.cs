using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;

namespace BTDB.KVDBLayer;

public static class ExtensionMethods
{
    public static bool CreateKey(this IKeyValueDBTransaction transaction, in ReadOnlySpan<byte> keyBuf)
    {
        if (FindExactKey(transaction, keyBuf)) return false;
        return transaction.CreateOrUpdateKeyValue(keyBuf, new ReadOnlySpan<byte>());
    }

    public static bool FindExactKey(this IKeyValueDBTransaction transaction, in ReadOnlySpan<byte> key)
    {
        return transaction.Find(key, 0) == FindResult.Exact;
    }

    public static bool FindExactOrNextKey(this IKeyValueDBTransaction transaction, in ReadOnlySpan<byte> key)
    {
        var r = transaction.Find(key, 0);
        switch (r)
        {
            case FindResult.Exact:
                return true;
            case FindResult.Previous:
                transaction.FindNextKey(new());
                break;
        }

        return false;
    }

    public static long GetKeyValueCount(this IKeyValueDBTransaction transaction, in ReadOnlySpan<byte> prefix)
    {
        if (!transaction.FindFirstKey(prefix)) return 0;
        var startIndex = transaction.GetKeyIndex();
        transaction.FindLastKey(prefix);
        var endIndex = transaction.GetKeyIndex();
        return endIndex - startIndex + 1;
    }

    public static long GetKeyIndex(this IKeyValueDBTransaction transaction, in ReadOnlySpan<byte> prefix)
    {
        var currentIndex = transaction.GetKeyIndex();
        if (!transaction.FindFirstKey(prefix)) return -1;
        var relative = currentIndex - transaction.GetKeyIndex();
        transaction.SetKeyIndex(currentIndex);
        return relative;
    }

    public static long EraseAll(this IKeyValueDBTransaction transaction, in ReadOnlySpan<byte> prefix)
    {
        if (!transaction.FindFirstKey(prefix)) return 0;
        var startIndex = transaction.GetKeyIndex();
        transaction.FindLastKey(prefix);
        var endIndex = transaction.GetKeyIndex();
        transaction.EraseRange(startIndex, endIndex);
        return endIndex - startIndex + 1;
    }

    public static bool TryRemove<TKey, TValue>(this ConcurrentDictionary<TKey, TValue> dict, TKey key)
    {
        return dict.TryRemove(key, out _);
    }

    public static Lazy<T> Force<T>(this Lazy<T> lazy)
    {
        _ = lazy.Value;
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

    public readonly struct ReadLockHelper : IDisposable
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

    public readonly struct UpgradeableReadLockHelper : IDisposable
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

    public readonly struct WriteLockHelper : IDisposable
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
