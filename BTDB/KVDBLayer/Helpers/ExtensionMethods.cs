using System;
using System.Collections.Concurrent;
using System.Threading;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public static class ExtensionMethods
{
    public static byte[] SlowGetKey(this IKeyValueDBCursor cursor)
    {
        var buf = new byte[cursor.GetStorageSizeOfCurrentKey().Key];
        var memoryBuf = buf.AsMemory();
        cursor.GetKeyMemory(ref memoryBuf, true);
        return buf;
    }

    public static void GetKeyIntoMemWriter(this IKeyValueDBCursor cursor, ref MemWriter writer)
    {
        writer.Reset();
        var span = cursor.GetKeySpan(
            writer.BlockWriteToSpan((int)cursor.GetStorageSizeOfCurrentKey().Key, out var needToBeWritten), true);
        if (needToBeWritten)
        {
            writer.WriteBlock(span);
        }
    }

    public static byte[] SlowGetValue(this IKeyValueDBCursor cursor)
    {
        var buf = new byte[cursor.GetStorageSizeOfCurrentKey().Value];
        var memoryBuf = buf.AsMemory();
        cursor.GetValueMemory(ref memoryBuf, true);
        return buf;
    }

    public static bool CreateKey(this IKeyValueDBCursor cursor, in ReadOnlySpan<byte> keyBuf)
    {
        if (FindExactKey(cursor, keyBuf)) return false;
        return cursor.CreateOrUpdateKeyValue(keyBuf, new());
    }

    public static bool FindExactKey(this IKeyValueDBCursor cursor, scoped in ReadOnlySpan<byte> key)
    {
        return cursor.Find(key, 0) == FindResult.Exact;
    }

    public static bool FindExactOrNextKey(this IKeyValueDBCursor cursor, in ReadOnlySpan<byte> key)
    {
        var r = cursor.Find(key, 0);
        switch (r)
        {
            case FindResult.Exact:
                return true;
            case FindResult.Previous:
                cursor.FindNextKey(new());
                break;
        }

        return false;
    }

    public static long GetKeyValueCount(this IKeyValueDBCursor cursor, in ReadOnlySpan<byte> prefix)
    {
        if (!cursor.FindFirstKey(prefix)) return 0;
        var startIndex = cursor.GetKeyIndex();
        cursor.FindLastKey(prefix);
        var endIndex = cursor.GetKeyIndex();
        return endIndex - startIndex + 1;
    }

    public static long GetKeyIndex(this IKeyValueDBCursor cursor, in ReadOnlySpan<byte> prefix)
    {
        using var cursorForPrefix = cursor.Transaction.CreateCursor();
        if (!cursorForPrefix.FindFirstKey(prefix)) return -1;
        return cursor.GetKeyIndex() - cursorForPrefix.GetKeyIndex();
    }

    public static long EraseAll(this IKeyValueDBCursor cursor, in ReadOnlySpan<byte> prefix)
    {
        if (!cursor.FindFirstKey(prefix)) return 0;
        using var cursorForEnd = cursor.Transaction.CreateCursor();
        cursorForEnd.FindLastKey(prefix);
        return cursor.EraseUpTo(cursorForEnd);
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
