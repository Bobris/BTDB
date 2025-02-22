﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using BTDB.KVDBLayer;

namespace DBBenchmark;

public class BtdbTimeTests : IDbTimeTests
{
    readonly bool _inMemory;
    readonly bool _memoryMapped;
    IFileCollection? _fileCollection;
    readonly bool _fastInMemory;

    public BtdbTimeTests()
    {
        _fastInMemory = true;

        CreateTestFileCollection();
    }

    public BtdbTimeTests(bool inMemory, bool memoryMapped)
    {
        _inMemory = inMemory;
        _memoryMapped = memoryMapped;

        _fileCollection = CreateTestFileCollection();
    }

    public void Dispose()
    {
        _fileCollection?.Dispose();
    }

    public (TimeSpan openTime, long memorySize) Open()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var memStart = GC.GetTotalMemory(true);

        var stopwatch = Stopwatch.StartNew();

        using (CreateKeyValueDb(_fileCollection))
        {
            stopwatch.Stop();

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var memFinish = GC.GetTotalMemory(false);

            return (openTime: stopwatch.Elapsed, memorySize: (memFinish - memStart) / 1024);
        }
    }

    public TimeSpan Insert(byte[] key, byte[] value)
    {
        using (var db = CreateKeyValueDb(_fileCollection, new NoCompressionStrategy()))
        {
            var stopwatch = Stopwatch.StartNew();

            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(key, value);
                tr.Commit();
            }

            stopwatch.Stop();
            return stopwatch.Elapsed;
        }
    }

    public TimeSpan InsertRange(Dictionary<byte[], byte[]> data)
    {
        var stopwatch = Stopwatch.StartNew();

        using (var db = CreateKeyValueDb(_fileCollection, new NoCompressionStrategy()))
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            foreach (var keyValue in data)
            {
                cursor.CreateOrUpdateKeyValue(keyValue.Key, keyValue.Value);
            }

            tr.Commit();
        }

        stopwatch.Stop();
        return stopwatch.Elapsed;
    }

    public TimeSpan InsertRangeCommitByItem(Dictionary<byte[], byte[]> data)
    {
        var stopwatch = Stopwatch.StartNew();

        using (var db = CreateKeyValueDb(_fileCollection, new NoCompressionStrategy()))
        {
            foreach (var keyValue in data)
            {
                using (var tr = db.StartTransaction())
                {
                    using var cursor = tr.CreateCursor();
                    cursor.CreateOrUpdateKeyValue(keyValue.Key, keyValue.Value);
                    tr.Commit();
                }
            }
        }

        stopwatch.Stop();
        return stopwatch.Elapsed;
    }

    public TimeSpan Read(byte[] key)
    {
        using var db = CreateKeyValueDb(_fileCollection, new NoCompressionStrategy());
        var stopwatch = Stopwatch.StartNew();
        using (var tr = db.StartTransaction())
        using (var cursor = tr.CreateCursor())
        {
            cursor.FindExactKey(key);
            var buf = new Span<byte>();
            cursor.GetValueSpan(ref buf);
        }

        stopwatch.Stop();
        return stopwatch.Elapsed;
    }

    public TimeSpan ReadValues(IEnumerable<byte[]> keys)
    {
        var stopwatch = Stopwatch.StartNew();

        using (var db = CreateKeyValueDb(_fileCollection, new NoCompressionStrategy()))
        using (var tr = db.StartTransaction())
        using (var cursor = tr.CreateCursor())
        {
            foreach (var key in keys)
            {
                cursor.FindExactKey(key);
                var buf = new Span<byte>();
                cursor.GetValueSpan(ref buf);
            }
        }

        stopwatch.Stop();
        return stopwatch.Elapsed;
    }

    public TimeSpan ReadAll(Dictionary<byte[], byte[]> exceptedData)
    {
        using var db = CreateKeyValueDb(_fileCollection, new NoCompressionStrategy());
        var stopwatch = Stopwatch.StartNew();
        using (var tr = db.StartTransaction())
        using (var cursor = tr.CreateCursor())
        {
            var buf = new Span<byte>();
            foreach (var data in exceptedData)
            {
                var key = cursor.FindExactKey(data.Key);
                if (!key) throw new Exception("Key not found");

                var value = cursor.GetValueSpan(ref buf);

                if (!value.SequenceEqual(data.Value)) throw new Exception("value different");
            }
        }

        stopwatch.Stop();
        return stopwatch.Elapsed;
    }

    public TimeSpan Delete(byte[] key)
    {
        var stopwatch = Stopwatch.StartNew();

        using (var db = CreateKeyValueDb(_fileCollection, new NoCompressionStrategy()))
        using (var tr = db.StartTransaction())
        using (var cursor = tr.CreateCursor())
        {
            cursor.FindExactKey(key);
            cursor.EraseCurrent();
            tr.Commit();
        }

        stopwatch.Stop();
        return stopwatch.Elapsed;
    }

    public TimeSpan DeleteAll()
    {
        var stopwatch = Stopwatch.StartNew();

        using (var db = CreateKeyValueDb(_fileCollection, new NoCompressionStrategy()))
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.EraseAll([]);
            tr.Commit();
        }

        stopwatch.Stop();
        return stopwatch.Elapsed;
    }

    IFileCollection? CreateTestFileCollection()
    {
        if (_fastInMemory)
            return null;
        if (_inMemory)
        {
            _fileCollection = new InMemoryFileCollection();
            return _fileCollection;
        }

        const string dbfilename = "data";
        if (Directory.Exists(dbfilename))
            Directory.Delete(dbfilename, true);
        Directory.CreateDirectory(dbfilename);
        if (_memoryMapped)
            return new OnDiskMemoryMappedFileCollection(dbfilename);
        return new OnDiskFileCollection(dbfilename);
    }

    public Dictionary<byte[], byte[]> GetDbData(string dbfilename)
    {
        var fileCollection = OpenTestFileCollection(dbfilename);

        using var db = CreateKeyValueDb(fileCollection, new SnappyCompressionStrategy());
        using var tr = db.StartTransaction();
        var data = new Dictionary<byte[], byte[]>();

        using var cursor = tr.CreateCursor();
        if (!cursor.FindFirstKey([]))
            return data;

        do
        {
            var key = cursor.SlowGetKey();
            var value = cursor.SlowGetValue();

            data.Add(key, value);
        } while (cursor.FindNextKey([]));

        return data;
    }

    IFileCollection? OpenTestFileCollection(string dbfilename)
    {
        if (_fastInMemory)
            return null;
        if (_inMemory)
            return _fileCollection;
        if (_memoryMapped)
            return new OnDiskMemoryMappedFileCollection(dbfilename);
        return new OnDiskFileCollection(dbfilename);
    }

    static IKeyValueDB CreateKeyValueDb(IFileCollection? fileCollection,
        ICompressionStrategy? compressionStrategy = null)
    {
        if (fileCollection == null)
            return new InMemoryKeyValueDB();

        if (compressionStrategy == null)
            return new BTreeKeyValueDB(fileCollection);

        return new BTreeKeyValueDB(fileCollection, compressionStrategy);
    }
}
