using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDB.AzureStorage.Test;

public class AzureBlobFileCollectionTests
{
    [Fact]
    public async Task CreateAsyncDownloadsExistingBlobsIntoLocalCache()
    {
        var backend = new InMemoryBlobStorageBackend();
        backend.SeedBlockBlob("00000001.kvi", [1, 2, 3]);
        backend.SeedBlockBlob("00000002.trl", [4, 5, 6]);

        await using var collection = await AzureBlobFileCollection.CreateAsync(new()
        {
            BlobStorageBackend = backend,
            TransactionLogFlushPeriod = TimeSpan.FromMinutes(5)
        });

        Assert.Equal<uint>(2, collection.GetCount());
        Assert.True(File.Exists(Path.Combine(collection.LocalCacheDirectory, "00000001.kvi")));
        Assert.True(File.Exists(Path.Combine(collection.LocalCacheDirectory, "00000002.trl")));

        var buffer = new byte[3];
        collection.GetFile(1)!.RandomRead(buffer, 0, false);
        Assert.Equal(new byte[] { 1, 2, 3 }, buffer);

        collection.GetFile(2)!.RandomRead(buffer, 0, false);
        Assert.Equal(new byte[] { 4, 5, 6 }, buffer);
    }

    [Fact]
    public async Task LoggerObservesInitialDownloads()
    {
        var backend = new InMemoryBlobStorageBackend();
        var logger = new TestAzureBlobFileCollectionLogger();
        backend.SeedBlockBlob("00000001.kvi", [1, 2, 3]);

        await using var collection = await AzureBlobFileCollection.CreateAsync(new()
        {
            BlobStorageBackend = backend,
            Logger = logger,
            TransactionLogFlushPeriod = TimeSpan.FromMinutes(5)
        });

        Assert.Contains(logger.Events, e => e.Kind == "initial-download" &&
                                           e.Operation == "00000001.kvi" &&
                                           e.QueueLength >= 0 &&
                                           e.FileLength == 3);
    }

    [Fact]
    public async Task UploadsRegularFilesAsBlockBlobsAndDeletesThem()
    {
        var backend = new InMemoryBlobStorageBackend();

        await using var collection = await AzureBlobFileCollection.CreateAsync(new()
        {
            BlobStorageBackend = backend,
            TransactionLogFlushPeriod = TimeSpan.FromMinutes(5)
        });

        var file = collection.AddFile("kvi");
        var writer = new MemWriter(file.GetExclusiveAppenderWriter());
        writer.WriteUInt8(11);
        writer.WriteUInt8(22);
        writer.WriteUInt8(33);
        writer.Flush();
        file.HardFlushTruncateSwitchToDisposedMode();

        await collection.FlushPendingChangesAsync();

        Assert.Equal(new byte[] { 11, 22, 33 }, backend.GetBlobContent("00000001.kvi"));
    }

    [Fact]
    public async Task CreateAsyncUploadsLocallyLongerFilesInsteadOfRedownloadingThem()
    {
        var backend = new InMemoryBlobStorageBackend();
        backend.SeedBlockBlob("00000001.trl", [1]);

        var localCacheDirectory = Path.Combine(Path.GetTempPath(), "BTDB.AzureStorage.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(localCacheDirectory);
        await File.WriteAllBytesAsync(Path.Combine(localCacheDirectory, "00000001.trl"), [1, 2, 3]);

        try
        {
            await using var collection = await AzureBlobFileCollection.CreateAsync(new()
            {
                BlobStorageBackend = backend,
                LocalCacheDirectory = localCacheDirectory,
                TransactionLogFlushPeriod = TimeSpan.FromMinutes(5)
            });

            await collection.FlushPendingChangesAsync();

            Assert.Equal(new byte[] { 1, 2, 3 }, await File.ReadAllBytesAsync(Path.Combine(localCacheDirectory, "00000001.trl")));
            Assert.Equal(new byte[] { 1, 2, 3 }, backend.GetBlobContent("00000001.trl"));
        }
        finally
        {
            Directory.Delete(localCacheDirectory, true);
        }
    }

    [Fact]
    public async Task DeleteIsQueuedBehindPendingUpload()
    {
        var backend = new InMemoryBlobStorageBackend();

        await using var collection = await AzureBlobFileCollection.CreateAsync(new()
        {
            BlobStorageBackend = backend,
            TransactionLogFlushPeriod = TimeSpan.FromMinutes(5)
        });

        var file = collection.AddFile("kvi");
        var writer = new MemWriter(file.GetExclusiveAppenderWriter());
        for (var i = 0; i < 256 * 1024; i++)
        {
            writer.WriteUInt8((byte)(i % 251));
        }

        writer.Flush();
        file.HardFlushTruncateSwitchToDisposedMode();
        file.Remove();

        await collection.FlushPendingChangesAsync();

        Assert.False(backend.Exists("00000001.kvi"));
        Assert.False(File.Exists(Path.Combine(collection.LocalCacheDirectory, "00000001.kvi")));
    }

    [Fact]
    public async Task HardFlushOnRegularFileThrows()
    {
        var backend = new InMemoryBlobStorageBackend();

        await using var collection = await AzureBlobFileCollection.CreateAsync(new()
        {
            BlobStorageBackend = backend,
            TransactionLogFlushPeriod = TimeSpan.FromMinutes(5)
        });

        var file = collection.AddFile("kvi");
        var writer = new MemWriter(file.GetExclusiveAppenderWriter());
        writer.WriteUInt8(11);
        writer.Flush();

        var exception = Assert.Throws<InvalidOperationException>(() => file.HardFlush());
        Assert.Equal("HardFlush is supported only for transaction log files in AzureBlobFileCollection.", exception.Message);
    }

    [Fact]
    public async Task UnsupportedFileTypesThrow()
    {
        var backend = new InMemoryBlobStorageBackend();

        await using var collection = await AzureBlobFileCollection.CreateAsync(new()
        {
            BlobStorageBackend = backend,
            TransactionLogFlushPeriod = TimeSpan.FromMinutes(5)
        });

        var exception = Assert.Throws<NotSupportedException>(() => collection.AddFile("hpv"));
        Assert.Equal(
            "AzureBlobFileCollection supports only BTDB KVDB file types 'trl', 'pvl', and 'kvi'. Unsupported file type: 'hpv'.",
            exception.Message);
    }

    [Fact]
    public async Task ConcurrentTemporaryTruncateFlushesAtLeastCurrentDurableTransactionLogLength()
    {
        var backend = new InMemoryBlobStorageBackend();

        await using var collection = await AzureBlobFileCollection.CreateAsync(new()
        {
            BlobStorageBackend = backend,
            TransactionLogFlushPeriod = TimeSpan.FromMinutes(5)
        });

        var file = collection.AddFile("trl");
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteUInt8(1);
        writer.WriteUInt8(2);
        writer.WriteUInt8(3);
        writer.Flush();
        file.HardFlush();

        collection.ConcurrentTemporaryTruncate(file.Index, 1);

        await WaitForBlobLengthAsync(backend, "00000001.trl", 3);

        Assert.Equal(new byte[] { 1, 2, 3 }, backend.GetBlobContent("00000001.trl"));
    }

    [Fact]
    public async Task AppenderWriterGetCurrentPositionStaysAbsoluteAfterFlush()
    {
        var backend = new InMemoryBlobStorageBackend();

        await using var collection = await AzureBlobFileCollection.CreateAsync(new()
        {
            BlobStorageBackend = backend,
            TransactionLogFlushPeriod = TimeSpan.FromMinutes(5)
        });

        var file = collection.AddFile("trl");
        var writer = new MemWriter(file.GetAppenderWriter());
        Assert.Equal(0, writer.GetCurrentPosition());

        writer.WriteUInt8(1);
        writer.WriteUInt8(2);
        writer.WriteUInt8(3);
        Assert.Equal(3, writer.GetCurrentPosition());

        writer.Flush();
        Assert.Equal(3, writer.GetCurrentPosition());

        writer.WriteUInt8(4);
        Assert.Equal(4, writer.GetCurrentPosition());
    }

    [Fact]
    public async Task HardFlushQueuesImmediateTransactionLogUpload()
    {
        var backend = new InMemoryBlobStorageBackend();

        await using var collection = await AzureBlobFileCollection.CreateAsync(new()
        {
            BlobStorageBackend = backend,
            TransactionLogFlushPeriod = TimeSpan.FromMinutes(5)
        });

        var file = collection.AddFile("trl");
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteUInt8(1);
        writer.Flush();
        file.HardFlush();

        await WaitForBlobLengthAsync(backend, "00000001.trl", 1);

        writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteUInt8(2);
        writer.WriteUInt8(3);
        writer.Flush();
        file.HardFlush();

        await WaitForBlobLengthAsync(backend, "00000001.trl", 3);

        Assert.Equal(new byte[] { 1, 2, 3 }, backend.GetBlobContent("00000001.trl"));
    }

    [Fact]
    public async Task LoggerObservesQueuedAndExecutingOperationsWithQueueLength()
    {
        var backend = new InMemoryBlobStorageBackend();
        var logger = new TestAzureBlobFileCollectionLogger();

        await using var collection = await AzureBlobFileCollection.CreateAsync(new()
        {
            BlobStorageBackend = backend,
            Logger = logger,
            TransactionLogFlushPeriod = TimeSpan.FromMinutes(5)
        });

        var file = collection.AddFile("trl");
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteUInt8(1);
        writer.Flush();
        file.HardFlush();

        await collection.FlushPendingChangesAsync();

        Assert.Contains(logger.Events, e => e.Kind == "queued" &&
                                           e.Operation == "SynchronizeTransactionLog 00000001.trl" &&
                                           e.QueueLength >= 1);
        Assert.Contains(logger.Events, e => e.Kind == "executing" &&
                                           e.Operation == "SynchronizeTransactionLog 00000001.trl" &&
                                           e.QueueLength >= 0 &&
                                           e.FileLength == 1);
    }

    [Fact]
    public async Task BTreeKeyValueDbCanReopenFromAzureBlobBackedFileCollection()
    {
        var backend = new InMemoryBlobStorageBackend();
        var key = new byte[] { 10, 20, 30 };
        var value = new byte[] { 40, 50, 60, 70 };

        await using (var collection = await AzureBlobFileCollection.CreateAsync(new()
               {
                   BlobStorageBackend = backend,
                   TransactionLogFlushPeriod = TimeSpan.FromMilliseconds(50)
               }))
        {
            using var db = new BTreeKeyValueDB(collection);
            db.DurableTransactions = true;
            using var tr = db.StartTransaction();
            using var cursor = tr.CreateCursor();
            Assert.True(cursor.CreateOrUpdateKeyValue(key, value));
            tr.Commit();
            await collection.FlushPendingChangesAsync();
        }

        await using (var reopenedCollection = await AzureBlobFileCollection.CreateAsync(new()
               {
                   BlobStorageBackend = backend,
                   TransactionLogFlushPeriod = TimeSpan.FromMilliseconds(50)
               }))
        {
            using var reopenedDb = new BTreeKeyValueDB(reopenedCollection);
            using var tr = reopenedDb.StartReadOnlyTransaction();
            using var cursor = tr.CreateCursor();
            Assert.Equal(FindResult.Exact, cursor.Find(key, 0));
            Assert.Equal(value, cursor.SlowGetValue());
        }

    }

    [Fact]
    public async Task TemporaryLocalCacheDirectoryIsPreservedByDefault()
    {
        var backend = new InMemoryBlobStorageBackend();
        string localCacheDirectory;

        await using (var collection = await AzureBlobFileCollection.CreateAsync(new()
               {
                   BlobStorageBackend = backend,
                   TransactionLogFlushPeriod = TimeSpan.FromMinutes(5)
               }))
        {
            localCacheDirectory = collection.LocalCacheDirectory;
            Assert.True(Directory.Exists(localCacheDirectory));
        }

        try
        {
            Assert.True(Directory.Exists(localCacheDirectory));
        }
        finally
        {
            Directory.Delete(localCacheDirectory, true);
        }
    }

    [Fact]
    public async Task TemporaryLocalCacheDirectoryCanBeDeletedOnDisposeWhenEnabled()
    {
        var backend = new InMemoryBlobStorageBackend();
        string localCacheDirectory;

        await using (var collection = await AzureBlobFileCollection.CreateAsync(new()
               {
                   BlobStorageBackend = backend,
                   DeleteLocalCacheDirectoryOnDispose = true,
                   TransactionLogFlushPeriod = TimeSpan.FromMinutes(5)
               }))
        {
            localCacheDirectory = collection.LocalCacheDirectory;
            Assert.True(Directory.Exists(localCacheDirectory));
        }

        Assert.False(Directory.Exists(localCacheDirectory));
    }

    static async Task WaitForBlobLengthAsync(InMemoryBlobStorageBackend backend, string blobName, long expectedLength)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (backend.Exists(blobName))
            {
                if (backend.GetBlobContent(blobName).Length == expectedLength)
                    return;
            }

            await Task.Delay(50);
        }

        throw new TimeoutException($"Blob '{blobName}' did not reach length {expectedLength}.");
    }

    sealed class TestAzureBlobFileCollectionLogger : IAzureBlobFileCollectionLogger
    {
        readonly object _lock = new();
        readonly List<LogEvent> _events = [];

        public IReadOnlyList<LogEvent> Events
        {
            get
            {
                lock (_lock)
                {
                    return _events.ToArray();
                }
            }
        }

        public void InitialDownloadExecuting(string blobName, int remainingDownloads, long fileLength)
        {
            Add("initial-download", blobName, remainingDownloads, fileLength);
        }

        public void OperationQueued(string operation, int queueLength)
        {
            Add("queued", operation, queueLength, null);
        }

        public void OperationExecuting(string operation, int queueLength, long? fileLength = null)
        {
            Add("executing", operation, queueLength, fileLength);
        }

        public void OperationFailed(string operation, int queueLength, Exception exception)
        {
            Add("failed", operation, queueLength, null);
        }

        void Add(string kind, string operation, int queueLength, long? fileLength = null)
        {
            lock (_lock)
            {
                _events.Add(new LogEvent(kind, operation, queueLength, fileLength));
            }
        }
    }

    readonly record struct LogEvent(string Kind, string Operation, int QueueLength, long? FileLength);
}
