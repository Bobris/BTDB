using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDB.Replication.Test;

public class CheckpointPublisherTest
{
    static BTreeKeyValueDB Open(IFileCollection files, bool compressed = false) => new(new KeyValueDBOptions
    {
        FileCollection = files, Compression = new NoCompressionStrategy(), FileSplitSize = 8096,
        KviCompressionStrategy = new DefaultCompressionKviStrategy(compressed ? 3 : -1, thresholdKeysToEnableCompression: 0),
        UseOddTransactionLogIds = true, CompactorScheduler = null
    });

    static async Task Populate(BTreeKeyValueDB db)
    {
        for (byte i = 0; i < 60; i++)
        {
            using var tr = await db.StartWritingTransaction((ulong)i + 1);
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([i], Enumerable.Repeat(i, 2000).ToArray());
            tr.Commit();
        }
        using (var tr = await db.StartWritingTransaction(61ul))
        {
            using var cursor = tr.CreateCursor();
            for (byte i = 0; i < 60; i += 2) cursor.CreateOrUpdateKeyValue([i], new byte[2000]);
            tr.SetUlong(2, 1234);
            tr.Commit();
        }
        Assert.True(await db.Compact(CancellationToken.None));
        using (var tr = await db.StartWritingTransaction(62ul))
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([200], new byte[2000]);
            cursor.CreateOrUpdateKeyValue([201], "inline"u8);
            cursor.CreateOrUpdateKeyValue([202], []);
            tr.Commit();
        }
    }

    // Models remote block writes without a local staging file, reads or seeks.
    sealed class UploadStream(IFileCollectionFile remote, Action onChunk) : IPositionLessStream
    {
        MemWriter _writer = new(remote.GetAppenderWriter());
        ulong _position;
        public int Read(Span<byte> data, ulong position) => throw new NotSupportedException();
        public void Write(ReadOnlySpan<byte> data, ulong position)
        {
            Assert.Equal(_position, position);
            while (!data.IsEmpty)
            {
                var length = Math.Min(257, data.Length);
                onChunk();
                _writer.WriteBlock(data[..length]);
                data = data[length..];
                _position += (uint)length;
            }
        }
        public void SetSize(ulong size) { Assert.Equal(0ul, size); Assert.Equal(0ul, _position); }
        public ulong GetSize() => _position;
        public void Flush() => _writer.Flush();
        public void HardFlush() => throw new NotSupportedException();
        public void Dispose() => Flush();
    }

    sealed class Storage : ICheckpointStorage, IDisposable
    {
        public readonly InMemoryFileCollection Files = new();
        public readonly List<string> Events = new();
        public readonly List<uint> PvlAttempts = new();
        public IReadOnlyDictionary<uint, uint>? LastMap;
        public bool FailPvl, FailKvi, FailTrl, FailChunk;
        public int Chunks;
        public TaskCompletionSource? UploadEntered, ContinueUpload;

        public Storage(KeyIndexSnapshot snapshot, uint? reusedPvl = null)
        {
            foreach (var source in snapshot.Sources.OrderBy(s => s.FileId))
            {
                if (source.FileType != KVFileType.TransactionLog && source.FileId != reusedPvl) continue;
                var destination = Files.AddFile("remote", FileIdParity.Any, source.FileId - 1);
                Assert.Equal(source.FileId, destination.Index);
                Copy(source, destination);
            }
        }

        static void Copy(KeyIndexFileSource source, IFileCollectionFile destination)
        {
            var buffer = new byte[1024];
            var writer = new MemWriter(destination.GetAppenderWriter());
            for (ulong offset = 0; offset < source.Length;)
            {
                var length = (int)Math.Min((ulong)buffer.Length, source.Length - offset);
                source.File.RandomRead(buffer.AsSpan(0, length), offset, false);
                writer.WriteBlock(buffer.AsSpan(0, length));
                offset += (uint)length;
            }
            writer.Flush();
        }

        public uint ReservePureValuesFileId() => Files.AddFile("pvl", FileIdParity.Even, 1000).Index;
        public async ValueTask EnsurePureValuesAsync(uint remoteId, KeyIndexFileSource source, CancellationToken ct)
        {
            Events.Add("pvl");
            PvlAttempts.Add(remoteId);
            UploadEntered?.TrySetResult();
            if (ContinueUpload != null) await ContinueUpload.Task.WaitAsync(ct);
            var target = Files.GetFile(remoteId)!;
            if (target.GetSize() == 0) Copy(source, target);
            if (FailPvl) throw new IOException("Response lost after PVL upload");
            Assert.Equal(source.Length, target.GetSize());
        }
        public ValueTask EnsureTransactionLogAsync(uint id, ulong length, CancellationToken ct)
        {
            Events.Add("trl");
            if (FailTrl) throw new IOException("Canonical TRL cut not yet published");
            Assert.True(Files.GetFile(id)!.GetSize() >= length);
            return ValueTask.CompletedTask;
        }
        public ValueTask PublishKeyIndexAsync(KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> map, CancellationToken ct)
        {
            Events.Add("kvi");
            if (FailKvi) throw new IOException("KVI publication rejected");
            var file = Files.AddFile("kvi", FileIdParity.Even, 1000);
            try
            {
                using var writer = new PositionLessStreamWriter(new UploadStream(file, () =>
                {
                    Chunks++;
                    if (FailChunk) throw new IOException("KVI chunk upload failed");
                }));
                snapshot.WriteTo(writer, 10000 + file.Index, map, ct);
                LastMap = map;
            }
            catch { file.Remove(); throw; } // The test store never selects a partial KVI.
            return ValueTask.CompletedTask;
        }
        public void Dispose() => Files.Dispose();
    }

    static byte[] Value(IKeyValueDBCursor cursor)
    {
        Span<byte> buffer = default;
        return cursor.GetValueSpan(ref buffer).ToArray();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StreamedKviRestoresWithRemappedPvlUnchangedTrlAndStableLocalReaders(bool compressed)
    {
        using var local = new InMemoryFileCollection();
        using var db = Open(local, compressed);
        await Populate(db);
        using var oldReader = db.StartReadOnlyTransaction();
        using var snapshot = db.CaptureKeyIndexSnapshot();
        Assert.Contains(snapshot.Sources, s => s.FileType == KVFileType.PureValues);
        Assert.Contains(snapshot.Sources, s => s.FileType == KVFileType.TransactionLog);
        var localFiles = local.Enumerate().Select(f => f.Index).Order().ToArray();
        using var storage = new Storage(snapshot);
        var publisher = new CheckpointPublisher(storage);
        await publisher.PublishAsync(snapshot);
        Assert.Equal("kvi", storage.Events[^1]);
        Assert.True(storage.Chunks > 1);
        Assert.All(storage.LastMap!, p => Assert.NotEqual(p.Key, p.Value));
        Assert.Equal(localFiles, local.Enumerate().Select(f => f.Index).Order());
        using (var tr = await db.StartWritingTransaction(63ul))
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([0], "new"u8);
            tr.Commit();
        }
        using var restored = Open(storage.Files, compressed);
        using var reader = restored.StartReadOnlyTransaction();
        Assert.Equal(62ul, reader.GetCommitUlong());
        Assert.Equal(1234ul, reader.GetUlong(2));
        using var actual = reader.CreateCursor();
        using var expected = oldReader.CreateCursor();
        foreach (var key in Enumerable.Range(0, 60).Select(i => (byte)i).Concat(new byte[] { 200, 201, 202 }))
        {
            Assert.True(expected.FindExactKey([key]));
            Assert.True(actual.FindExactKey([key]));
            Assert.Equal(Value(expected), Value(actual));
        }
    }

    [Fact]
    public async Task DownloadedPvlKeepsIdAndSuccessfulUploadsAreReusedAcrossSnapshots()
    {
        using var local = new InMemoryFileCollection();
        using var db = Open(local);
        await Populate(db);
        using var snapshot = db.CaptureKeyIndexSnapshot();
        var downloaded = snapshot.Sources.First(s => s.FileType == KVFileType.PureValues);
        using var storage = new Storage(snapshot, downloaded.FileId);
        var publisher = new CheckpointPublisher(storage);
        publisher.RememberDownloadedPureValues(downloaded);
        await publisher.PublishAsync(snapshot);
        Assert.Equal(downloaded.FileId, storage.LastMap![downloaded.FileId]);
        var uploaded = storage.PvlAttempts.Count;
        await db.Compact(CancellationToken.None);
        using var next = db.CaptureKeyIndexSnapshot();
        await publisher.PublishAsync(next);
        Assert.Equal(uploaded, storage.PvlAttempts.Count);
    }

    [Theory]
    [InlineData("pvl")]
    [InlineData("trl")]
    [InlineData("kvi")]
    [InlineData("chunk")]
    public async Task FailedPublicationRetainsPlacementsAndNeverStartsKviBeforeDependencies(string failure)
    {
        using var local = new InMemoryFileCollection();
        using var db = Open(local);
        await Populate(db);
        using var snapshot = db.CaptureKeyIndexSnapshot();
        using var storage = new Storage(snapshot)
        {
            FailPvl = failure == "pvl", FailTrl = failure == "trl", FailKvi = failure == "kvi", FailChunk = failure == "chunk"
        };
        var publisher = new CheckpointPublisher(storage);
        await Assert.ThrowsAsync<IOException>(() => publisher.PublishAsync(snapshot).AsTask());
        if (failure is "pvl" or "trl") Assert.DoesNotContain("kvi", storage.Events);
        var attempts = storage.PvlAttempts.ToArray();
        storage.FailPvl = storage.FailTrl = storage.FailKvi = storage.FailChunk = false;
        await publisher.PublishAsync(snapshot);
        if (failure is "kvi" or "chunk") Assert.Equal(attempts, storage.PvlAttempts);
        if (failure == "pvl") Assert.Equal(attempts[0], storage.PvlAttempts[1]);
        using var restored = Open(storage.Files);
        using var reader = restored.StartReadOnlyTransaction();
        Assert.Equal(62ul, reader.GetCommitUlong());
    }

    [Fact]
    public async Task CancellationDuringPvlUploadDoesNotStartKvi()
    {
        using var local = new InMemoryFileCollection();
        using var db = Open(local);
        await Populate(db);
        using var snapshot = db.CaptureKeyIndexSnapshot();
        using var storage = new Storage(snapshot)
        {
            UploadEntered = new(TaskCreationOptions.RunContinuationsAsynchronously),
            ContinueUpload = new(TaskCreationOptions.RunContinuationsAsynchronously)
        };
        using var cancellation = new CancellationTokenSource();
        var publish = new CheckpointPublisher(storage).PublishAsync(snapshot, cancellation.Token).AsTask();
        await storage.UploadEntered.Task;
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => publish);
        Assert.DoesNotContain("kvi", storage.Events);
    }

    [Fact]
    public async Task ExportRejectsMissingPvlAndAnyTrlRemapBeforeWriting()
    {
        using var local = new InMemoryFileCollection();
        using var db = Open(local);
        await Populate(db);
        using var snapshot = db.CaptureKeyIndexSnapshot();
        using var remote = new InMemoryFileCollection();
        var file = remote.AddFile("kvi");
        var map = snapshot.Sources.Where(s => s.FileType == KVFileType.PureValues).ToDictionary(s => s.FileId, s => s.FileId);
        var trl = snapshot.Sources.First(s => s.FileType == KVFileType.TransactionLog);
        map.Add(trl.FileId, trl.FileId + 1000);
        Assert.Throws<ArgumentException>(() => snapshot.WriteTo(file.GetAppenderWriter(), 10000, map));
        Assert.Equal(0ul, file.GetSize());
        map.Clear();
        Assert.Throws<ArgumentException>(() => snapshot.WriteTo(file.GetAppenderWriter(), 10000, map));
        Assert.Equal(0ul, file.GetSize());
    }
}
