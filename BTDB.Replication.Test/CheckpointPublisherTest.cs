using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication.Test.Simulation;
using BTDB.StreamLayer;
using Xunit;

namespace BTDB.Replication.Test;

public class CheckpointPublisherTest
{
    internal static BTreeKeyValueDB Open(IFileCollection files, bool compressed = false) => new(new KeyValueDBOptions
    {
        FileCollection = files, Compression = new NoCompressionStrategy(), FileSplitSize = 8096,
        KviCompressionStrategy = new DefaultCompressionKviStrategy(compressed ? 3 : -1, thresholdKeysToEnableCompression: 0),
        CompactorScheduler = null
    });

    internal static ValueTask<BTreeKeyValueDB> OpenForPublication(InMemoryReplicationFileStorage files,
        TransactionLogCapture capture, bool compressed = false) => BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
    {
        FileCollection = new LocalReplicatedCollection(files), TransactionLogCapture = capture,
        Compression = new NoCompressionStrategy(), FileSplitSize = 8096,
        KviCompressionStrategy = new DefaultCompressionKviStrategy(compressed ? 3 : -1, thresholdKeysToEnableCompression: 0),
        CompactorScheduler = null
    });

    internal static LeaseAuthority CreateAuthority()
    {
        var clock = new DeterministicScheduler(17);
        var authority = new LeaseAuthority(clock.CreateScope("checkpoint"), 0, TimeSpan.FromTicks(1));
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromSeconds(10)));
        return authority;
    }

    internal static CanonicalTrlPublisher CreateCanonical(BTreeKeyValueDB db, TransactionLogCapture capture,
        Storage storage, LeaseAuthority? authority = null) => new(db, capture, storage, authority ?? CreateAuthority(),
        1, id => $"trl/{id}");

    internal static async Task Populate(BTreeKeyValueDB db)
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

    internal sealed class Storage : ICheckpointStorage, ICanonicalTrlStorage, IDisposable
    {
        public readonly InMemoryReplicationFileStorage Files = new();
        public readonly Dictionary<uint, KVFileType> Types = new();
        public readonly List<string> Events = new();
        public readonly List<uint> PvlAttempts = new();
        public readonly List<uint> KviAttempts = new();
        public bool LoseKviResponse;
        public IReadOnlyDictionary<uint, uint>? LastMap;
        public bool FailPvl, FailKvi, FailTrl, FailChunk;
        public int Chunks;
        readonly Dictionary<string, (uint Id, TrlObjectState State)> _trls = new();
        int _version;
        public bool DelayTrl;
        public bool CancelAfterTrl;
        public bool RejectTrl;
        public readonly Queue<Action> Delayed = new();
        public Action? AfterPvl;
        public Action<TrlWrite>? AfterTrl;
        public Action? BeforeKvi;
        public int ReadChunkSize = int.MaxValue;
        public bool CorruptRead, TruncateRead;
        public bool OmitChecksum;
        public bool IsSealed = true;
        public Func<RemoteFile, ulong, CancellationToken, ValueTask>? BeforeRead;
        public Func<CancellationToken, ValueTask>? BeforeEnumerate;
        public TaskCompletionSource? UploadEntered, ContinueUpload;

        public Storage() { }

        // Reconstruct the remote service from persisted object bodies/metadata only. No publisher, capture,
        // authority, receipts, pending requests or test callbacks survive a simulated process restart.
        public Storage CopyPersistedObjects()
        {
            var copy = new Storage { _version = _version };
            foreach (var file in Files.Enumerate())
            {
                var type = Types[file.Index];
                var hint = type switch
                {
                    KVFileType.TransactionLog => "trl",
                    KVFileType.PureValues => "pvl",
                    _ => "kvi"
                };
                var destination = copy.Files.ImportFile(file.Index, hint);
                Copy(new(file.Index, type, file.GetSize(), 0, file), destination);
                copy.Types.Add(file.Index, type);
            }
            foreach (var (key, (id, state)) in _trls)
            {
                if (copy.Files.GetFile(id) == null) continue;
                copy._trls.Add(key, (id, new(state.Token, state.Length, TrlMetadata.Decode(state.Metadata.Encode()))));
            }
            return copy;
        }

        public Storage(KeyIndexSnapshot snapshot, uint? reusedPvl = null)
        {
            // A remote file can survive after local compaction removed its ID.
            var oldPvl = snapshot.Sources.First(s => s.FileType == KVFileType.PureValues);
            Copy(oldPvl, Files.ImportFile(1000, "pvl"));
            Types.Add(1000, KVFileType.PureValues);
            foreach (var source in snapshot.Sources.OrderBy(s => s.FileId))
            {
                if (source.FileType != KVFileType.PureValues || source.FileId != reusedPvl) continue;
                var destination = Files.ImportFile(source.FileId, "remote");
                Assert.Equal(source.FileId, destination.Index);
                Copy(source, destination);
                Types.Add(source.FileId, source.FileType);
            }
        }

        internal static void Copy(KeyIndexFileSource source, IFileCollectionFile destination)
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

        static string Hash(IFileCollectionFile file)
        {
            var bytes = new byte[checked((int)file.GetSize())];
            file.RandomRead(bytes, 0, false);
            return Convert.ToHexString(SHA256.HashData(bytes));
        }

        public RemoteFile Describe(uint id)
        {
            var file = Files.GetFile(id)!;
            var bytes = new byte[checked((int)file.GetSize())];
            file.RandomRead(bytes, 0, false);
            var checksum = Convert.ToHexString(SHA256.HashData(bytes));
            return new(id, Types[id], file.GetSize(), checksum, IsSealed, OmitChecksum ? null : checksum);
        }

        public async IAsyncEnumerable<RemoteFile> EnumerateAsync([EnumeratorCancellation] CancellationToken ct)
        {
            if (BeforeEnumerate != null) await BeforeEnumerate(ct);
            foreach (var file in Files.Enumerate())
            {
                ct.ThrowIfCancellationRequested();
                yield return Describe(file.Index);
            }
        }

        public async ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            if (BeforeRead != null) await BeforeRead(file, offset, ct);
            if (TruncateRead) return 0;
            if (Files.GetFile(file.FileId) == null || Describe(file.FileId).Version != file.Version)
                throw new IOException("Remote version changed or disappeared.");
            var count = (int)Math.Min((ulong)Math.Min(buffer.Length, ReadChunkSize), file.Length - offset);
            Files.GetFile(file.FileId)!.RandomRead(buffer.Span[..count], offset, false);
            if (CorruptRead && count != 0) buffer.Span[0] ^= 1;
            return count;
        }

        public async ValueTask EnsurePureValuesAsync(uint remoteId, KeyIndexFileSource source, CancellationToken ct)
        {
            Events.Add("pvl");
            PvlAttempts.Add(remoteId);
            UploadEntered?.TrySetResult();
            if (ContinueUpload != null) await ContinueUpload.Task.WaitAsync(ct);
            using var expected = new InMemoryReplicationFileStorage();
            var expectedFile = expected.ImportFile(remoteId, "pvl");
            Copy(source, expectedFile);
            var sha = Hash(expectedFile);
            var target = Files.GetFile(remoteId);
            if (target != null)
            {
                if (Describe(remoteId).Sha256 != sha) throw new RemoteFileConflictException();
            }
            else
            {
                target = Files.ImportFile(remoteId, "pvl");
                Copy(source, target);
                Types.Add(remoteId, KVFileType.PureValues);
            }
            if (FailPvl) throw new IOException("Response lost after PVL upload");
            Assert.Equal(source.Length, target.GetSize());
            AfterPvl?.Invoke();
        }
        public ValueTask<TrlObjectState?> ReadAsync(string key, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            return ValueTask.FromResult(_trls.TryGetValue(key, out var value) ? value.State : null);
        }
        public ValueTask ReadRangeAsync(string key, string token, uint offset, Memory<byte> destination, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var (id, state) = _trls[key];
            Assert.Equal(token, state.Token);
            Files.GetFile(id)!.RandomRead(destination.Span, offset, false);
            return ValueTask.CompletedTask;
        }
        public ValueTask<TrlWriteResult> WriteAsync(TrlWrite write, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            Events.Add("trl");
            if (FailTrl) throw new IOException("Canonical TRL request failed");
            var suffix = new byte[write.AppendLength];
            write.ReadAppend(0, suffix);
            if (DelayTrl)
            {
                Delayed.Enqueue(() => Apply(write, suffix));
                return ValueTask.FromResult(new TrlWriteResult(TrlWriteOutcome.Ambiguous));
            }
            if (RejectTrl) return ValueTask.FromResult(new TrlWriteResult(TrlWriteOutcome.Rejected));
            var result = Apply(write, suffix);
            if (CancelAfterTrl) throw new OperationCanceledException("TRL response lost after effect");
            return ValueTask.FromResult(result);
        }
        TrlWriteResult Apply(TrlWrite write, byte[] suffix)
        {
            var existing = _trls.GetValueOrDefault(write.Key).State;
            if (existing?.Token != write.ExpectedToken || (existing?.Length ?? 0) != write.ExpectedLength)
                return new(TrlWriteOutcome.Rejected);
            var file = Files.GetFile(write.FileId);
            if (file == null)
            {
                file = Files.ImportFile(write.FileId, "trl");
                Types.Add(write.FileId, KVFileType.TransactionLog);
            }
            Assert.Equal((ulong)write.ExpectedLength, file.GetSize());
            var writer = new MemWriter(file.GetAppenderWriter());
            writer.WriteBlock(suffix);
            writer.Flush();
            var state = new TrlObjectState((++_version).ToString(), write.Length, write.Metadata);
            _trls[write.Key] = (write.FileId, state);
            AfterTrl?.Invoke(write);
            return new(TrlWriteOutcome.Applied, state);
        }
        public ValueTask PublishKeyIndexAsync(uint id, KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> map, CancellationToken ct)
        {
            KviAttempts.Add(id);
            BeforeKvi?.Invoke();
            Events.Add("kvi");
            if (FailKvi) throw new IOException("KVI publication rejected");
            using var expected = new InMemoryReplicationFileStorage();
            var expectedFile = expected.ImportFile(id, "kvi");
            snapshot.WriteTo(expectedFile.GetAppenderWriter(), 10000 + id, map, ct);
            var sha = Hash(expectedFile);
            if (Files.GetFile(id) != null)
            {
                if (Describe(id).Sha256 != sha) throw new RemoteFileConflictException();
                return ValueTask.CompletedTask;
            }
            var file = Files.ImportFile(id, "kvi");
            try
            {
                using var writer = new PositionLessStreamWriter(new UploadStream(file, () =>
                {
                    Chunks++;
                    if (FailChunk) throw new IOException("KVI chunk upload failed");
                }));
                snapshot.WriteTo(writer, 10000 + file.Index, map, ct);
                LastMap = map;
                Types.Add(id, KVFileType.KeyIndex);
            }
            catch { file.Remove(); throw; } // The test store never selects a partial KVI.
            if (LoseKviResponse)
            {
                LoseKviResponse = false;
                throw new IOException("KVI response lost after publication");
            }
            return ValueTask.CompletedTask;
        }
        public void Dispose() => Files.Dispose();
    }

    sealed class PublicationFixture : IDisposable
    {
        public readonly InMemoryReplicationFileStorage Local;
        public readonly BTreeKeyValueDB Db;
        public readonly Storage Remote = new();
        public readonly LeaseAuthority Authority = CreateAuthority();
        public readonly CanonicalTrlPublisher Canonical;
        public readonly CheckpointPublisher Publisher;
        PublicationFixture(InMemoryReplicationFileStorage local, BTreeKeyValueDB db, TransactionLogCapture capture)
        {
            Local = local;
            Db = db;
            Canonical = CreateCanonical(db, capture, Remote, Authority);
            Publisher = new(new ReplicationFileSet(local, Remote), Canonical);
        }
        public static async Task<PublicationFixture> Create()
        {
            var local = new InMemoryReplicationFileStorage();
            var capture = new TransactionLogCapture();
            var db = await OpenForPublication(local, capture);
            var fixture = new PublicationFixture(local, db, capture);
            try { await Populate(db); return fixture; }
            catch { fixture.Dispose(); throw; }
        }
        public void Dispose()
        {
            Canonical.Dispose();
            Db.Dispose();
            Local.Dispose();
            Remote.Dispose();
        }
        public async Task AssertRestoresSnapshot(KeyIndexSnapshot snapshot)
        {
            using var cache = new InMemoryReplicationFileStorage();
            await using var files = new ReplicationFileSet(cache, Remote);
            await files.InitializeAsync();
            using var restored = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
            {
                FileCollection = files, Compression = new NoCompressionStrategy(), CompactorScheduler = null
            });
            using var reader = restored.StartReadOnlyTransaction();
            Assert.Equal(snapshot.CommitUlong, reader.GetCommitUlong());
            Assert.Equal(63, reader.GetKeyValueCount());
            Assert.Equal(1234ul, reader.GetUlong(2));
        }
    }

    [Fact]
    public async Task PendingCanonicalSelectionBlocksAllCheckpointUploadsUntilReconciled()
    {
        using var f = await PublicationFixture.Create();
        using var snapshot = f.Db.CaptureKeyIndexSnapshot();
        f.Remote.DelayTrl = true;
        Assert.Equal(CheckpointPublishResult.Pending, await f.Publisher.PublishAsync(snapshot));
        Assert.Single(f.Remote.Delayed);
        Assert.Equal(default, f.Canonical.PublishedPosition);
        Assert.Equal(new[] { "trl" }, f.Remote.Events);
        Assert.Equal(0, f.Remote.Chunks);

        // The pending request prepares a successor, but its presence must not permit a KVI upload.
        f.Remote.Delayed.Dequeue()();
        Assert.NotEmpty(f.Remote.Files.Enumerate());
        using (var tr = await f.Db.StartWritingTransaction(63ul))
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([250], "later"u8);
            tr.Commit();
        }
        f.Remote.DelayTrl = false;
        f.Remote.BeforeKvi = () =>
        {
            Assert.Equal(new TransactionLogPosition(snapshot.TransactionLogFileId, snapshot.TransactionLogOffset),
                f.Canonical.PublishedPosition);
            Assert.All(snapshot.Sources.Where(s => s.FileType == KVFileType.TransactionLog),
                source => Assert.True(f.Remote.Files.GetFile(source.FileId)!.GetSize() >= source.Length));
        };
        Assert.Equal(CheckpointPublishResult.Published, await f.Publisher.PublishAsync(snapshot));
        Assert.Equal("kvi", f.Remote.Events[^1]);
        await f.AssertRestoresSnapshot(snapshot);
    }

    [Theory]
    [InlineData("before")]
    [InlineData("trl")]
    [InlineData("pvl")]
    public async Task AuthorityLossStopsCheckpointBeforeFirstKviChunk(string boundary)
    {
        using var f = await PublicationFixture.Create();
        using var snapshot = f.Db.CaptureKeyIndexSnapshot();
        if (boundary == "before") f.Authority.Fence();
        if (boundary == "trl") f.Remote.AfterTrl = write => { if (write.FileId == 1) f.Authority.Fence(); };
        if (boundary == "pvl") f.Remote.AfterPvl = f.Authority.Fence;
        Assert.Equal(CheckpointPublishResult.AuthorityLost, await f.Publisher.PublishAsync(snapshot));
        Assert.DoesNotContain("kvi", f.Remote.Events);
        Assert.Equal(0, f.Remote.Chunks);
        if (boundary == "before") Assert.Empty(f.Remote.Events);
        if (boundary == "trl") Assert.DoesNotContain("pvl", f.Remote.Events);
        if (boundary == "pvl") Assert.Single(f.Remote.PvlAttempts);
        using var tr = await f.Db.StartWritingTransaction(63ul);
        tr.Commit(); // Fencing remote work does not gate local application commits.
    }

    [Fact]
    public async Task RestoredCanonicalCutCanPublishCheckpointBeforeAnyNewLocalCommit()
    {
        using var f = await PublicationFixture.Create();
        using (var initial = f.Db.CaptureKeyIndexSnapshot())
            Assert.Equal(CheckpointPublishResult.Published, await f.Publisher.PublishAsync(initial));
        using var cache = new InMemoryReplicationFileStorage();
        await using var files = new ReplicationFileSet(cache, f.Remote);
        await files.InitializeAsync();
        var capture = new TransactionLogCapture();
        using var restored = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = files, TransactionLogCapture = capture,
            Compression = new NoCompressionStrategy(), CompactorScheduler = null
        });
        Assert.Equal(default, capture.Completed);
        using var snapshot = restored.CaptureKeyIndexSnapshot();
        using var canonical = new CanonicalTrlPublisher(restored, capture, f.Remote, f.Authority, 1,
            id => $"trl/{id}", f.Canonical.Tail);
        var previousTrlWrites = f.Remote.Events.Count(e => e == "trl");
        var previousPvlWrites = f.Remote.PvlAttempts.Count;
        Assert.Equal(CheckpointPublishResult.Published, await new CheckpointPublisher(files, canonical).PublishAsync(snapshot));
        Assert.Equal(previousTrlWrites, f.Remote.Events.Count(e => e == "trl"));
        Assert.Equal(previousPvlWrites, f.Remote.PvlAttempts.Count);
        await f.AssertRestoresSnapshot(snapshot);
    }

    [Fact]
    public async Task CanonicalConflictPermanentlyPreventsCheckpointUpload()
    {
        using var f = await PublicationFixture.Create();
        using var snapshot = f.Db.CaptureKeyIndexSnapshot();
        f.Remote.RejectTrl = true;
        Assert.Equal(CheckpointPublishResult.Conflict, await f.Publisher.PublishAsync(snapshot));
        f.Remote.RejectTrl = false;
        Assert.Equal(CheckpointPublishResult.Conflict, await f.Publisher.PublishAsync(snapshot, retryPending: true));
        Assert.Equal(new[] { "trl" }, f.Remote.Events);
        Assert.Equal(0, f.Remote.Chunks);
    }

    [Fact]
    public async Task CancelledCanonicalReplyDoesNotStartKviAndCanResumeTheSameSnapshot()
    {
        using var f = await PublicationFixture.Create();
        using var snapshot = f.Db.CaptureKeyIndexSnapshot();
        f.Remote.CancelAfterTrl = true;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => f.Publisher.PublishAsync(snapshot).AsTask());
        Assert.Equal(new[] { "trl" }, f.Remote.Events);
        Assert.Equal(0, f.Remote.Chunks);
        f.Remote.CancelAfterTrl = false;
        Assert.Equal(CheckpointPublishResult.Published, await f.Publisher.PublishAsync(snapshot));
        await f.AssertRestoresSnapshot(snapshot);
    }

    [Fact]
    public async Task LostKviResponseRetainsIdentityAndBlocksAnotherSnapshotUntilReconciled()
    {
        using var f = await PublicationFixture.Create();
        using var snapshot = f.Db.CaptureKeyIndexSnapshot();
        f.Remote.LoseKviResponse = true;
        await Assert.ThrowsAsync<IOException>(() => f.Publisher.PublishAsync(snapshot).AsTask());
        var id = Assert.Single(f.Remote.KviAttempts);
        var chunks = f.Remote.Chunks;
        var pvlUploads = f.Remote.PvlAttempts.Count;
        using var other = f.Db.CaptureKeyIndexSnapshot();
        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Publisher.PublishAsync(other).AsTask());
        Assert.Single(f.Remote.KviAttempts);
        Assert.Equal(CheckpointPublishResult.Published, await f.Publisher.PublishAsync(snapshot));
        Assert.Equal(new[] { id, id }, f.Remote.KviAttempts);
        Assert.Equal(chunks, f.Remote.Chunks);
        Assert.Equal(pvlUploads, f.Remote.PvlAttempts.Count);
        await f.AssertRestoresSnapshot(snapshot);
        Assert.Equal(CheckpointPublishResult.Published, await f.Publisher.PublishAsync(other));
        Assert.True(f.Remote.KviAttempts[^1] > id);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ConflictingOrMissingShaFencesCheckpointSession(bool kvi, bool missingSha)
    {
        using var f = await PublicationFixture.Create();
        using var snapshot = f.Db.CaptureKeyIndexSnapshot();
        f.Remote.LoseKviResponse = kvi;
        f.Remote.FailPvl = !kvi;
        await Assert.ThrowsAsync<IOException>(() => f.Publisher.PublishAsync(snapshot).AsTask());
        var id = kvi ? Assert.Single(f.Remote.KviAttempts) : Assert.Single(f.Remote.PvlAttempts);
        f.Remote.FailPvl = false;
        if (missingSha) f.Remote.OmitChecksum = true;
        else
        {
            var file = f.Remote.Files.GetFile(id)!;
            var bytes = new byte[checked((int)file.GetSize())];
            file.RandomRead(bytes, 0, false);
            bytes[^1] ^= 1;
            file.Remove();
            var writer = new MemWriter(f.Remote.Files.ImportFile(id, kvi ? "kvi" : "pvl").GetAppenderWriter());
            writer.WriteBlock(bytes);
            writer.Flush();
        }
        Assert.Equal(CheckpointPublishResult.Conflict, await f.Publisher.PublishAsync(snapshot));
        Assert.False(f.Canonical.HasAuthority);
        Assert.Equal(CheckpointPublishResult.AuthorityLost, await f.Publisher.PublishAsync(snapshot));
    }

    [Fact]
    public async Task RemoteAllocationRefreshesInventoryWithoutReservationObjects()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new Storage();
        await using var files = new ReplicationFileSet(local, remote);
        Assert.Equal(2u, await files.AllocateRemoteFileIdAsync());
        Assert.Empty(remote.Files.Enumerate());
        // Another publication since the previous allocation must be observed.
        var writer = new MemWriter(remote.Files.ImportFile(100, "pvl").GetAppenderWriter());
        writer.WriteUInt8(1);
        writer.Flush();
        remote.Types.Add(100, KVFileType.PureValues);
        Assert.Equal(102u, await files.AllocateRemoteFileIdAsync());
        Assert.Equal(104u, await files.AllocateRemoteFileIdAsync());
        await using var restarted = new ReplicationFileSet(local, remote);
        Assert.Equal(102u, await restarted.AllocateRemoteFileIdAsync()); // Unpublished choices need no cleanup.
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
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await OpenForPublication(local, capture, compressed);
        await Populate(db);
        using var oldReader = db.StartReadOnlyTransaction();
        using var snapshot = db.CaptureKeyIndexSnapshot();
        Assert.Contains(snapshot.Sources, s => s.FileType == KVFileType.PureValues);
        Assert.Contains(snapshot.Sources, s => s.FileType == KVFileType.TransactionLog);
        var localFiles = local.Enumerate().Select(f => f.Index).Order().ToArray();
        using var storage = new Storage(snapshot);
        using var canonical = CreateCanonical(db, capture, storage);
        var publisher = new CheckpointPublisher(new ReplicationFileSet(local, storage), canonical);
        Assert.Equal(CheckpointPublishResult.Published, await publisher.PublishAsync(snapshot, retryPending: true));
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
        using var restoredLocal = new InMemoryReplicationFileStorage();
        await using var restoreFiles = new ReplicationFileSet(restoredLocal, storage);
        var cachedPvls = new Dictionary<uint, uint>();
        var nextLocalId = 10000u;
        foreach (var remoteId in storage.LastMap!.Values)
        {
            var source = storage.Files.GetFile(remoteId)!;
            var cached = restoredLocal.ImportFile(nextLocalId, "pvl");
            var remoteFile = storage.Describe(remoteId);
            Storage.Copy(new(remoteId, KVFileType.PureValues, source.GetSize(), 0, source), cached);
            restoreFiles.RememberVerifiedPureValues(new(nextLocalId, KVFileType.PureValues, cached.GetSize(), 0, cached), remoteFile);
            cachedPvls.Add(remoteId, nextLocalId);
            nextLocalId += 2;
        }
        await restoreFiles.InitializeAsync();
        using var restored = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = restoreFiles, Compression = new NoCompressionStrategy(), CompactorScheduler = null
            });
        Assert.Null(restoredLocal.GetFile(1000)); // Unreferenced remote PVL stays lazy, including its metadata.
        Assert.All(cachedPvls, mapping =>
        {
            Assert.Equal(mapping.Value, restoreFiles.GetLocalFileId(mapping.Key));
            Assert.NotNull(restoredLocal.GetFile(mapping.Value));
            Assert.Null(restoredLocal.GetFile(mapping.Key));
        });
        using (var exported = restored.CaptureKeyIndexSnapshot())
        {
            Assert.All(exported.Sources.Where(source => source.FileType == KVFileType.PureValues),
                source => Assert.Contains(source.FileId, cachedPvls.Values));
        }
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
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await OpenForPublication(local, capture);
        await Populate(db);
        using var snapshot = db.CaptureKeyIndexSnapshot();
        var downloaded = snapshot.Sources.First(s => s.FileType == KVFileType.PureValues);
        using var storage = new Storage(snapshot, downloaded.FileId);
        var files = new ReplicationFileSet(local, storage);
        using var canonical = CreateCanonical(db, capture, storage);
        var publisher = new CheckpointPublisher(files, canonical);
        files.RememberVerifiedPureValues(downloaded, storage.Describe(downloaded.FileId));
        Assert.Equal(CheckpointPublishResult.Published, await publisher.PublishAsync(snapshot, retryPending: true));
        Assert.Equal(downloaded.FileId, storage.LastMap![downloaded.FileId]);
        var uploaded = storage.PvlAttempts.Count;
        await db.Compact(CancellationToken.None);
        using var next = db.CaptureKeyIndexSnapshot();
        Assert.Equal(CheckpointPublishResult.Published, await publisher.PublishAsync(next));
        Assert.Equal(uploaded, storage.PvlAttempts.Count);
    }

    [Theory]
    [InlineData("pvl")]
    [InlineData("trl")]
    [InlineData("kvi")]
    [InlineData("chunk")]
    public async Task FailedPublicationRetainsPlacementsAndNeverStartsKviBeforeDependencies(string failure)
    {
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await OpenForPublication(local, capture);
        await Populate(db);
        using var snapshot = db.CaptureKeyIndexSnapshot();
        using var storage = new Storage(snapshot)
        {
            FailPvl = failure == "pvl", FailTrl = failure == "trl", FailKvi = failure == "kvi", FailChunk = failure == "chunk"
        };
        using var canonical = CreateCanonical(db, capture, storage);
        var publisher = new CheckpointPublisher(new ReplicationFileSet(local, storage), canonical);
        await Assert.ThrowsAsync<IOException>(() => publisher.PublishAsync(snapshot).AsTask());
        if (failure is "pvl" or "trl") Assert.DoesNotContain("kvi", storage.Events);
        var attempts = storage.PvlAttempts.ToArray();
        storage.FailPvl = storage.FailTrl = storage.FailKvi = storage.FailChunk = false;
        Assert.Equal(CheckpointPublishResult.Published, await publisher.PublishAsync(snapshot, retryPending: true));
        if (failure is "kvi" or "chunk") Assert.Equal(attempts, storage.PvlAttempts);
        if (failure == "pvl") Assert.Equal(attempts[0], storage.PvlAttempts[1]);
        using var restored = Open(storage.Files);
        using var reader = restored.StartReadOnlyTransaction();
        Assert.Equal(62ul, reader.GetCommitUlong());
    }

    [Fact]
    public async Task CancellationDuringPvlUploadDoesNotStartKvi()
    {
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await OpenForPublication(local, capture);
        await Populate(db);
        using var snapshot = db.CaptureKeyIndexSnapshot();
        using var storage = new Storage(snapshot)
        {
            UploadEntered = new(TaskCreationOptions.RunContinuationsAsynchronously),
            ContinueUpload = new(TaskCreationOptions.RunContinuationsAsynchronously)
        };
        using var cancellation = new CancellationTokenSource();
        using var canonical = CreateCanonical(db, capture, storage);
        var publish = new CheckpointPublisher(new ReplicationFileSet(local, storage), canonical).PublishAsync(snapshot, cancellation.Token).AsTask();
        await storage.UploadEntered.Task;
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => publish);
        Assert.DoesNotContain("kvi", storage.Events);
    }

    [Fact]
    public async Task ExportRejectsMissingPvlAndAnyTrlRemapBeforeWriting()
    {
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await OpenForPublication(local, capture);
        await Populate(db);
        using var snapshot = db.CaptureKeyIndexSnapshot();
        using var remote = new InMemoryReplicationFileStorage();
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
