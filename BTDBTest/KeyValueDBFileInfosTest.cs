using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class KeyValueDBFileInfosTest
{
    static BTreeKeyValueDB Open(IFileCollection files, uint fileSplitSize = int.MaxValue) => new(new KeyValueDBOptions
    {
        FileCollection = files, Compression = new NoCompressionStrategy(), CompactorScheduler = null,
        FileSplitSize = fileSplitSize
    });

    static void WriteKey(IKeyValueDB db, byte key)
    {
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue([key], new byte[50]);
        tr.Commit();
    }

    static void WriteFile(IFileCollection files, byte[] content)
    {
        var file = files.AddFile("trl");
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock(content);
        writer.Flush();
        file.HardFlush();
    }

    [Fact]
    public void ForeignAndCorruptedFilesAreTreatedAsUnknownAndDeleted()
    {
        using var files = new InMemoryFileCollection();
        using var otherFiles = new InMemoryFileCollection();
        using (var db = Open(files)) WriteKey(db, 1);
        using (var db = Open(otherFiles)) WriteKey(db, 2);
        var foreignTrl = otherFiles.Enumerate().First();
        var foreignContent = new byte[foreignTrl.GetSize()];
        foreignTrl.RandomRead(foreignContent, 0, false);
        // Transaction log of another database (different Guid)
        WriteFile(files, foreignContent);
        // Unknown magic
        WriteFile(files, "NOTBTDB"u8.ToArray());
        // Valid magic but unknown file type
        WriteFile(files, [.. "BTDB2"u8, 200]);
        // Truncated header
        WriteFile(files, [.. "BTDB3"u8, 1, 2]);
        Assert.Equal(5u, files.GetCount());
        var infos = new FileCollectionWithFileInfos(files);
        Assert.Equal(4, infos.FileInfos.Count(i => i.Value.FileType == KVFileType.Unknown));
        using (var db = Open(files))
        {
            using var tr = db.StartReadOnlyTransaction();
            Assert.Equal(1, tr.GetKeyValueCount());
        }

        Assert.Equal(1u, files.GetCount());
    }

    // Enumerates files newest first, as a directory listing may.
    sealed class ReversedFileCollection(IFileCollection inner) : IFileCollection
    {
        public IFileCollectionFile AddFile(string humanHint) => inner.AddFile(humanHint);
        public uint GetCount() => inner.GetCount();
        public IFileCollectionFile GetFile(uint index) => inner.GetFile(index);
        public System.Collections.Generic.IEnumerable<IFileCollectionFile> Enumerate() => inner.Enumerate().Reverse().ToList();
        public void ConcurrentTemporaryTruncate(uint index, uint offset) => inner.ConcurrentTemporaryTruncate(index, offset);
        public void Dispose() => inner.Dispose();
    }

    /// A foreign transaction log listed before the database's own files does not make them foreign: the database
    /// keeps its content and only the foreign file is deleted.
    [Fact]
    public void ForeignFileListedFirstDoesNotDeleteTheDatabase()
    {
        using var files = new InMemoryFileCollection();
        using var otherFiles = new InMemoryFileCollection();
        using (var db = Open(files, 1024))
            for (byte i = 0; i < 30; i++) WriteKey(db, i);
        using (var db = Open(otherFiles)) WriteKey(db, 200);
        var foreignTrl = otherFiles.Enumerate().First();
        var foreignContent = new byte[foreignTrl.GetSize()];
        foreignTrl.RandomRead(foreignContent, 0, false);
        var own = files.GetCount();
        Assert.True(own > 1);
        WriteFile(files, foreignContent);
        using (var db = Open(new ReversedFileCollection(files)))
        {
            using var tr = db.StartReadOnlyTransaction();
            Assert.Equal(30, tr.GetKeyValueCount());
        }

        Assert.Equal(own, files.GetCount());
    }

    /// A key index whose Guid byte is damaged, listed first, is the one foreign file: the database replays its logs.
    [Fact]
    public void KeyIndexWithDamagedGuidListedFirstDoesNotDeleteTheDatabase()
    {
        using var files = new InMemoryFileCollection();
        using (var db = Open(files, 1024))
        {
            for (byte i = 0; i < 30; i++) WriteKey(db, i);
            db.CreateKvi(default);
        }

        var index = files.Enumerate().Last();
        var content = new byte[index.GetSize()];
        index.RandomRead(content, 0, false);
        Assert.Equal(KVFileType.ModernKeyIndexWithUlongs, (KVFileType)content[5 + 16]);
        content[8] ^= 1; // inside the Guid after the "BTDB3" magic
        var damaged = files.AddFile("kvi");
        var writer = new MemWriter(damaged.GetAppenderWriter());
        writer.WriteBlock(content);
        writer.Flush();
        index.Remove();
        using (var db = Open(new ReversedFileCollection(files)))
        {
            using var tr = db.StartReadOnlyTransaction();
            Assert.Equal(30, tr.GetKeyValueCount());
        }
    }

    // Before the fix a damaged key length in a key index was allocated before the index was found damaged, so opening
    // allocated one and a half times it, here 384 MB, before it replayed the logs instead.
    [Fact]
    public void KeyIndexWithDamagedKeyLengthIsRejectedWithoutAllocatingIt()
    {
        using var files = new InMemoryFileCollection();
        using (var db = Open(files, 1024))
        {
            for (byte i = 0; i < 30; i++) WriteKey(db, i);
            db.CreateKvi(default);
        }

        var index = files.Enumerate().Last();
        var content = new byte[index.GetSize()];
        index.RandomRead(content, 0, false);
        var reader = MemReader.CreateFromPinnedSpan(content);
        FileKeyIndex.SkipHeader(ref reader);
        var firstEntry = (int)reader.GetCurrentPosition();
        var length = new MemWriter();
        length.WriteVUInt32(1 << 28);
        // The first entry shares no prefix, so its next number is the length of its key.
        var damagedContent = content[..(firstEntry + 1)].Concat(length.GetSpan().ToArray())
            .Concat(content[(firstEntry + 2)..]).ToArray();
        var damaged = files.AddFile("kvi");
        var writer = new MemWriter(damaged.GetAppenderWriter());
        writer.WriteBlock(damagedContent);
        writer.Flush();
        index.Remove();
        var before = GC.GetTotalAllocatedBytes(true);
        using (var db = Open(files))
        {
            using var tr = db.StartReadOnlyTransaction();
            Assert.Equal(30, tr.GetKeyValueCount());
        }

        Assert.InRange(GC.GetTotalAllocatedBytes(true) - before, 0, 64 << 20);
    }

    // Before the fix the count of ulongs in a key index header was allocated at once, so a damaged count made opening
    // allocate it, here 512 MB, before the header was found damaged.
    [Fact]
    public void KeyIndexWithDamagedUlongCountIsRejectedWithoutAllocatingIt()
    {
        using var files = new InMemoryFileCollection();
        using (var db = Open(files, 1024))
        {
            for (byte i = 0; i < 30; i++) WriteKey(db, i);
            db.CreateKvi(default);
        }

        var index = files.Enumerate().Last();
        var content = new byte[index.GetSize()];
        index.RandomRead(content, 0, false);
        var reader = MemReader.CreateFromPinnedSpan(content);
        FileCollectionWithFileInfos.SkipHeader(ref reader);
        Assert.Equal(KVFileType.ModernKeyIndexWithUlongs, (KVFileType)reader.ReadUInt8());
        reader.SkipVInt64();
        reader.SkipVUInt32();
        reader.SkipVUInt32();
        reader.SkipVUInt64();
        reader.SkipVUInt64();
        reader.Skip1Byte();
        var countAt = (int)reader.GetCurrentPosition();
        var count = new MemWriter();
        count.WriteVUInt32(1 << 26);
        var damagedContent = content[..countAt].Concat(count.GetSpan().ToArray()).Concat(content[(countAt + 1)..])
            .ToArray();
        var damaged = files.AddFile("kvi");
        var writer = new MemWriter(damaged.GetAppenderWriter());
        writer.WriteBlock(damagedContent);
        writer.Flush();
        index.Remove();
        var before = GC.GetTotalAllocatedBytes(true);
        using (var db = Open(files))
        {
            using var tr = db.StartReadOnlyTransaction();
            Assert.Equal(30, tr.GetKeyValueCount());
        }

        Assert.InRange(GC.GetTotalAllocatedBytes(true) - before, 0, 64 << 20);
    }

    [Fact]
    public void ReadFileInfoHandlesAllHeaderVariants()
    {
        using var files = new InMemoryFileCollection();
        using (var db = Open(files, 1024))
        {
            for (byte i = 0; i < 30; i++) WriteKey(db, i);
            db.CreateKvi(default);
        }

        foreach (var file in files.Enumerate())
        {
            var info = FileCollectionWithFileInfos.ReadFileInfo(file);
            Assert.NotEqual(KVFileType.Unknown, info.FileType);
            var size = file.GetSize();
            var content = new byte[size];
            file.RandomRead(content, 0, false);
            var reader = MemReader.CreateFromReadOnlyMemory(content);
            Assert.Equal(info.FileType, FileCollectionWithFileInfos.ReadFileInfo(ref reader).FileType);
        }

        Assert.Contains(files.Enumerate(),
            f => FileCollectionWithFileInfos.ReadFileInfo(f).FileType == KVFileType.KeyIndex);

        var garbage = "XXXXXX"u8.ToArray();
        var r1 = MemReader.CreateFromReadOnlyMemory(garbage);
        Assert.Equal(KVFileType.Unknown, FileCollectionWithFileInfos.ReadFileInfo(ref r1).FileType);
        var truncated = "BTDB3"u8.ToArray();
        var r2 = MemReader.CreateFromReadOnlyMemory(truncated);
        Assert.Equal(KVFileType.Unknown, FileCollectionWithFileInfos.ReadFileInfo(ref r2).FileType);
        Assert.Throws<EndOfStreamException>(() =>
        {
            var r3 = MemReader.CreateFromReadOnlyMemory(truncated);
            FileCollectionWithFileInfos.ReadFileInfo(ref r3, true);
        });
        byte[] unknownType = [.. "BTDB2"u8, 100];
        var r4 = MemReader.CreateFromReadOnlyMemory(unknownType);
        Assert.Equal(KVFileType.Unknown, FileCollectionWithFileInfos.ReadFileInfo(ref r4).FileType);
    }

    readonly ThreadLocal<HashAlgorithm> _sha1 = new(SHA1.Create);

    ByteBuffer Hash(byte[] content) => ByteBuffer.NewAsync(_sha1.Value!.ComputeHash(content));

    [Fact]
    public async Task ChunkStoragePersistsLeafFlagsAndRollsFiles()
    {
        using var files = new InMemoryFileCollection();
        var contents = Enumerable.Range(0, 10).Select(i =>
        {
            var c = new byte[600 + i];
            new Random(i).NextBytes(c);
            return c;
        }).ToArray();
        using (var db = Open(files, 1024))
        {
            var cs = db.GetSubDB<IChunkStorage>(1)!;
            Assert.Same(cs, db.GetSubDB<IChunkStorage>(1));
            Assert.Throws<ArgumentException>(() => db.GetSubDB<string>(1));
            using (var tr = cs.StartTransaction())
            {
                foreach (var c in contents) tr.Put(Hash(c), ByteBuffer.NewAsync(c), true);
                // Same content again as non leaf updates only flag
                tr.Put(Hash(contents[0]), ByteBuffer.NewAsync(contents[0]), false);
                tr.Put(Hash(contents[0]), ByteBuffer.NewAsync(contents[0]), false);
                tr.Put(Hash(contents[1]), ByteBuffer.NewAsync(contents[1]), true);
                Assert.Throws<InvalidOperationException>(() =>
                    tr.Put(Hash(contents[2]), ByteBuffer.NewAsync(new byte[3]), true));
                Assert.Throws<ArgumentException>(() =>
                    tr.Put(ByteBuffer.NewAsync(new byte[21]), ByteBuffer.NewAsync(contents[2]), true));
                Assert.Throws<ArgumentException>(() => { _ = tr.Get(ByteBuffer.NewAsync(new byte[19])); });
                Assert.Equal(contents[5], (await tr.Get(Hash(contents[5]))).ToByteArray());
                Assert.Equal(0, (await tr.Get(Hash([1, 2, 3]))).Length);
            }
        }

        Assert.True(files.Enumerate().Count(f => FileCollectionWithFileInfos.ReadFileInfo(f).FileType ==
                                                  KVFileType.PureValuesWithId) > 1);
        using (var db = Open(files, 1024))
        {
            var cs = db.GetSubDB<IChunkStorage>(1)!;
            using var tr = cs.StartTransaction();
            foreach (var c in contents) Assert.Equal(c, (await tr.Get(Hash(c))).ToByteArray());
            Assert.Throws<ArgumentException>(() => { _ = tr.Get(ByteBuffer.NewAsync(new byte[19])); });
        }
    }
}
