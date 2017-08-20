using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest
{
    public class KeyValueDBRollbackTest
    {
        [Fact]
        public void CanRoolback()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 1024,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    for (var i = 1; i < 100; i++)
                    {
                        using (var tr = kv.StartTransaction())
                        {
                            var key = new byte[4];
                            BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i);
                            tr.CreateOrUpdateKeyValueUnsafe(key, new byte[200]);
                            tr.CreateOrUpdateKeyValueUnsafe(key, new byte[0]);
                            tr.SetCommitUlong((ulong)i);
                            tr.Commit();
                        }
                        if (i % 5 == 0)
                            kv.Compact(new System.Threading.CancellationToken());
                        if (i == 50) kv.PreserveHistoryUpToCommitUlong = (ulong)i;
                    }
                }
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 1024,
                    OpenUpToCommitUlong = 50,
                    PreserveHistoryUpToCommitUlong = 80,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    using (var tr = kv.StartTransaction())
                    {
                        Assert.Equal(50, tr.GetKeyValueCount());
                    }
                    kv.Compact(new System.Threading.CancellationToken());
                }
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 1024,
                    OpenUpToCommitUlong = 80,
                    PreserveHistoryUpToCommitUlong = 80,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    using (var tr = kv.StartTransaction())
                    {
                        Assert.Equal(80, tr.GetKeyValueCount());
                    }
                }

                // Openning without long enough preserving in previous open, removed posibility to rollback before it
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 1024,
                    OpenUpToCommitUlong = 50,
                    PreserveHistoryUpToCommitUlong = 80,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    using (var tr = kv.StartTransaction())
                    {
                        Assert.Equal(80, tr.GetKeyValueCount());
                    }
                }
            }
        }

        [Fact]
        public void CannotRoolbackTooFar()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 1024,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    for (var i = 1; i < 100; i++)
                    {
                        using (var tr = kv.StartTransaction())
                        {
                            var key = new byte[4];
                            BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i);
                            tr.CreateOrUpdateKeyValueUnsafe(key, new byte[200]);
                            tr.CreateOrUpdateKeyValueUnsafe(key, new byte[0]);
                            tr.SetCommitUlong((ulong)i);
                            tr.Commit();
                        }
                        if (i % 5 == 0)
                            kv.Compact(new System.Threading.CancellationToken());
                        if (i == 50) kv.PreserveHistoryUpToCommitUlong = (ulong)i;
                    }
                }
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 1024,
                    OpenUpToCommitUlong = 40,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    using (var tr = kv.StartTransaction())
                    {
                        Assert.InRange(tr.GetKeyValueCount(), 41, 50);
                    }
                }
            }
        }

        [Fact]
        public void CanRoolbackToStartIfNoTrlMissing()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 1024,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    for (var i = 1; i < 60; i++)
                    {
                        using (var tr = kv.StartTransaction())
                        {
                            var key = new byte[4];
                            BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i);
                            tr.CreateOrUpdateKeyValueUnsafe(key, new byte[200]);
                            tr.SetCommitUlong((ulong)i);
                            tr.Commit();
                        }
                        if (i % 5 == 0)
                            kv.Compact(new System.Threading.CancellationToken());
                        if (i == 50) kv.PreserveHistoryUpToCommitUlong = (ulong)i;
                    }
                }
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 1024,
                    OpenUpToCommitUlong = 0,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    using (var tr = kv.StartTransaction())
                    {
                        Assert.Equal(0, tr.GetKeyValueCount());
                    }
                }
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 1024,
                    OpenUpToCommitUlong = 1,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    using (var tr = kv.StartTransaction())
                    {
                        Assert.Equal(1, tr.GetKeyValueCount());
                    }
                }
            }
        }

        [Fact]
        public void CannotRoolbackToStartIfAnyTrlMissing()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 1024,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    for (var i = 1; i < 100; i++)
                    {
                        using (var tr = kv.StartTransaction())
                        {
                            var key = new byte[4];
                            BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i);
                            tr.CreateOrUpdateKeyValueUnsafe(key, new byte[200]);
                            tr.CreateOrUpdateKeyValueUnsafe(key, new byte[0]);
                            tr.SetCommitUlong((ulong)i);
                            tr.Commit();
                        }
                        if (i % 5 == 0)
                            kv.Compact(new System.Threading.CancellationToken());
                        if (i == 50) kv.PreserveHistoryUpToCommitUlong = (ulong)i;
                    }
                }
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 1024,
                    OpenUpToCommitUlong = 0,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    using (var tr = kv.StartTransaction())
                    {
                        Assert.InRange(tr.GetKeyValueCount(), 41, 50);
                    }
                }
            }
        }
    }
}
