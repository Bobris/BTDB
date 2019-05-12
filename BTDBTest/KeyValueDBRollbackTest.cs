using System.Threading;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using static BTDBTest.ObjectDbTest;

namespace BTDBTest
{
    public class KeyValueDBRollbackTest
    {
        [Fact]
        public void CanRollback()
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
                        if (i == 50)
                            kv.PreserveHistoryUpToCommitUlong = (ulong)i;
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
                // All commits after 50 are lost
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
                        Assert.Equal(50, tr.GetKeyValueCount());
                    }
                }

                // Openning without long enough preserving in previous open, removed posibility to rollback before it
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 1024,
                    OpenUpToCommitUlong = 45,
                    PreserveHistoryUpToCommitUlong = 80,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    using (var tr = kv.StartTransaction())
                    {
                        Assert.Equal(50, tr.GetKeyValueCount());
                    }
                }
            }
        }

        [Fact]
        public void CannotRollbackTooFar()
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
        public void CanRollbackToStartIfNoTrlMissing()
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
                // Again after open with OpenUpToCommitUlong you lost option to replay old history
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
                        Assert.Equal(0, tr.GetKeyValueCount());
                    }
                }
            }
        }

        [Fact]
        public void CannotRollbackToStartIfAnyTrlMissing()
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

        [Fact]
        public void ComplexTrlRollback()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                var options = new KeyValueDBOptions
                {
                    Compression = new SnappyCompressionStrategy(),
                    FileCollection = fileCollection,
                    FileSplitSize = 100 * 1024 * 1024,
                    OpenUpToCommitUlong = null,
                    PreserveHistoryUpToCommitUlong = null,
                    CompactorScheduler = CompactorScheduler.Instance,
                };

                using (var kvDb = new KeyValueDB(options))
                using (var objDb = new ObjectDB())
                {
                    objDb.Open(kvDb, false);

                    for (ulong i = 0; i < 100; i += 3)
                    {
                        using (var tr = objDb.StartWritingTransaction().Result)
                        {
                            var person = tr.Singleton<Person>();
                            person.Age = (uint)i;
                            tr.Store(person);
                            tr.SetCommitUlong(i);
                            tr.Commit();
                        }
                    }

                }

                options = new KeyValueDBOptions
                {
                    Compression = new SnappyCompressionStrategy(),
                    FileCollection = fileCollection,
                    FileSplitSize = 100 * 1024 * 1024,
                    OpenUpToCommitUlong = 9UL,
                    PreserveHistoryUpToCommitUlong = 9UL,
                    CompactorScheduler = CompactorScheduler.Instance,
                };
                using (var kvDb = new KeyValueDB(options))
                using (var objDb = new ObjectDB())
                {
                    objDb.Open(kvDb, false);

                    using (var tr = objDb.StartReadOnlyTransaction())
                    {
                        Assert.Equal(9UL, tr.GetCommitUlong());
                    }

                    for (ulong i = 10; i < 200; i += 5)
                    {
                        using (var tr = objDb.StartWritingTransaction().Result)
                        {
                            var person = tr.Singleton<Person>();
                            person.Age = (uint)i;
                            tr.Store(person);
                            tr.SetCommitUlong(i);
                            tr.Commit();
                        }
                    }

                }

                options = new KeyValueDBOptions
                {
                    Compression = new SnappyCompressionStrategy(),
                    FileCollection = fileCollection,
                    FileSplitSize = 100 * 1024 * 1024,
                    OpenUpToCommitUlong = 50UL,
                    PreserveHistoryUpToCommitUlong = 50UL,
                    CompactorScheduler = CompactorScheduler.Instance,
                };
                using (var kvDb = new KeyValueDB(options))
                using (var objDb = new ObjectDB())
                {
                    objDb.Open(kvDb, false);

                    using (var tr = objDb.StartReadOnlyTransaction())
                    {
                        Assert.Equal(50UL, tr.GetCommitUlong());
                    }
                }
            }
        }

        [Fact]
        public void ComplexTrlRollbackWhenKviLost()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                var options = new KeyValueDBOptions
                {
                    Compression = new SnappyCompressionStrategy(),
                    FileCollection = fileCollection,
                    FileSplitSize = 100 * 1024 * 1024,
                    OpenUpToCommitUlong = null,
                    PreserveHistoryUpToCommitUlong = null,
                    CompactorScheduler = CompactorScheduler.Instance,
                };

                using (var kvDb = new KeyValueDB(options))
                using (var objDb = new ObjectDB())
                {
                    objDb.Open(kvDb, false);

                    for (ulong i = 0; i < 100; i += 3)
                    {
                        using (var tr = objDb.StartWritingTransaction().Result)
                        {
                            var person = tr.Singleton<Person>();
                            person.Age = (uint)i;
                            tr.Store(person);
                            tr.SetCommitUlong(i);
                            tr.Commit();
                        }
                    }
                }

                options = new KeyValueDBOptions
                {
                    Compression = new SnappyCompressionStrategy(),
                    FileCollection = fileCollection,
                    FileSplitSize = 100 * 1024 * 1024,
                    OpenUpToCommitUlong = 9UL,
                    PreserveHistoryUpToCommitUlong = 9UL,
                    CompactorScheduler = CompactorScheduler.Instance,
                };
                using (var kvDb = new KeyValueDB(options))
                using (var objDb = new ObjectDB())
                {
                    objDb.Open(kvDb, false);

                    using (var tr = objDb.StartReadOnlyTransaction())
                    {
                        Assert.Equal(9UL, tr.GetCommitUlong());
                    }
                }

                // Delete KVI file
                fileCollection.GetFile(3).Remove();

                options = new KeyValueDBOptions
                {
                    Compression = new SnappyCompressionStrategy(),
                    FileCollection = fileCollection,
                    OpenUpToCommitUlong = 9UL,
                    PreserveHistoryUpToCommitUlong = 9UL,
                    FileSplitSize = 100 * 1024 * 1024,
                    CompactorScheduler = CompactorScheduler.Instance,
                };
                using (var kvDb = new KeyValueDB(options))
                using (var objDb = new ObjectDB())
                {
                    objDb.Open(kvDb, false);

                    using (var tr = objDb.StartReadOnlyTransaction())
                    {
                        Assert.Equal(9UL, tr.GetCommitUlong());
                    }
                    for (ulong i = 10; i < 200; i += 5)
                    {
                        using (var tr = objDb.StartWritingTransaction().Result)
                        {
                            var person = tr.Singleton<Person>();
                            person.Age = (uint)i;
                            tr.Store(person);
                            tr.SetCommitUlong(i);
                            tr.Commit();
                        }
                    }
                }

                options = new KeyValueDBOptions
                {
                    Compression = new SnappyCompressionStrategy(),
                    FileCollection = fileCollection,
                    FileSplitSize = 100 * 1024 * 1024,
                    OpenUpToCommitUlong = 50UL,
                    PreserveHistoryUpToCommitUlong = 50UL,
                    CompactorScheduler = CompactorScheduler.Instance,
                };
                using (var kvDb = new KeyValueDB(options))
                using (var objDb = new ObjectDB())
                {
                    objDb.Open(kvDb, false);

                    using (var tr = objDb.StartReadOnlyTransaction())
                    {
                        Assert.Equal(50UL, tr.GetCommitUlong());
                    }
                }
            }
        }

        [Fact]
        public void ReopenAndReopenWithRoolbackDoesNotCorruptDB()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 4096,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    for (var i = 1; i < 60; i++)
                    {
                        using (var tr = kv.StartTransaction())
                        {
                            var key = new byte[4];
                            BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i);
                            tr.CreateOrUpdateKeyValueUnsafe(key, new byte[2000]);
                            tr.SetCommitUlong((ulong)i);
                            tr.Commit();
                        }
                        if (i % 2 == 0)
                        {
                            using (var tr = kv.StartTransaction())
                            {
                                var key = new byte[4];
                                BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i - 1);
                                tr.FindExactKey(key);
                                tr.EraseCurrent();
                                tr.Commit();
                            }
                        }
                        if (i % 5 == 0)
                            kv.Compact(new System.Threading.CancellationToken());
                        if (i == 50) kv.PreserveHistoryUpToCommitUlong = (ulong)i;
                    }
                }
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 4096,
                    PreserveHistoryUpToCommitUlong = 50,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    kv.Compact(new System.Threading.CancellationToken());
                    ReadAllValues(kv);
                }
                using (var kv = new KeyValueDB(new KeyValueDBOptions
                {
                    FileCollection = fileCollection,
                    FileSplitSize = 4096,
                    PreserveHistoryUpToCommitUlong = 50,
                    OpenUpToCommitUlong = 50,
                    Compression = new NoCompressionStrategy()
                }))
                {
                    ReadAllValues(kv);
                }
            }
        }

        void ReadAllValues(IKeyValueDB kv)
        {
            using (var tr = kv.StartTransaction())
            {
                if (tr.FindFirstKey()) do
                    {
                        tr.GetValueAsByteArray();
                    } while (tr.FindNextKey());
            }
        }

        [Fact]
        public void CompatorShouldNotBePesimist()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                var options = new KeyValueDBOptions
                {
                    Compression = new NoCompressionStrategy(),
                    FileCollection = fileCollection,
                    FileSplitSize = 8096,
                    OpenUpToCommitUlong = null,
                    PreserveHistoryUpToCommitUlong = null,
                    CompactorScheduler = CompactorScheduler.Instance,
                };

                using (var kvDb = new KeyValueDB(options))
                {
                    for (var i = 0; i < 100; i++)
                    {
                        using (var tr = kvDb.StartWritingTransaction().Result)
                        {
                            var key = new byte[4];
                            BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i);
                            tr.CreateOrUpdateKeyValueUnsafe(key, new byte[2000]);
                            tr.SetCommitUlong((ulong)i);
                            tr.Commit();
                        }
                    }
                    kvDb.PreserveHistoryUpToCommitUlong = 100;
                    kvDb.Compact(new System.Threading.CancellationToken());
                    var fileCountAfterFirstCompaction = fileCollection.GetCount();
                    for (var i = 0; i < 50; i++)
                    {
                        using (var tr = kvDb.StartWritingTransaction().Result)
                        {
                            var key = new byte[4];
                            BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i);
                            tr.FindExactKey(key);
                            tr.EraseCurrent();
                            tr.SetCommitUlong(100 + (ulong)i);
                            tr.Commit();
                        }
                    }
                    kvDb.PreserveHistoryUpToCommitUlong = 150;
                    kvDb.Compact(new System.Threading.CancellationToken());
                    Assert.InRange(fileCollection.GetCount(), fileCountAfterFirstCompaction + 1, fileCountAfterFirstCompaction + 3);
                    using (var trOlder = kvDb.StartReadOnlyTransaction())
                    {
                        for (var i = 50; i < 100; i++)
                        {
                            using (var tr = kvDb.StartWritingTransaction().Result)
                            {
                                var key = new byte[4];
                                BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i);
                                tr.FindExactKey(key);
                                tr.EraseCurrent();
                                tr.SetCommitUlong(100 + (ulong)i);
                                tr.Commit();
                            }
                        }
                        kvDb.Compact(new System.Threading.CancellationToken());
                        Assert.InRange(fileCollection.GetCount(), fileCountAfterFirstCompaction / 3, 2 * fileCountAfterFirstCompaction / 3);
                        kvDb.PreserveHistoryUpToCommitUlong = 200;
                        kvDb.Compact(new System.Threading.CancellationToken());
                        Assert.InRange(fileCollection.GetCount(), fileCountAfterFirstCompaction / 3, 2 * fileCountAfterFirstCompaction / 3);
                    }
                    kvDb.Compact(new System.Threading.CancellationToken());
                    Assert.InRange<uint>(fileCollection.GetCount(), 1, 4);
                }
            }
        }

        [Fact]
        public void CompatorShouldNotBePesimistDespiteRunningTransactions()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                var options = new KeyValueDBOptions
                {
                    Compression = new NoCompressionStrategy(),
                    FileCollection = fileCollection,
                    FileSplitSize = 8096,
                    CompactorScheduler = CompactorScheduler.Instance,
                };

                using (var kvDb = new KeyValueDB(options))
                {
                    for (var i = 0; i < 100; i++)
                    {
                        using (var tr = kvDb.StartWritingTransaction().Result)
                        {
                            var key = new byte[4];
                            BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i);
                            tr.CreateOrUpdateKeyValueUnsafe(key, new byte[2000]);
                            tr.Commit();
                        }
                    }
                    kvDb.Compact(new System.Threading.CancellationToken());
                    var fileCountAfterFirstCompaction = fileCollection.GetCount();
                    using (var trOlder = kvDb.StartReadOnlyTransaction())
                    {
                        for (var i = 0; i < 50; i++)
                        {
                            using (var tr = kvDb.StartWritingTransaction().Result)
                            {
                                var key = new byte[4];
                                BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i * 2);
                                tr.FindExactKey(key);
                                tr.EraseCurrent();
                                tr.Commit();
                            }
                        }
                        while (kvDb.Compact(new System.Threading.CancellationToken())) ;
                        Assert.InRange(fileCollection.GetCount(), fileCountAfterFirstCompaction + 2, fileCountAfterFirstCompaction + 50);
                    }
                    for (var i = 0; i < 4; i++)
                    {
                        using (var tr = kvDb.StartWritingTransaction().Result)
                        {
                            var key = new byte[4];
                            BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i);
                            tr.CreateOrUpdateKeyValueUnsafe(key, new byte[2000]);
                            tr.Commit();
                        }
                    }
                    while (kvDb.Compact(new System.Threading.CancellationToken())) ;
                    Assert.InRange(fileCollection.GetCount(), fileCountAfterFirstCompaction / 3, 2 * fileCountAfterFirstCompaction / 3);
                }
            }
        }

        [Fact]
        public void CanOpenDbAfterDeletingAndCompacting()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                var options = new KeyValueDBOptions
                {
                    Compression = new NoCompressionStrategy(),
                    FileCollection = fileCollection,
                    FileSplitSize = 4096,
                    OpenUpToCommitUlong = null,
                    PreserveHistoryUpToCommitUlong = null,
                    CompactorScheduler = null,
                };

                using (var kvDb = new KeyValueDB(options))
                {
                    using (var tr = kvDb.StartWritingTransaction().Result)
                    {
                        tr.CreateOrUpdateKeyValue(new byte[5], new byte[3000]);
                        tr.CreateOrUpdateKeyValue(new byte[6], new byte[2000]);
                        tr.Commit();
                    }

                    kvDb.Compact(CancellationToken.None);
                }
                using (var kvDb = new KeyValueDB(options))
                {
                    using (var tr = kvDb.StartWritingTransaction().Result)
                    {
                        tr.FindFirstKey();
                        tr.EraseCurrent();
                        tr.Commit();
                    }

                    kvDb.Compact(CancellationToken.None);
                }

                using (var kvDb = new KeyValueDB(options))
                {
                    // If there is error in KVI 3 it will create new KVI 4, but there is no problem in KVI 3
                    Assert.Null(kvDb.FileCollection.FileInfoByIdx(4));
                }
            }
        }

    }
}
