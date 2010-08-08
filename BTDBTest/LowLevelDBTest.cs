using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using BTDB;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class LowLevelDBTest
    {
        [Test]
        public void CreateEmptyDatabase()
        {
            using (var stream = CreateTestStream())
            {
                using (ILowLevelDB db = new LowLevelDB())
                {
                    Assert.IsTrue(db.Open(stream, false));
                }
            }
        }

        [Test]
        public void OpenEmptyDatabase()
        {
            using (var stream = CreateTestStream())
            {
                using (ILowLevelDB db = new LowLevelDB())
                {
                    Assert.IsTrue(db.Open(stream, false));
                }
                using (ILowLevelDB db = new LowLevelDB())
                {
                    Assert.IsFalse(db.Open(stream, false));
                }
            }
        }

        [Test]
        public void FirstTransaction()
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.CreateKey(_key1));
                    tr.Commit();
                }
            }
        }

        [Test]
        public void MoreComplexTransaction()
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.CreateKey(_key1));
                    Assert.False(tr.CreateKey(_key1));
                    Assert.False(tr.FindExactKey(_key2));
                    Assert.True(tr.CreateKey(_key2));
                    Assert.True(tr.FindExactKey(_key1));
                    Assert.True(tr.FindExactKey(_key2));
                    Assert.False(tr.FindExactKey(_key3));
                    tr.Commit();
                }
            }
        }

        [Test]
        public void CommitWorks()
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    using (var tr2 = db.StartTransaction())
                    {
                        Assert.False(tr2.FindExactKey(_key1));
                    }
                    tr1.Commit();
                }
                using (var tr3 = db.StartTransaction())
                {
                    Assert.True(tr3.FindExactKey(_key1));
                }
            }
        }

        [Test]
        public void RollbackWorks()
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    // Rollback because of missing commit
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.False(tr2.FindExactKey(_key1));
                }
            }
        }

        [Test]
        public void BiggerKey([Values(0, 1, 268, 269, 270, 4364, 4365, 4366, 1200000)] int keyLength)
        {
            var key = new byte[keyLength];
            for (int i = 0; i < keyLength; i++) key[i] = (byte)i;
            using (var stream = CreateTestStream())
            {
                using (ILowLevelDB db = new LowLevelDB())
                {
                    db.Open(stream, false);
                    using (var tr1 = db.StartTransaction())
                    {
                        tr1.CreateKey(key);
                        tr1.Commit();
                    }
                    using (var tr2 = db.StartTransaction())
                    {
                        Assert.True(tr2.FindExactKey(key));
                        var buf = tr2.ReadKey();
                        Assert.AreEqual(key, buf);
                        Debug.WriteLine(tr2.CalculateStats().ToString());
                    }
                }
                Debug.WriteLine("KeySize:{0,7} DataBaseSize:{1,7}", keyLength, stream.GetSize());
            }
        }

        [Test]
        public void TwoTransactions()
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    tr2.CreateKey(_key2);
                    Assert.True(tr2.FindExactKey(_key1));
                    Assert.True(tr2.FindExactKey(_key2));
                    Assert.False(tr2.FindExactKey(_key3));
                    tr2.Commit();
                }
                using (var tr3 = db.StartTransaction())
                {
                    Assert.True(tr3.FindExactKey(_key1));
                    Assert.True(tr3.FindExactKey(_key2));
                    Assert.False(tr3.FindExactKey(_key3));
                }
            }
        }

        [Test]
        public void MultipleTransactions([Values(1000)] int transactionCount)
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                var key = new byte[2];
                for (int i = 0; i < transactionCount; i++)
                {
                    key[0] = (byte)(i / 256);
                    key[1] = (byte)(i % 256);
                    using (var tr1 = db.StartTransaction())
                    {
                        tr1.CreateKey(key);
                        if (i % 100 == 0 || i == transactionCount - 1)
                        {
                            for (int j = 0; j < i; j++)
                            {
                                key[0] = (byte)(j / 256);
                                key[1] = (byte)(j % 256);
                                Assert.True(tr1.FindExactKey(key));
                            }
                        }
                        tr1.Commit();
                    }
                }
            }
        }

        [Test]
        public void MultipleTransactions2([Values(1000)] int transactionCount)
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                var key = new byte[2];
                for (int i = 0; i < transactionCount; i++)
                {
                    key[0] = (byte)((transactionCount - i) / 256);
                    key[1] = (byte)((transactionCount - i) % 256);
                    using (var tr1 = db.StartTransaction())
                    {
                        tr1.CreateKey(key);
                        if (i % 100 == 0 || i == transactionCount - 1)
                        {
                            for (int j = 0; j < i; j++)
                            {
                                key[0] = (byte)((transactionCount - j) / 256);
                                key[1] = (byte)((transactionCount - j) % 256);
                                Assert.True(tr1.FindExactKey(key));
                            }
                        }
                        tr1.Commit();
                    }
                }
            }
        }

        [Test]
        public void SimpleFindPreviousKeyWorks()
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    tr1.CreateKey(_key2);
                    tr1.CreateKey(_key3);
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.True(tr2.FindExactKey(_key3));
                    Assert.True(tr2.FindPreviousKey());
                    Assert.AreEqual(_key1, tr2.ReadKey());
                    Assert.False(tr2.FindPreviousKey());
                }
            }
        }

        [Test]
        public void SimpleFindNextKeyWorks()
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    tr1.CreateKey(_key2);
                    tr1.CreateKey(_key3);
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.True(tr2.FindExactKey(_key3));
                    Assert.True(tr2.FindNextKey());
                    Assert.AreEqual(_key2, tr2.ReadKey());
                    Assert.False(tr2.FindNextKey());
                }
            }
        }

        [Test]
        public void AdvancedFindPreviousAndNextKeyWorks()
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                var key = new byte[2];
                const int keysCreated = 10000;
                using (var tr = db.StartTransaction())
                {
                    for (int i = 0; i < keysCreated; i++)
                    {
                        key[0] = (byte)(i / 256);
                        key[1] = (byte)(i % 256);
                        tr.CreateKey(key);
                    }
                    tr.Commit();
                }
                using (var tr = db.StartTransaction())
                {
                    Assert.AreEqual(-1, tr.GetKeyIndex());
                    tr.FindExactKey(key);
                    Assert.AreEqual(keysCreated - 1, tr.GetKeyIndex());
                    for (int i = 1; i < keysCreated; i++)
                    {
                        Console.WriteLine(keysCreated - 1 - i);
                        Assert.True(tr.FindPreviousKey());
                        Assert.AreEqual(keysCreated - 1 - i, tr.GetKeyIndex());
                    }
                    Assert.False(tr.FindPreviousKey());
                    Assert.AreEqual(0, tr.GetKeyIndex());
                    for (int i = 1; i < keysCreated; i++)
                    {
                        Assert.True(tr.FindNextKey());
                        Assert.AreEqual(i, tr.GetKeyIndex());
                    }
                    Assert.False(tr.FindNextKey());
                    Assert.AreEqual(keysCreated - 1, tr.GetKeyIndex());
                }
            }
        }

        [Test]
        public void ValueStoreWorks([Values(0, 1, 256, 268, 269, 270, 512, 4364, 4365, 4366, 5000, 1200000, 1200012)] int firstLength,
            [Values(0, 1, 256, 268, 269, 270, 512, 4364, 4365, 4366, 5000, 1200000, 1200012)] int secondLength)
        {
            var valbuf = new byte[firstLength];
            new Random(0).NextBytes(valbuf);
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    tr1.CreateKey(_key2);
                    tr1.CreateKey(_key3);
                    tr1.WriteValue(0, valbuf.Length, valbuf, 0);
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.True(tr2.FindExactKey(_key1));
                    Assert.True(tr2.FindExactKey(_key2));
                    Assert.True(tr2.FindExactKey(_key3));
                    tr2.SetValueSize(secondLength);
                    tr2.Commit();
                }
                using (var tr3 = db.StartTransaction())
                {
                    Assert.True(tr3.FindExactKey(_key1));
                    Assert.True(tr3.FindExactKey(_key2));
                    Assert.True(tr3.FindExactKey(_key3));
                    var valbuf2 = tr3.ReadValue();
                    for (int i = 0; i < Math.Min(firstLength, secondLength); i++)
                    {
                        Assert.AreEqual(valbuf[i], valbuf2[i]);
                    }
                    for (int i = Math.Min(firstLength, secondLength); i < secondLength; i++)
                    {
                        Assert.AreEqual(0, valbuf2[i]);
                    }
                }
            }
        }

        [Test]
        public void FindFirstKeyWorks()
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr = db.StartTransaction())
                {
                    Assert.False(tr.FindFirstKey());
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.CreateKey(_key3);
                    Assert.True(tr.FindFirstKey());
                    Assert.AreEqual(_key1, tr.ReadKey());
                    tr.Commit();
                }
            }
        }

        [Test]
        public void FindLastKeyWorks()
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr = db.StartTransaction())
                {
                    Assert.False(tr.FindLastKey());
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.CreateKey(_key3);
                    Assert.True(tr.FindLastKey());
                    Assert.AreEqual(_key2, tr.ReadKey());
                    tr.Commit();
                }
            }
        }

        [Test]
        public void SimplePrefixWorks()
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.CreateKey(_key3);
                    Assert.AreEqual(3, tr.GetKeyValueCount());
                    tr.SetKeyPrefix(_key1, 0, 3);
                    Assert.AreEqual(2, tr.GetKeyValueCount());
                    tr.FindFirstKey();
                    Assert.AreEqual(new byte[0], tr.ReadKey());
                    tr.FindLastKey();
                    Assert.AreEqual(_key3.Skip(3).ToArray(),tr.ReadKey());
                    tr.Commit();
                }
            }
        }

        readonly byte[] _key1 = new byte[] { 1, 2, 3 };
        readonly byte[] _key2 = new byte[] { 1, 3, 2 };
        readonly byte[] _key3 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

        static IStream CreateTestStream(bool log = false)
        {
            if (log)
            {
                return new LoggingStream(new StreamProxy(new MemoryStream(), true), true, LogDebug);
            }
            return new StreamProxy(new MemoryStream(), true);
        }

        static void LogDebug(string s)
        {
            Debug.WriteLine(s);
        }
    }
}
