using System.Diagnostics;
using System.IO;
using BTDB;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class BTDBTest
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
            var buf = new byte[keyLength];
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
                        tr2.ReadKey(0, keyLength, buf, 0);
                        Assert.AreEqual(key, buf);
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
        public void MultipleTransactions([Values(100)] int transactionCount)
        {
            using (var stream = CreateTestStream())
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                var key = new byte[1];
                for (int i = 0; i < transactionCount; i++)
                {
                    key[0] = (byte)i;
                    using (var tr1 = db.StartTransaction())
                    {
                        tr1.CreateKey(key);
                        tr1.Commit();
                    }
                }
                using (var tr2 = db.StartTransaction())
                {
                    for (int i = 0; i < transactionCount; i++)
                    {
                        key[0] = (byte) i;
                        Assert.True(tr2.FindExactKey(key));
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
                    Assert.AreEqual(_key1,tr2.ReadKey());
                    Assert.False(tr2.FindPreviousKey());
                }
            }
        }
       
        readonly byte[] _key1 = new byte[] { 1, 2, 3 };
        readonly byte[] _key2 = new byte[] { 1, 3, 2 };
        readonly byte[] _key3 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

        static IStream CreateTestStream(bool log=false)
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
