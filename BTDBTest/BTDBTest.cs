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
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, Nothing))
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
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, Nothing))
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
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, Nothing))
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr = db.StartTransaction())
                {
                    Assert.AreEqual(true, tr.CreateKey(_key1));
                    tr.Commit();
                }
            }
        }

        [Test]
        public void MoreComplexTransaction()
        {
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, Nothing))
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr = db.StartTransaction())
                {
                    Assert.AreEqual(true, tr.CreateKey(_key1));
                    Assert.AreEqual(false, tr.CreateKey(_key1));
                    Assert.AreEqual(false, tr.FindExactKey(_key2));
                    Assert.AreEqual(true, tr.CreateKey(_key2));
                    Assert.AreEqual(true, tr.FindExactKey(_key1));
                    Assert.AreEqual(true, tr.FindExactKey(_key2));
                    Assert.AreEqual(false, tr.FindExactKey(_key3));
                    tr.Commit();
                }
            }
        }

        [Test]
        public void CommitWorks()
        {
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, Nothing))
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    using (var tr2 = db.StartTransaction())
                    {
                        Assert.AreEqual(false, tr2.FindExactKey(_key1));
                    }
                    tr1.Commit();
                }
                using (var tr3 = db.StartTransaction())
                {
                    Assert.AreEqual(true, tr3.FindExactKey(_key1));
                }
            }
        }

        [Test]
        public void RollbackWorks()
        {
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, Nothing))
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
                    Assert.AreEqual(false, tr2.FindExactKey(_key1));
                }
            }
        }

        [Test]
        public void BiggerKey([Values(0, 1, 268, 269, 270, 4364, 4365, 4366, 1200000)] int keyLength)
        {
            var key = new byte[keyLength];
            var buf = new byte[keyLength];
            for (int i = 0; i < keyLength; i++) key[i] = (byte)i;
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, Nothing))
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
                        Assert.AreEqual(true, tr2.FindExactKey(key));
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
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, LogDebug))
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
                    Assert.AreEqual(true, tr2.FindExactKey(_key1));
                    Assert.AreEqual(true, tr2.FindExactKey(_key2));
                    Assert.AreEqual(false, tr2.FindExactKey(_key3));
                    tr2.Commit();
                }
                using (var tr3 = db.StartTransaction())
                {
                    Assert.AreEqual(true, tr3.FindExactKey(_key1));
                    Assert.AreEqual(true, tr3.FindExactKey(_key2));
                    Assert.AreEqual(false, tr3.FindExactKey(_key3));
                }
            }
        }

        readonly byte[] _key1 = new byte[] { 1, 2, 3 };
        readonly byte[] _key2 = new byte[] { 1, 3, 2 };
        readonly byte[] _key3 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

        static void Nothing(string s)
        {
        }

        static void LogDebug(string s)
        {
            Debug.WriteLine(s);
        }
    }
}
