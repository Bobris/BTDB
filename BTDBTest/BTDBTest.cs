using System;
using System.IO;
using BTDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace BTDBTest
{
    [TestClass]
    public class BTDBTest
    {
        private static void Nothing(string s)
        {
        }

        [TestMethod]
        public void CreateEmptyDatabase()
        {
            using(var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, Nothing))
            {
                using (ILowLevelDB db = new LowLevelDB())
                {
                    Assert.IsTrue(db.Open(stream, false));
                }
            }
        }
        [TestMethod]
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
    }
}


