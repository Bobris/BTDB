using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using LightningDB;

namespace DBBenchmark
{
    public class LightingDbTimeTests : IDbTimeTests
    {
        readonly string _dBpath = Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory())!.FullName, "TestDb", Guid.NewGuid().ToString());

        string DbDBpath
        {
            get
            {
                if (!Directory.Exists(_dBpath))
                {
                    Directory.CreateDirectory(_dBpath);
                }

                return _dBpath;
            }
        }

        readonly EnvironmentConfiguration _environmentConfiguration = new EnvironmentConfiguration
        {
            MapSize = 10L * 1024 * 1024 * 1024
        };

        LightningEnvironment? _environment;

        LightningEnvironment Environment => _environment ??= new LightningEnvironment(DbDBpath, _environmentConfiguration);


        public LightingDbTimeTests()
        {
            Environment.Open();
        }

        public void Dispose()
        {
            Environment.Dispose();
            Directory.Delete(DbDBpath, true);
        }

        public (TimeSpan openTime, long memorySize) Open()
        {
            Environment.Dispose();
            _environment = null;

            GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
            var memStart = GC.GetTotalMemory(true);

            var stopwatch = Stopwatch.StartNew();

            Environment.Open();

            using (var tx = Environment.BeginTransaction())
            using (tx.OpenDatabase(configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.None }))
            {
                stopwatch.Stop();

                GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
                var memFinish = GC.GetTotalMemory(false);
                var memDif = (memFinish - memStart);

                return (openTime: stopwatch.Elapsed, memorySize: memDif / 1024);
            }
        }

        public TimeSpan Insert(byte[] key, byte[] value)
        {
            var stopwatch = Stopwatch.StartNew();

            using (var tx = Environment.BeginTransaction(TransactionBeginFlags.NoSync))
            using (var db = tx.OpenDatabase(configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create }))
            {
                tx.Put(db, key, value);
                tx.Commit();
            }

            stopwatch.Stop();

            return stopwatch.Elapsed;
        }

        public TimeSpan InsertRange(Dictionary<byte[], byte[]> data)
        {
            var stopwatch = Stopwatch.StartNew();

            using (var tx = Environment.BeginTransaction(TransactionBeginFlags.NoSync))
            using (var db = tx.OpenDatabase(configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create }))
            {
                foreach (var keyValue in data)
                {
                    tx.Put(db, keyValue.Key, keyValue.Value);
                }

                tx.Commit();

            }
            stopwatch.Stop();

            return stopwatch.Elapsed;
        }

        public TimeSpan InsertRangeCommitByItem(Dictionary<byte[], byte[]> data)
        {
            var stopwatch = Stopwatch.StartNew();

            foreach (var keyValue in data)
            {
                using (var tx = Environment.BeginTransaction(TransactionBeginFlags.NoSync)) // Transaction is closed after commit
                using (var db = tx.OpenDatabase(configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create }))
                {
                    tx.Put(db, keyValue.Key, keyValue.Value);
                    tx.Commit();
                }
            }

            stopwatch.Stop();

            return stopwatch.Elapsed;
        }

        public TimeSpan Read(byte[] key)
        {
            var stopwatch = Stopwatch.StartNew();

            using (var tx = Environment.BeginTransaction(TransactionBeginFlags.ReadOnly))
            using (var db = tx.OpenDatabase())
            {
                tx.Get(db, key);
            }

            stopwatch.Stop();

            return stopwatch.Elapsed;
        }

        public TimeSpan ReadValues(IEnumerable<byte[]> keys)
        {
            var stopwatch = Stopwatch.StartNew();

            using (var tx = Environment.BeginTransaction(TransactionBeginFlags.ReadOnly))
            using (var db = tx.OpenDatabase())
            {
                foreach (var key in keys)
                {
                    tx.Get(db, key);
                }
            }

            stopwatch.Stop();
            return stopwatch.Elapsed;
        }

        public TimeSpan ReadAll(Dictionary<byte[], byte[]> exceptedData)
        {
            var stopwatch = Stopwatch.StartNew();

            using (var tx = Environment.BeginTransaction(TransactionBeginFlags.ReadOnly))
            using (var db = tx.OpenDatabase())
            {
                var cursor = tx.CreateCursor(db);

                foreach (var data in exceptedData)
                {
                    var key = cursor.MoveTo(data.Key);
                    if (!key) throw new Exception("Key not found");

                    var value = cursor.GetCurrent().Value;

                    if (value.Length != data.Value.Length) throw new Exception("value length different");

                    for (var j = 0; j < value.Length; j++)
                    {
                        if (value[j] != data.Value[j]) throw new Exception("value different");
                    }
                }

            }

            stopwatch.Stop();
            return stopwatch.Elapsed;
        }

        public TimeSpan Delete(byte[] key)
        {
            var stopwatch = Stopwatch.StartNew();

            using (var tx = Environment.BeginTransaction(TransactionBeginFlags.NoSync))
            using (var db = tx.OpenDatabase(configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.None }))
            {
                tx.Delete(db, key);
                tx.Commit();
            }

            stopwatch.Stop();

            return stopwatch.Elapsed;
        }

        public TimeSpan DeleteAll()
        {
            var stopwatch = Stopwatch.StartNew();

            using (var tx = Environment.BeginTransaction(TransactionBeginFlags.NoSync))
            using (var db = tx.OpenDatabase(configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.None }))
            {
                tx.TruncateDatabase(db);
                tx.Commit();
            }

            stopwatch.Stop();

            return stopwatch.Elapsed;
        }
    }
}