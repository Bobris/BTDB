using BTDB.Buffer;
using BTDB.KVDBLayer;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using BTDB.ODBLayer;

namespace SimpleTester
{
    public enum KVType
    {
        Managed,
        BTree
    }

    public class KeyValueSpeedTest
    {
        readonly Stopwatch _sw = new Stopwatch();
        readonly bool _inMemory;
        readonly bool _memoryMapped;
        IFileCollection _fileCollection;
        readonly bool _fastInMemory;
        readonly KVType _kvType;

        public KeyValueSpeedTest(KVType kvType, bool inMemory, bool memoryMapped)
        {
            _kvType = kvType;
            _inMemory = inMemory;
            _memoryMapped = memoryMapped;
        }

        public KeyValueSpeedTest(KVType kvType)
        {
            _kvType = kvType;
            _fastInMemory = true;
        }

        IFileCollection CreateTestFileCollection()
        {
            if (_fastInMemory)
                return null;
            if (_inMemory)
            {
                _fileCollection = new InMemoryFileCollection();
                return _fileCollection;
            }

            const string dbfilename = "data";
            if (Directory.Exists(dbfilename))
                Directory.Delete(dbfilename, true);
            Directory.CreateDirectory(dbfilename);
            if (_memoryMapped)
                return new OnDiskMemoryMappedFileCollection(dbfilename);
            return new OnDiskFileCollection(dbfilename);
        }

        IFileCollection OpenTestFileCollection()
        {
            if (_fastInMemory)
                return null;
            if (_inMemory)
            {
                return _fileCollection;
            }

            const string dbfilename = "data";
            if (_memoryMapped)
                return new OnDiskMemoryMappedFileCollection(dbfilename);
            return new OnDiskFileCollection(dbfilename);
        }

        void DoWork5(bool alsoDoReads)
        {
            _sw.Restart();
            long pureDataLength = 0;
            using (var fileCollection = CreateTestFileCollection())
            using (var db = CreateKeyValueDB(fileCollection))
            {
                for (int i = 0; i < 200; i++)
                {
                    long pureDataLengthPrevTr = pureDataLength;
                    using (var tr = db.StartTransaction())
                    {
                        for (int j = 0; j < 200; j++)
                        {
                            tr.CreateOrUpdateKeyValue(new[] {(byte) j, (byte) i}, new byte[1 + i * j]);
                            pureDataLength += 2 + 1 + i * j;
                        }

                        if (alsoDoReads)
                            using (var trCheck = db.StartTransaction())
                            {
                                long pureDataLengthCheck = 0;
                                while (trCheck.FindNextKey())
                                {
                                    pureDataLengthCheck +=
                                        trCheck.GetKey().Length + trCheck.GetValueAsReadOnlySpan().Length;
                                }

                                if (pureDataLengthCheck != pureDataLengthPrevTr)
                                {
                                    throw new Exception("Transactions are not in serializable mode");
                                }
                            }

                        tr.Commit();
                    }
                }
            }

            _sw.Stop();
            Console.WriteLine("Pure data length: {0}", pureDataLength);
            Console.WriteLine("Time: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
        }

        IKeyValueDB CreateKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compressionStrategy = null)
        {
            if (fileCollection == null)
            {
                switch (_kvType)
                {
                    case KVType.Managed:
                        return new InMemoryKeyValueDB();
                    default:
                        throw new NotImplementedException();
                }
            }

            switch (_kvType)
            {
                case KVType.Managed:
                    if (compressionStrategy == null)
                        return new KeyValueDB(fileCollection);
                    return new KeyValueDB(fileCollection, compressionStrategy);
                case KVType.BTree:
                    if (compressionStrategy == null)
                        return new BTreeKeyValueDB(fileCollection);
                    return new BTreeKeyValueDB(fileCollection, compressionStrategy);
                default:
                    throw new NotImplementedException();
            }
        }

        void DoWork5ReadCheck()
        {
            _sw.Restart();
            using (var fileCollection = new OnDiskFileCollection("data"))
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                using (var trCheck = db.StartTransaction())
                {
                    long pureDataLengthCheck = 0;
                    while (trCheck.FindNextKey())
                    {
                        pureDataLengthCheck += trCheck.GetKey().Length + trCheck.GetValue().Length;
                    }

                    if (pureDataLengthCheck != 396130000)
                    {
                        throw new Exception("Bug");
                    }
                }
            }

            _sw.Stop();
            Console.WriteLine("Time: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
        }

        public void HugeTest()
        {
            const int keyCount = 100;
            using (var fileCollection = CreateTestFileCollection())
            {
                _sw.Start();
                using (var db = CreateKeyValueDB(fileCollection, new NoCompressionStrategy()))
                {
                    var key = new byte[100];
                    var value = new byte[100000000];
                    for (int i = 0; i < keyCount; i++)
                    {
                        using (var tr = db.StartTransaction())
                        {
                            key[0] = (byte) (i / 100);
                            key[1] = (byte) (i % 100);
                            value[100] = (byte) (i / 100);
                            value[200] = (byte) (i % 100);
                            tr.CreateOrUpdateKeyValue(key, value);
                            tr.Commit();
                        }
                    }
                }

                _sw.Stop();
                Console.WriteLine("Time to create 10GB DB: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
                _sw.Restart();
                using (IKeyValueDB db = new KeyValueDB(fileCollection))
                {
                    _sw.Stop();
                    Console.WriteLine("Time to open 10GB DB: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
                    _sw.Restart();
                    var key = new byte[100];
                    for (int i = 0; i < keyCount; i++)
                    {
                        using (var tr = db.StartTransaction())
                        {
                            key[0] = (byte) (i / 100);
                            key[1] = (byte) (i % 100);
                            tr.FindExactKey(key);
                            var value = tr.GetValueAsByteArray();
                            if (value[100] != (byte) (i / 100)) throw new InvalidDataException();
                            if (value[200] != (byte) (i % 100)) throw new InvalidDataException();
                        }
                    }

                    _sw.Stop();
                    Console.WriteLine("Time to read all values 10GB DB: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
                    Console.WriteLine(db.CalcStats());
                }

                _sw.Restart();
                using (var db = CreateKeyValueDB(fileCollection))
                {
                    _sw.Stop();
                    Console.WriteLine("Time to open2 10GB DB: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
                    _sw.Restart();
                    var key = new byte[100];
                    for (int i = 0; i < keyCount; i++)
                    {
                        using (var tr = db.StartTransaction())
                        {
                            key[0] = (byte) (i / 100);
                            key[1] = (byte) (i % 100);
                            tr.FindExactKey(key);
                            var value = tr.GetValueAsByteArray();
                            if (value[100] != (byte) (i / 100)) throw new InvalidDataException();
                            if (value[200] != (byte) (i % 100)) throw new InvalidDataException();
                        }
                    }

                    _sw.Stop();
                    Console.WriteLine("Time to read2 all values 10GB DB: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
                    Console.WriteLine(db.CalcStats());
                }
            }
        }

        void CreateTestDB(int keys)
        {
            var rnd = new Random(1234);
            using (var fileCollection = CreateTestFileCollection())
            {
                using (var db = CreateKeyValueDB(fileCollection, new NoCompressionStrategy()))
                {
                    using (var tr = db.StartTransaction())
                    {
                        for (int i = 0; i < keys; i++)
                        {
                            var key = new byte[rnd.Next(10, 50)];
                            rnd.NextBytes(key);
                            var value = new byte[rnd.Next(50, 500)];
                            rnd.NextBytes(value);
                            tr.CreateOrUpdateKeyValueUnsafe(key, value);
                        }

                        tr.Commit();
                    }
                }

                using (var db = CreateKeyValueDB(fileCollection))
                {
                }
            }
        }

        void OpenDBSpeedTest()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var memStart = GC.GetTotalMemory(false);
            _sw.Start();
            using (var fileCollection = OpenTestFileCollection())
            {
                using (IKeyValueDB db = CreateKeyValueDB(fileCollection))
                {
                    _sw.Stop();
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    GC.Collect();
                    var memFinish = GC.GetTotalMemory(false);
                    Console.WriteLine("Time to open DB : {0,15}ms Memory: {1}KB {2}KB", _sw.Elapsed.TotalMilliseconds,
                        (memFinish - memStart) / 1024, Process.GetCurrentProcess().WorkingSet64 / 1024);
                    Console.WriteLine(db.CalcStats());
                    _sw.Restart();
                }
            }

            _sw.Stop();
            Console.WriteLine("Time to close DB: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
        }

        void CheckDBTest(int keys)
        {
            var rnd = new Random(1234);
            using (var fileCollection = OpenTestFileCollection())
            {
                using (var db = CreateKeyValueDB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        if (tr.GetKeyValueCount() != keys) throw new Exception("KeyCount does not match");
                        for (var i = 0; i < keys; i++)
                        {
                            var key = new byte[rnd.Next(10, 50)];
                            rnd.NextBytes(key);
                            var value = new byte[rnd.Next(50, 500)];
                            rnd.NextBytes(value);
                            if (!tr.FindExactKey(key)) throw new Exception("Key not found");
                            var value2 = tr.GetValueAsByteArray();
                            if (value.Length != value2.Length) throw new Exception("value length different");
                            for (var j = 0; j < value.Length; j++)
                            {
                                if (value[j] != value2[j]) throw new Exception("value different");
                            }
                        }
                    }
                }
            }
        }

        void CreateKeySequence(int keys)
        {
            var sw = Stopwatch.StartNew();
            var key = new byte[8];
            var value = new byte[10];

            using (var fileCollection = CreateTestFileCollection())
            {
                using (IKeyValueDB db = CreateKeyValueDB(fileCollection, new NoCompressionStrategy()))
                {
                    using (var tr = db.StartTransaction())
                    {
                        for (int i = 0; i < keys; i++)
                        {
                            int o = 0;
                            PackUnpack.PackVUInt(key, ref o, (uint) i);
                            tr.CreateOrUpdateKeyValue(ByteBuffer.NewSync(key, 0, o), ByteBuffer.NewSync(value));
                        }

                        tr.Commit();
                    }
                }
            }

            Console.WriteLine("CreateSequence:" + sw.Elapsed.TotalMilliseconds);
        }

        void CheckKeySequence(int keys)
        {
            var sw = Stopwatch.StartNew();
            var key = new byte[8];

            using (var fileCollection = CreateTestFileCollection())
            {
                using (IKeyValueDB db = CreateKeyValueDB(fileCollection, new NoCompressionStrategy()))
                {
                    using (var tr = db.StartTransaction())
                    {
                        for (int i = 0; i < keys; i++)
                        {
                            int o = 0;
                            PackUnpack.PackVUInt(key, ref o, (uint) i);
                            tr.Find(ByteBuffer.NewSync(key, 0, o));
                        }

                        tr.Commit();
                    }
                }
            }

            Console.WriteLine("CheckSequence:" + sw.Elapsed.TotalMilliseconds);
        }

        void CreateRandomKeySequence(int keys)
        {
            var sw = Stopwatch.StartNew();
            var rnd = new Random(1234);
            var key = new byte[50];
            var value = new byte[500];
            using (var fileCollection = CreateTestFileCollection())
            {
                using (var db = CreateKeyValueDB(fileCollection, new NoCompressionStrategy()))
                {
                    using (var tr = db.StartTransaction())
                    {
                        for (int i = 0; i < keys; i++)
                        {
                            var keyLen = rnd.Next(10, 50);
                            rnd.NextBytes(key.AsSpan(0, keyLen));
                            var valueLen = rnd.Next(50, 500);
                            rnd.NextBytes(value.AsSpan(0, valueLen));
                            tr.CreateOrUpdateKeyValue(key.AsSpan(0, keyLen), value.AsSpan(0, valueLen));
                        }

                        tr.Commit();
                    }

                    Console.WriteLine("CreateSequence:" + sw.Elapsed.TotalMilliseconds + " Mem kb:" +
                                      Process.GetCurrentProcess().WorkingSet64 / 1024);
                }
            }
        }

        public class BtdbTest
        {
            [PrimaryKey(1)] public ulong CompanyId { get; set; }
            [PrimaryKey(2)] public ulong TestId { get; set; }

            [SecondaryKey("Name", IncludePrimaryKeyOrder = 1)]
            public string TestName { get; set; }

            public string Data1 { get; set; }
            public string Data2 { get; set; }
            public string Data3 { get; set; }
        }

        public interface IBtdbTestTable : IRelation<BtdbTest>
        {
            IEnumerator<BtdbTest> FindByName(ulong companyId, string testName);
        }

        void CreateBtdbTestInserts(int count)
        {
            var sw = Stopwatch.StartNew();
            var rnd = new Random(1234);
            using var fileCollection = CreateTestFileCollection();
            using var db = CreateKeyValueDB(fileCollection);
            using var odb = new ObjectDB();
            odb.Open(db,false);
            Func<IObjectDBTransaction, IBtdbTestTable> table;
            using (var tr = odb.StartTransaction())
            {
                 table = tr.InitRelation<IBtdbTestTable>("BtdbTest");
                 tr.Commit();
            }
            for (var i = 0; i < count; i++)
            {
                using var tr = odb.StartTransaction();
                var tbl = table(tr);
                tbl.Upsert(new BtdbTest { CompanyId = 123456, TestId = (ulong)i, TestName = "test name "+i, Data1 = "data1 "+i, Data2 = "data2 "+i, Data3="data3 "+i});
                tr.Commit();
            }

            Console.WriteLine("CreateBtdbTestInserts:" + sw.Elapsed.TotalMilliseconds + " Mem kb:" +
                              Process.GetCurrentProcess().WorkingSet64 / 1024);
        }


        public void Run()
        {
            Console.WriteLine("Type: {3} InMemory: {0} TrullyInMemory: {1} MemoryMapped: {2}", _inMemory, _fastInMemory,
                _memoryMapped, _kvType);
            //CreateTestDB(9999999);
            //CreateRandomKeySequence(10000000);
            //DoWork5(true);
            //CheckKeySequence(10000000);
            //CreateTestDB(9999999);
            //OpenDBSpeedTest();
            //CheckDBTest(9999999);
            //HugeTest();
            //DoWork5(false);
            //DoWork5(true);
            //DoWork5ReadCheck();
            CreateBtdbTestInserts(10000000);
        }
    }
}
