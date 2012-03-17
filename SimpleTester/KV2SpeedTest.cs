using System;
using System.Diagnostics;
using System.IO;
using BTDB.Buffer;
using BTDB.KV2DBLayer;
using BTDB.KVDBLayer;

namespace SimpleTester
{
    class KV2SpeedTest
    {
        readonly Stopwatch _sw = new Stopwatch();

        static IFileCollection CreateTestFileCollection()
        {
            if (true)
            {
                const string dbfilename = "data";
                if (Directory.Exists(dbfilename))
                    Directory.Delete(dbfilename, true);
                Directory.CreateDirectory(dbfilename);
                return new OnDiskFileCollection(dbfilename);
            }
            else
            {
                return new InMemoryFileCollection();
            }
        }

        static IFileCollection OpenTestFileCollection()
        {
            const string dbfilename = "data";
            return new OnDiskFileCollection(dbfilename);
        }

        void DoWork5(bool alsoDoReads)
        {
            _sw.Restart();
            long pureDataLength = 0;
            using (var fileCollection = CreateTestFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
            {
                for (int i = 0; i < 200; i++)
                {
                    long pureDataLengthPrevTr = pureDataLength;
                    using (var tr = db.StartTransaction())
                    {
                        for (int j = 0; j < 200; j++)
                        {
                            tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(new[] { (byte)j, (byte)i }), ByteBuffer.NewAsync(new byte[1 + i * j]));
                            pureDataLength += 2 + 1 + i * j;
                        }
                        if (alsoDoReads) using (var trCheck = db.StartTransaction())
                            {
                                long pureDataLengthCheck = 0;
                                if (trCheck.FindFirstKey())
                                {
                                    do
                                    {
                                        pureDataLengthCheck += trCheck.GetKey().Length + trCheck.GetValue().Length;
                                    } while (trCheck.FindNextKey());
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

        void DoWork5ReadCheck()
        {
            _sw.Restart();
            using (var fileCollection = new OnDiskFileCollection("data"))
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
            {
                using (var trCheck = db.StartTransaction())
                {
                    long pureDataLengthCheck = 0;
                    if (trCheck.FindFirstKey())
                    {
                        do
                        {
                            pureDataLengthCheck += trCheck.GetKey().Length + trCheck.GetValue().Length;
                        } while (trCheck.FindNextKey());
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
                using (IKeyValueDB db = new KeyValue2DB(fileCollection, new NoCompressionStrategy()))
                {
                    var key = new byte[100];
                    var value = new byte[100000000];
                    for (int i = 0; i < keyCount; i++)
                    {
                        using (var tr = db.StartTransaction())
                        {
                            key[0] = (byte)(i / 100);
                            key[1] = (byte)(i % 100);
                            value[100] = (byte)(i / 100);
                            value[200] = (byte)(i % 100);
                            tr.CreateOrUpdateKeyValue(key, value);
                            tr.Commit();
                        }
                    }
                }
                _sw.Stop();
                Console.WriteLine("Time to create 10GB DB: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
                _sw.Restart();
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                    _sw.Stop();
                    Console.WriteLine("Time to open 10GB DB: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
                    _sw.Restart();
                    var key = new byte[100];
                    for (int i = 0; i < keyCount; i++)
                    {
                        using (var tr = db.StartTransaction())
                        {
                            key[0] = (byte)(i / 100);
                            key[1] = (byte)(i % 100);
                            tr.FindExactKey(key);
                            var value = tr.GetValueAsByteArray();
                            if (value[100] != (byte)(i / 100)) throw new InvalidDataException();
                            if (value[200] != (byte)(i % 100)) throw new InvalidDataException();
                        }
                    }
                    _sw.Stop();
                    Console.WriteLine("Time to read all values 10GB DB: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
                    Console.WriteLine(db.CalcStats());
                }
                _sw.Restart();
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                    _sw.Stop();
                    Console.WriteLine("Time to open2 10GB DB: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
                    _sw.Restart();
                    var key = new byte[100];
                    for (int i = 0; i < keyCount; i++)
                    {
                        using (var tr = db.StartTransaction())
                        {
                            key[0] = (byte)(i / 100);
                            key[1] = (byte)(i % 100);
                            tr.FindExactKey(key);
                            var value = tr.GetValueAsByteArray();
                            if (value[100] != (byte)(i / 100)) throw new InvalidDataException();
                            if (value[200] != (byte)(i % 100)) throw new InvalidDataException();
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
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                }
            }
        }

        void OpenDBSpeedTest()
        {
            _sw.Start();
            using (var fileCollection = OpenTestFileCollection())
            {
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                    _sw.Stop();
                    Console.WriteLine("Time to open DB: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
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
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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

        public void Run()
        {
            CreateTestDB(9999999);
            OpenDBSpeedTest();
            CheckDBTest(9999999);
            //HugeTest();
            //DoWork5(true);
            //DoWork5ReadCheck();
        }

    }
}
