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
            using (var fileCollection = CreateTestFileCollection())
            {
                _sw.Start();
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                    var key = new byte[100];
                    var value = new byte[100000000];
                    for (int i = 0; i < 100; i++)
                    {
                        using (var tr = db.StartTransaction())
                        {
                            key[0] = (byte) (i/100);
                            key[1] = (byte) (i%100);
                            value[100] = (byte) (i/100);
                            value[200] = (byte) (i%100);
                            tr.CreateOrUpdateKeyValue(key, value);
                            tr.Commit();
                        }
                    }
                }
                _sw.Stop();
                Console.WriteLine("Time to create 10GB DB: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
                _sw.Start();
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                    _sw.Stop();
                    Console.WriteLine("Time to open 10GB DB: {0,15}ms", _sw.Elapsed.TotalMilliseconds);
                    _sw.Start();
                    var key = new byte[100];
                    for (int i = 0; i < 100; i++)
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
            }
            
        }

        public void Run()
        {
            HugeTest();
            //DoWork5(true);
            //DoWork5ReadCheck();
        }
    }
}
