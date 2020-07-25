using System;
using System.Collections.Generic;
using System.Linq;

namespace DBBenchmark
{
    public class KeyValueSpeedTest
    {

        Dictionary<byte[], byte[]> _testData = new Dictionary<byte[], byte[]>();

        static Dictionary<byte[], byte[]> CreateRandomData(int keysCount)
        {
            var data = new Dictionary<byte[], byte[]>();

            var rnd = new Random(1234);
            for (var i = 0; i < keysCount; i++)
            {
                var key = new byte[rnd.Next(10, 50)]; //Generate random length byte array        
                rnd.NextBytes(key); // fill byte array

                var value = new byte[rnd.Next(50, 500)]; //Generate random length byte array
                rnd.NextBytes(value); // fill byte array

                data.Add(key, value);
            }

            return data;
        }

        static Dictionary<byte[], byte[]> CreateHugeTestData()
        {
            var data = new Dictionary<byte[], byte[]>();
            const int keyCount = 100;

            for (var i = 0; i < keyCount; i++)
            {
                var key = new byte[100];
                var value = new byte[100000000];

                key[0] = (byte)(i / 100);
                key[1] = (byte)(i % 100);
                value[100] = (byte)(i / 100);
                value[200] = (byte)(i % 100);

                data.Add(key, value);
            }

            return data;
        }

        static Dictionary<byte[], byte[]> LoadSampleData()
        {
            using (var btdb = new BtdbTimeTests(false, true))
            {
                return btdb.GetDbData("sampleData");
            }
        }

        static (byte[] key, byte[] value) CreateRandomKeyValuePair()
        {
            var rnd = new Random(1234);

            var key = new byte[rnd.Next(5, 20)]; //Generate random length byte array        
            rnd.NextBytes(key); // fill byte array

            var value = new byte[rnd.Next(50, 500)]; //Generate random length byte array
            rnd.NextBytes(value); // fill byte array

            return (key, value);
        }

        static Dictionary<byte[], byte[]> ShuffleData(Dictionary<byte[], byte[]> data)
        {
            var rnd = new Random();

            return data.OrderBy(x => rnd.Next())
                .ToDictionary(item => item.Key, item => item.Value);
        }


        void OpenSpeedTest(IDbTimeTests dbTests)
        {
            var statistics = dbTests.Open();

            Console.WriteLine(
                $"Time to open DB : {statistics.openTime.TotalMilliseconds,15}ms Memory: {statistics.memorySize}KB");
        }

        void InsertDataByItemTest(IDbTimeTests dbTests)
        {
            var timeSpan = dbTests.InsertRangeCommitByItem(_testData);

            Console.WriteLine($"InsertDataByItem: {timeSpan.TotalMilliseconds,15}ms");
        }

        void InsertItemTest(IDbTimeTests dbTests, byte[] key, byte[] value)
        {
            var timeSpan = dbTests.Insert(key, value);

            Console.WriteLine($"InsertItem: {timeSpan.TotalMilliseconds,15}ms");
        }

        void InsertDataTest(IDbTimeTests dbTests)
        {
            var timeSpan = dbTests.InsertRange(_testData);

            Console.WriteLine($"InsertData: {timeSpan.TotalMilliseconds,15}ms");
        }

        void ReadAllDataTest(IDbTimeTests dbTests)
        {
            var timeSpan = dbTests.ReadAll(_testData);

            Console.WriteLine($"ReadAllData: {timeSpan.TotalMilliseconds,15}ms");
        }

        void ReadItemTest(IDbTimeTests dbTests, byte[] key)
        {
            var timeSpan = dbTests.Read(key);

            Console.WriteLine($"ReadItem: {timeSpan.TotalMilliseconds,15}ms");
        }

        void HugeTest(IDbTimeTests dbTests)
        {
            var data = CreateHugeTestData();

            var timeSpan = dbTests.InsertRangeCommitByItem(data);

            Console.WriteLine($"Time to create 10GB DB: {timeSpan.TotalMilliseconds,15}ms");

            timeSpan = dbTests.ReadValues(data.Keys.ToList());

            Console.WriteLine($"Time to read all values 10GB DB: {timeSpan.TotalMilliseconds,15}ms");
        }

        public void Run()
        {
            //            _testData = ShuffleData(LoadSampleData());
            _testData = CreateRandomData(99999);

            const bool inMemory = false;
            const bool memoryMapped = true;

            var randomKeyValuePair = CreateRandomKeyValuePair();

            Console.WriteLine($"--------------- BTDB inMemory: {inMemory}, memoryMapped: {memoryMapped} -------------");
            using (var db = new BtdbTimeTests(inMemory, memoryMapped))
            {
                //                HugeTest(db);
                //                InsertDataByItemTest(db);
                InsertDataTest(db);
                ReadAllDataTest(db);
                InsertItemTest(db, randomKeyValuePair.key, randomKeyValuePair.value);
                ReadItemTest(db, _testData.ToArray()[9999].Key);
            }

            Console.WriteLine("--------------- LMDB -------------");
            using (var db = new LightingDbTimeTests())
            {
                //                HugeTest(db);
                //                InsertDataByItemTest(db);
                InsertDataTest(db);
                ReadAllDataTest(db);
                InsertItemTest(db, randomKeyValuePair.key, randomKeyValuePair.value);
                ReadItemTest(db, _testData.ToArray()[9999].Key);
            }
        }

    }
}
