using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using HdrHistogram;

namespace SimpleTester
{
    public class MultiCoreDBTest : IKeyValueDBLogger
    {
        public class Person
        {
            [PrimaryKey(1)] public ulong Id { get; set; }
            [SecondaryKey("Email")] public string? Email { get; set; }
            public string? Description { get; set; }
        }

        public interface IPersonTable : IRelation<Person>
        {
            bool Insert(Person person);
            void Update(Person person);
            Person FindById(ulong id);
            void RemoveById(ulong id);
            IOrderedDictionaryEnumerator<ulong, Person> ListById(AdvancedEnumeratorParam<ulong> param);
            IOrderedDictionaryEnumerator<string, Person> ListByEmail(AdvancedEnumeratorParam<string> param);
        }

        IKeyValueDB? _kvdb;
        IObjectDB? _odb;
        Func<IObjectDBTransaction, IPersonTable>? _personRelation;
        Stopwatch _eventSw = new Stopwatch();

        void DoInEvent(string name, Action<IObjectDBTransaction> consume)
        {
            _eventSw.Restart();
            var tr = _odb!.StartWritingTransaction().GetAwaiter().GetResult();
            _eventSw.Stop();

            consume(tr);
        }

        long _lastAllocatorId;
        readonly Random _randomForEvents = new Random();
        string? _dbdir;
        OnDiskFileCollection? _fc;

        public MultiCoreDBTest(Dictionary<string, IntHistogram> eventStats)
        {
        }

        ulong AllocatedId()
        {
            return (ulong)Interlocked.Increment(ref _lastAllocatorId);
        }

        void DoEventInsertPerson()
        {
            DoInEvent("InsertPerson", tr =>
            {
                var rel = _personRelation!(tr);
                var item = new Person();
                item.Id = AllocatedId();
                item.Email =
                    $"{(char)_randomForEvents.Next('a', 'z')}{(uint)_randomForEvents.Next()}@{RandomString(_randomForEvents, 5, 10)}";
                item.Description = RandomString(_randomForEvents, 50, 100);
                rel.Insert(item);
            });
        }

        static string RandomString(Random random, int lengthFrom, int lengthTo)
        {
            var len = random.Next(lengthFrom, lengthTo);
            return string.Create(len, random, ((span, random1) =>
            {
                for (var i = 0; i < span.Length; i++)
                {
                    span[i] = (char)random1.Next('a', 'z');
                }
            }));
        }

        void Initialize()
        {
            _dbdir = Path.GetTempPath() + "/deleteMeDB";
            Directory.Delete(_dbdir, true);
            Directory.CreateDirectory(_dbdir);
            _fc = new OnDiskFileCollection(_dbdir);
            _kvdb = new KeyValueDB(new KeyValueDBOptions
            {
                Compression = new SnappyCompressionStrategy(),
                FileSplitSize = 100 * 1024 * 1024,
                FileCollection = _fc
            });
            _kvdb.Logger = this;
            _odb = new ObjectDB();
            _odb.Open(_kvdb, false);
            using (var tr = _odb.StartWritingTransaction().GetAwaiter().GetResult())
            {
                _personRelation = tr.InitRelation<IPersonTable>("Person");
                tr.Commit();
            }
        }

        void Dispose()
        {
            _odb!.Dispose();
            _kvdb!.Dispose();
            _fc!.Dispose();
        }

        public void ReportTransactionLeak(IKeyValueDBTransaction transaction)
        {
            throw new InvalidOperationException("Transaction LEAK");
        }

        public void CompactionStart(ulong totalWaste)
        {
            Console.WriteLine($"Compaction started with {totalWaste} waste");
        }

        public void CompactionCreatedPureValueFile(uint fileId, ulong size, uint itemsInMap, ulong roughMemory)
        {
            Console.WriteLine($"Pvl file {fileId} with size {size} created. Items in map {itemsInMap} roughly {roughMemory} bytes.");
        }

        public void KeyValueIndexCreated(uint fileId, long keyValueCount, ulong size, TimeSpan duration)
        {
            Console.WriteLine(
                $"KeyValueIndexCreated file id: {fileId} kvcount: {keyValueCount} size: {size} duration: {duration.TotalSeconds:F2}s");
        }

        public void LogWarning(string message)
        {
            Console.WriteLine("Warning: "+message);
        }

        public void Run()
        {
            Initialize();
            var cancelationSource = new CancellationTokenSource();
            var eventTask = new Task(() =>
            {
                var cancelation = cancelationSource.Token;
                while (!cancelation.IsCancellationRequested)
                {
                }
            });
            var consoleTask = new Task(() =>
            {
                var cancelation = cancelationSource.Token;
                while (!cancelation.IsCancellationRequested)
                {
                    switch (Console.ReadLine())
                    {
                        case "e":
                            cancelationSource.Cancel();
                            break;
                        case "m":
                            Console.WriteLine($"GC Memory: {GC.GetTotalMemory(false)} Working set: {Process.GetCurrentProcess().WorkingSet64}");
                            break;
                        case "s":
                            Console.WriteLine(_kvdb!.CalcStats());
                            break;
                    }
                }
            });
            Task.WaitAll(eventTask, consoleTask);
            Dispose();
        }
    }
}
