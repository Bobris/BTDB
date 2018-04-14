using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace SimpleTester
{
    public class CompactorLatencyMeasurement
    {
        public CompactorLatencyMeasurement()
        {
        }

        public class SmallObject
        {
            public ulong Id { get; set; }
            public string Label { get; set; }
        }

        public class SmallObjects
        {
            public IOrderedDictionary<ulong, SmallObject> Items { get; set; }
        }

        public void Run()
        {
            var collection = new InMemoryFileCollection();
            using (var kvDb = new KeyValueDB(collection, new NoCompressionStrategy(), 100 * 1024 * 1024))
            {
                ulong itemsCount = 0;
                using (var objDb = new ObjectDB())
                {
                    objDb.Open(kvDb, false);

                    Console.WriteLine("started generating");
                    using (var tr = objDb.StartWritingTransaction().Result)
                    {
                        var objects = tr.Singleton<SmallObjects>();
                        while (true)
                        {
                            objects.Items.Add(itemsCount, new SmallObject() {Id = itemsCount, Label = "bu"});
                            
                            if(itemsCount % 1_000_000 == 0)
                                Console.WriteLine("Generated {0}", itemsCount);

                            if (itemsCount % 1000 == 0 && collection.GetCount() == 20)
                                break;

                            itemsCount++;
                        }

                        tr.Commit();
                    }
                    Console.WriteLine("finished generating");


                    using (var tr = objDb.StartWritingTransaction().Result)
                    {
                        var objects = tr.Singleton<SmallObjects>();
                        itemsCount = (ulong)objects.Items.Count;
                        tr.Commit();
                    }

                    Console.WriteLine("removing items started");
                    using (var tr = objDb.StartWritingTransaction().Result)
                    {
                        var objects = tr.Singleton<SmallObjects>();
                        for (ulong i = 0; i < itemsCount / 5; i++)
                        {
                            if (i % 2 == 0)
                                continue;
                            
                            objects.Items.Remove(i);
                        }

                        tr.Commit();
                    }

                    Console.WriteLine("removing items finished");

                    var transactionCreationStarted = new ManualResetEventSlim(false);
                    var compactionFinished = new ManualResetEventSlim(false);

                    Task.Run(() =>
                    {
                        Console.WriteLine("Started waiting for transaction creating");
                        transactionCreationStarted.Wait();
                        Console.WriteLine("Started Compacting");
                        Trace.Assert(kvDb.Compact(CancellationToken.None));
                        Console.WriteLine("Finished Compacting");
                        compactionFinished.Set();
                    });

                    Console.WriteLine("Started concurrent transaction creation");

                    long msMax = 0;
                    long average = 0;
                    long iterations = 0;
                    Stopwatch watch = new Stopwatch();

                    while (true)
                    {
                        var compactionFinishedBeforeLasttransaction = compactionFinished.IsSet;
                        iterations++;
                        watch.Start();
                        var task = objDb.StartWritingTransaction();
                        if(!transactionCreationStarted.IsSet)
                            transactionCreationStarted.Set();

                        task.Wait();

                        var ms = watch.ElapsedMilliseconds;
                        average += ms;
                        msMax = Math.Max(ms, msMax);
                        watch.Reset();
                        using (var tr = task.Result)
                        {
                            tr.Commit();
                        }

                        if ((compactionFinishedBeforeLasttransaction && compactionFinished.IsSet))
                            break;
                    }
                    Console.WriteLine("Finished concurrent transaction creation, longest transaction create time was {0}ms, " +
                                     "average {1}ms, iterations {2}", msMax, average/(double)iterations, iterations);
                }
            }
        }
    }
}