using System;
using System.Diagnostics;
using System.Reactive.Subjects;
using BTDB.Reactive;

namespace SimpleTester
{
    class RxSpeedTest
    {
        sealed class EmptyIntObserver : IObserver<int>
        {
            public void OnNext(int value)
            {
            }

            public void OnError(Exception error)
            {
            }

            public void OnCompleted()
            {
            }
        }

        public void RunFastSubject()
        {
            //Observers  My Fast Original Delegate Del+null
            //        0      490     2882      490       43
            //        1      589     3090     1794      498
            //        2     1341     3299     2388     1793
            //        3     1469     3509     2983     2389
            //        4     1785     3720     3578     2984
            Console.WriteLine("Observers  My Fast Original Delegate Del+null");
            for (int observers = 0; observers < 5; observers++)
            {
                Console.Write("{0,9} ", observers);
                Stopwatch sw;
                for (int impl = 0; impl < 2; impl++)
                {
                    ISubject<int> subj = null;
                    switch (impl)
                    {
                        case 0: subj = new FastSubject<int>();
                            break;
                        case 1: subj = new Subject<int>();
                            break;
                    }
                    for (int i = 0; i < observers; i++) subj.Subscribe(new EmptyIntObserver());
                    subj.OnNext(0);
                    sw = Stopwatch.StartNew();
                    for (int i = 0; i < 100000000; i++) subj.OnNext(i);
                    Console.Write("{0,8} ", sw.ElapsedMilliseconds);
                }
                {
                    Action<int> d = p => { };
                    for (int i = 0; i < observers; i++)
                    {
                        d += p => { };
                    }
                    d(0);
                    sw = Stopwatch.StartNew();
                    for (int i = 0; i < 100000000; i++) d(i);
                    Console.Write("{0,8} ", sw.ElapsedMilliseconds);
                }
                {
                    Action<int> d = null;
                    for (int i = 0; i < observers; i++)
                    {
                        d += p => { };
                    }
                    { var locald = d; if (locald!=null) locald(0); }
                    sw = Stopwatch.StartNew();
                    for (int i = 0; i < 100000000; i++) { var locald = d; if (locald != null) locald(i); }
                    Console.Write("{0,8} ", sw.ElapsedMilliseconds);
                }
                Console.WriteLine();
            }
        }

        public void RunFastSubscribe()
        {
            //Subscribe  My Fast Original
            //               552      803
            Console.WriteLine("Subscribe  My Fast Original");
            Console.Write("          ");
            for (int impl = 0; impl < 2; impl++)
            {
                var subj = new FastSubject<int>();
                switch (impl)
                {
                    case 0: subj.FastSubscribe(i => { });
                        break;
                    case 1: subj.Subscribe(i => { });
                        break;
                }
                var sw = Stopwatch.StartNew();
                for (int i = 0; i < 100000000; i++) subj.OnNext(i);
                Console.Write("{0,8} ", sw.ElapsedMilliseconds);
            }
            Console.WriteLine();
        }

        public void Run()
        {
            RunFastSubscribe();
            RunFastSubject();
        }
    }
}
