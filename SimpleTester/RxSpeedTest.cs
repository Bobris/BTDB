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
            //        0      472     2877      315       43
            //        1      588     3087     1537      314
            //        2     1198     3298     1853     1573
            //        3     1926     3509     2134     1850
            //        4     2208     3718     2972     2182
            Console.WriteLine("FastSubject");
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

        public void RunFastBehaviourSubject()
        {
            //FastBehaviourSubject is not really about speed, but also about new parameter less constructor
            //Observers  My Fast Original
            //        0     1923     2878
            //        1     2406     3101
            //        2     3230     3307
            //        3     3113     3508
            //        4     3637     3719
            Console.WriteLine("FastBehaviourSubject");
            Console.WriteLine("Observers  My Fast Original");
            for (int observers = 0; observers < 5; observers++)
            {
                Console.Write("{0,9} ", observers);
                for (int impl = 0; impl < 2; impl++)
                {
                    ISubject<int> subj = null;
                    switch (impl)
                    {
                        case 0: subj = new FastBehaviourSubject<int>(0);
                            break;
                        case 1: subj = new BehaviorSubject<int>(0);
                            break;
                    }
                    for (int i = 0; i < observers; i++) subj.Subscribe(new EmptyIntObserver());
                    subj.OnNext(0);
                    var sw = Stopwatch.StartNew();
                    for (int i = 0; i < 100000000; i++) subj.OnNext(i);
                    Console.Write("{0,8} ", sw.ElapsedMilliseconds);
                }
                Console.WriteLine();
            }
        }

        public void RunFastSubscribe()
        {
            //Subscribe  My Fast Original
            //               595      866
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
            RunFastBehaviourSubject();
        }
    }
}
