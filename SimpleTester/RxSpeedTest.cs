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

        public static void RunFastSubject()
        {
            //Observers  My Fast Original Delegate Del+null
            //        0      472     2877      315       43
            //        1      588     3087     1537      314
            //        2     1198     3298     1853     1573
            //        3     1926     3509     2134     1850
            //        4     2208     3718     2972     2182
            Console.WriteLine("FastSubject");
            Console.WriteLine("Observers  My Fast Original Delegate Del+null");
            for (var observers = -1; observers < 5; observers++)
            {
                Console.Write("{0,9} ", observers);
                Stopwatch sw;
                {
                    var subj = new FastSubject<int>();
                    for (var i = 0; i < observers; i++) subj.Subscribe(new EmptyIntObserver());
                    subj.OnNext(0);
                    sw = Stopwatch.StartNew();
                    for (var i = 0; i < 100000000; i++) subj.OnNext(i);
                    Console.Write("{0,8} ", sw.ElapsedMilliseconds);
                }
                {
                    var subj = new Subject<int>();
                    for (var i = 0; i < observers; i++) subj.Subscribe(new EmptyIntObserver());
                    subj.OnNext(0);
                    sw = Stopwatch.StartNew();
                    for (var i = 0; i < 100000000; i++) subj.OnNext(i);
                    Console.Write("{0,8} ", sw.ElapsedMilliseconds);
                }
                {
                    Action<int> d = p => { };
                    for (var i = 0; i < observers; i++)
                    {
                        d += p => { };
                    }
                    d(0);
                    sw = Stopwatch.StartNew();
                    for (var i = 0; i < 100000000; i++) d(i);
                    Console.Write("{0,8} ", sw.ElapsedMilliseconds);
                }
                {
                    Action<int>? d = null;
                    for (var i = 0; i < observers; i++)
                    {
                        d += p => { };
                    }
                    {
                        var localD = d;
                        localD?.Invoke(0);
                    }
                    sw = Stopwatch.StartNew();
                    for (var i = 0; i < 100000000; i++)
                    {
                        var localD = d;
                        localD?.Invoke(i);
                    }
                    Console.Write("{0,8} ", sw.ElapsedMilliseconds);
                }
                Console.WriteLine();
            }
        }

        public static void RunFastBehaviourSubject()
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
            for (var observers = 0; observers < 5; observers++)
            {
                Console.Write("{0,9} ", observers);
                {
                    var subj = new FastBehaviourSubject<int>(0);
                    for (var i = 0; i < observers; i++) subj.Subscribe(new EmptyIntObserver());
                    subj.OnNext(0);
                    var sw = Stopwatch.StartNew();
                    for (var i = 0; i < 100000000; i++) subj.OnNext(i);
                    Console.Write("{0,8} ", sw.ElapsedMilliseconds);
                }
                {
                    var subj = new BehaviorSubject<int>(0);
                    for (var i = 0; i < observers; i++) subj.Subscribe(new EmptyIntObserver());
                    subj.OnNext(0);
                    var sw = Stopwatch.StartNew();
                    for (var i = 0; i < 100000000; i++) subj.OnNext(i);
                    Console.Write("{0,8} ", sw.ElapsedMilliseconds);
                }
                Console.WriteLine();
            }
        }

        public void Run()
        {
            RunFastSubject();
            RunFastBehaviourSubject();
        }
    }
}
