using System;
using System.Diagnostics;
using System.Reactive.Subjects;
using BTDB.Reactive;

namespace SimpleTester
{
    class RxSpeedTest
    {
        sealed class EmptyIntObserver: IObserver<int>
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

        public void Run()
        {
//Observers  My Fast Original
//        0      356     2643
//        1      551     2793
//        2     1106     2977
//        3     1299     3171
//        4     1578     3390
            Console.WriteLine("Observers  My Fast Original");
            for(int observers = 0;observers<5;observers++)
            {
                Console.Write("{0,9} ",observers);
                for(int impl = 0;impl<2;impl++)
                {
                    ISubject<int> subj = null;
                    switch (impl)
                    {
                        case 0: subj = new FastSubject<int>();
                            break;
                        case 1: subj = new Subject<int>();
                            break;
                    }
                    for (int i = 0; i < observers;i++) subj.Subscribe(new EmptyIntObserver());
                    subj.OnNext(0);
                    var sw = Stopwatch.StartNew();
                    for (int i = 0; i<100000000;i++) subj.OnNext(i);
                    Console.Write("{0,8} ", sw.ElapsedMilliseconds);
                }
                Console.WriteLine();
            }
        }

        
    }
}
