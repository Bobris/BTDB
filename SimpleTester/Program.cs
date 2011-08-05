using System;
using BTDB.KVDBLayer.Implementation;
using BTDB.KVDBLayer.Helpers;
using BTDB.StreamLayer;

namespace SimpleTester
{
    static class Program
    {
        static void Main(string[] args)
        {
            //new KeyValueDBReplayer("bug.log").Replay();
            //new SpeedTest1().Test();
            //new ChannelSpeedTest().Run(args);
            new RxSpeedTest().Run();
        }
    }
}
