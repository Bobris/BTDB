using System;
using System.Threading.Tasks;

namespace SimpleTester
{
    static class Program
    {
        static void Main(string[] args)
        {
            new KeyValueDBReplayer("bug.log").Replay();
            //new SpeedTest1().Test();
        }
    }
}
