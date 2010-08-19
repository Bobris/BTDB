using System;
using System.Threading.Tasks;

namespace SimpleTester
{
    static class Program
    {
        static void Main(string[] args)
        {
            var btdbTest = new BTDBTest.LowLevelDBTest();
            btdbTest.ValueStoreWorks(100000000, 1000000000);
        }
    }
}
