using System;
using System.Threading.Tasks;

namespace SimpleTester
{
    static class Program
    {
        static void Main(string[] args)
        {
            var btdbTest = new BTDBTest.LowLevelDBTest();
            btdbTest.MultipleTransactions(128);
        }
    }
}
