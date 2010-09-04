using System;
using System.Threading.Tasks;

namespace SimpleTester
{
    static class Program
    {
        static void Main(string[] args)
        {
            var btdbTest = new BTDBTest.LowLevelDBTest();
            for (int i = 10000; i <= 10000; i++)
            {
                Parallel.For(1, i + 1, j =>
                                           {
                                               for (int k = 0; k <= i - j; k++)
                                               {
                                                   btdbTest.AdvancedEraseRangeWorks(i, k, j);
                                               }
                                           });
            }
        }
    }
}
