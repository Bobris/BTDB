using System;
using System.Threading.Tasks;

namespace SimpleTester
{
    static class Program
    {
        static void Main(string[] args)
        {
            var random = new Random();
            var btdbTest = new BTDBTest.LowLevelDBTest();
            for (int i = 0; i < 1000; i++)
            {
                int l1 = random.Next(7000000);
                int l2 = random.Next(7000000);
                Console.WriteLine("{0}: {1}->{2}",i,l1,l2);
                btdbTest.ValueStoreWorks(l1, l2);
            }
        }
    }
}
