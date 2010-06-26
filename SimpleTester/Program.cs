using System;
using System.Threading.Tasks;

namespace SimpleTester
{
    static class Program
    {
        static void Main(string[] args)
        {
            var btdbTest = new BTDBTest.LowLevelDBTest();
            btdbTest.MultipleTransactions(256);
            int i = 256;
            while (true)
            {
                Console.WriteLine(i);
                int i1 = i;
                Parallel.Invoke(() => btdbTest.MultipleTransactions(i1), () => btdbTest.MultipleTransactions2(i1));
                i++;
            }
        }
    }
}
