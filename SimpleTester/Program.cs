using System;

namespace SimpleTester
{
    static class Program
    {
        static void Main(string[] args)
        {
            var btdbTest = new BTDBTest.LowLevelDBTest();
            btdbTest.ValueStoreWorks(256, 512);
            for (int i = 0; i < 60000;i++ )
            {
                Console.WriteLine(i);
                for(int j=0;j<i;j++)
                {
                    btdbTest.ValueStoreWorks(j, i);
                    btdbTest.ValueStoreWorks(i, j);
                }
            }
        }
    }
}
