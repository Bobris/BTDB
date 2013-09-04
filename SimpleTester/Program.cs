namespace SimpleTester
{
    static class Program
    {
        static void Main(string[] args)
        {
            //new KeyValueDBReplayer("bug.log").Replay();
            //new SpeedTest1().Test();
            //new ChannelSpeedTest().Run(args);
            //new RxSpeedTest().Run();
            //new ComplexServiceTest().Run();
            //new KeyValueSpeedTest(true).Run();
            new KeyValueSpeedTest(false).Run();
            //new EventStorageSpeedTestAwaitable().Run();
            //new EventStorageSpeedTestDisruptor().Run();
            //new EventStorageSpeedTest().Run();
        }
    }
}
