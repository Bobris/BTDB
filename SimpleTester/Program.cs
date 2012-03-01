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
            new KV2SpeedTest().Run();
        }
    }
}
