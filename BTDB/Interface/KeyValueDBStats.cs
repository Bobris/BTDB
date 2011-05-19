namespace BTDB
{
    public class KeyValueDBStats
    {
        public ulong KeyValuePairCount { get; set; }
        public ulong ReallyUsedSize { get; set; }
        public ulong WastedSize { get; set; }
        public ulong DatabaseStreamSize { get; set; }
        public ulong TransactionNumber { get; set; }
        public ulong TotalBytesRead { get; set; }
        public ulong TotalBytesWritten { get; set; }
        public override string ToString()
        {
            return string.Format(@"KeyValuePairCount:   {0,15}
TransactionNumber:   {1,15}
WastedSize:          {2,15}
ReallyUsedSize:      {3,15}
DatabaseStreamSize:  {4,15}
Total Bytes Read:    {5,15}
Total Bytes Written: {6,15}", KeyValuePairCount, TransactionNumber, WastedSize, ReallyUsedSize, DatabaseStreamSize, TotalBytesRead, TotalBytesWritten);
        }
    }
}
