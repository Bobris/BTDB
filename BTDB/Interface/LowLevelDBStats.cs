namespace BTDB
{
    public class LowLevelDBStats
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
            return string.Format(@"KeyValuePairCount: {0}
TransactionNumber: {1}
WastedSize: {2}
ReallyUsedSize: {3}
DatabaseStreamSize: {4}
Total Bytes Read: {5}
Total Bytes Written: {6}", KeyValuePairCount, TransactionNumber, WastedSize, ReallyUsedSize, DatabaseStreamSize, TotalBytesRead, TotalBytesWritten);
        }
    }
}
