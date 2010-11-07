namespace BTDB
{
    public enum DurabilityPromiseType
    {
        /// <summary>
        /// Transactions are not flushed to physical disk, database could became corrupted in case of power outage
        /// </summary>
        NotDurable,
        /// <summary>
        /// Transactions are flushed to physical disk except last one, no corruption could happen (1 Full flush per transaction)
        /// </summary>
        NearlyDurable,
        /// <summary>
        /// Transactions are flushed to physical disk imidietly, no corruption could happen (2 Full flushes per transaction)
        /// </summary>
        CompletelyDurable,
    }
}