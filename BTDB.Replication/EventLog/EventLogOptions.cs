using System;
using System.Security.Cryptography;

namespace BTDB.Replication.EventLog;

public sealed record EventLogOptions
{
    /// <summary>Maximum length of an ordinary split, including its header and seal.</summary>
    public int SplitCap { get; init; } = 256 * 1024;

    /// <summary>Seal a split in the same write once less than this much space remains.</summary>
    public int SealFreeSpace { get; init; } = 64 * 1024;

    /// <summary>Records above this size are rejected before acceptance.</summary>
    public int MaxRecordSize { get; init; } = 16 * 1024 * 1024;

    public int MaxBatchRecords { get; init; } = 1024;

    /// <summary>Retry delay for an ambiguous storage write; the same write is repeated until resolved.</summary>
    public TimeSpan AmbiguousRetryDelay { get; init; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Maximum attempts to take over a contended tail before giving up for this round.</summary>
    public int TakeoverAttempts { get; init; } = 5;

    /// <summary>An idle owner validates its tail and sends a heartbeat at this interval.</summary>
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Without a batch or heartbeat for this long, a follower treats the owner as unreachable.</summary>
    public TimeSpan OwnerTimeout { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>Delay before contending again after a lost takeover race.</summary>
    public TimeSpan ContentionBackoff { get; init; } = TimeSpan.FromMilliseconds(200);

    /// <summary>Committed bytes the owner keeps for subscribers that reconnect.</summary>
    public long RecentCacheBytes { get; init; } = 16 * 1024 * 1024;

    /// <summary>Merge fan-out per level; zero disables background merging.</summary>
    public int MergeFanOut { get; init; } = 16;

    public int MergeLevels { get; init; } = 2;

    /// <summary>Delay between marking a covered object and deleting it; must exceed the longest reader pass.</summary>
    public TimeSpan DeletionDelay { get; init; } = TimeSpan.FromDays(1);

    public long MaxMergedObjectLength { get; init; } = 1L << 30;

    /// <summary>Source of publisher and owner session identities.</summary>
    public Func<ulong> NewSessionId { get; init; } = () =>
    {
        Span<byte> bytes = stackalloc byte[8];
        RandomNumberGenerator.Fill(bytes);
        return BitConverter.ToUInt64(bytes) | 1;
    };

    internal void Validate(string topic)
    {
        var minimum = EventLogFormat.SplitHeaderLength(topic) + EventLogFormat.FrameLength(1, 1, 0) +
                      EventLogFormat.SealLength;
        if (SplitCap < minimum || SplitCap > EventLogFormat.MaxObjectLength)
            throw new ArgumentOutOfRangeException(nameof(SplitCap));
        if (SealFreeSpace < 0 || SealFreeSpace >= SplitCap) throw new ArgumentOutOfRangeException(nameof(SealFreeSpace));
        if (MaxRecordSize <= 0 || MaxRecordSize > EventLogFormat.MaxObjectLength / 2)
            throw new ArgumentOutOfRangeException(nameof(MaxRecordSize));
        if (MaxBatchRecords <= 0) throw new ArgumentOutOfRangeException(nameof(MaxBatchRecords));
        if (MergeFanOut is 1 or < 0 || MergeLevels < 0 || MergeLevels > 9)
            throw new ArgumentOutOfRangeException(nameof(MergeFanOut));
    }
}
