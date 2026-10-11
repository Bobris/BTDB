using System;
using System.Security.Cryptography;

namespace BTDB.Replication.EventLog;

public sealed record EventLogOptions
{
    /// <summary>Maximum length of an ordinary split, including its header and seal.</summary>
    public int SplitCap { get; init; } = 256 * 1024;

    /// <summary>Seal a split in the same write once less than this much space remains.</summary>
    public int SealFreeSpace { get; init; } = 64 * 1024;

    /// <summary>Records above this size are rejected before acceptance. The default admits 50 MiB payloads plus
    /// a small envelope (such as event-sourcing metadata). A publication round to a remote owner carries at most this
    /// many record bytes, so all nodes must use the same value.</summary>
    public int MaxRecordSize { get; init; } = 51 * 1024 * 1024;

    public int MaxBatchRecords { get; init; } = 1024;

    /// <summary>Before each storage write the owner waits this long, so records that publishers send right after the
    /// previous commit (often from other nodes answering it) share the write instead of waiting for the next one. It
    /// adds up to this much latency to every commit and lowers the maximum write rate. Nonzero overrides the adaptive
    /// window of <see cref="AdaptiveCommitLingerMax"/>; zero (default) leaves the choice to it. Waits below 2 ms yield
    /// the thread in a loop because timers are coarser, which costs CPU while records wait.</summary>
    public TimeSpan CommitLinger { get; init; } = TimeSpan.Zero;

    /// <summary>The owner chooses the commit window itself, up to this value, from when records arrive after each
    /// commit: it waits only while publishers answer commits (publish-and-await loops, typically spread over nodes) and
    /// stays at zero for independent arrivals. The default 3 ms covers answers from other nodes in one region. Zero
    /// disables the window unless <see cref="CommitLinger"/> sets a fixed one.</summary>
    public TimeSpan AdaptiveCommitLingerMax { get; init; } = TimeSpan.FromMilliseconds(3);

    /// <summary>Publication rounds one node keeps in flight per topic, so records arriving during the owner's storage
    /// write are already queued for its next write. Concurrent callers often form single-record rounds, so keep this
    /// above the expected number of concurrent publications per node. One restores strictly sequential rounds.</summary>
    public int MaxPublishRoundsInFlight { get; init; } = 64;

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

    /// <summary>Record bytes the owner queues for one live subscriber (beyond a single message, which always fits).
    /// A subscriber falling further behind is disconnected and catches up from storage.</summary>
    public long SubscriberBufferBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>Merge fan-out per level; zero disables background merging.</summary>
    public int MergeFanOut { get; init; } = 16;

    public int MergeLevels { get; init; } = 2;

    /// <summary>Delay between marking a covered object and deleting it; must exceed the longest reader pass.</summary>
    public TimeSpan DeletionDelay { get; init; } = TimeSpan.FromDays(1);

    public long MaxMergedObjectLength { get; init; } = 1L << 30;

    /// <summary>How often the owner runs a background merge and cleanup pass.</summary>
    public TimeSpan MergeInterval { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Wall clock for deletion deadlines, which storage persists.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

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
        if (SubscriberBufferBytes <= 0) throw new ArgumentOutOfRangeException(nameof(SubscriberBufferBytes));
        if (CommitLinger < TimeSpan.Zero || CommitLinger > TimeSpan.FromSeconds(1))
            throw new ArgumentOutOfRangeException(nameof(CommitLinger));
        if (AdaptiveCommitLingerMax < TimeSpan.Zero || AdaptiveCommitLingerMax > TimeSpan.FromMilliseconds(100))
            throw new ArgumentOutOfRangeException(nameof(AdaptiveCommitLingerMax));
        if (MaxPublishRoundsInFlight <= 0) throw new ArgumentOutOfRangeException(nameof(MaxPublishRoundsInFlight));
        if (MergeFanOut is 1 or < 0 || MergeLevels < 0 || MergeLevels > 9)
            throw new ArgumentOutOfRangeException(nameof(MergeFanOut));
    }
}
