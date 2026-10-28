using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;
using Xunit;
using static BTDB.Replication.Test.EventLog.EventLogTestExtensions;

namespace BTDB.Replication.Test.EventLog;

/// One publisher session submits records through owner lanes while storage mutations randomly time out without
/// effect or lose their response, owners take over and merges run. Every receipt names the record's offset, and the
/// history holds each record exactly once, in order.
public class EventLogRandomFaultTest
{
    const string Topic = "t";
    static readonly EventLogOwner[] Owners = [new("a", "http://a"), new("b", "http://b")];

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 60).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task RecordsCommitOnceInOrderUnderFaults(int seed)
    {
        var random = new Random(seed);
        var storage = new FaultyEventLogStorage();
        var options = new EventLogOptions { SplitCap = 256, SealFreeSpace = 64, MergeFanOut = 2, MergeLevels = 2 };
        var faultsOn = false;
        storage.Fault = _ =>
        {
            if (!faultsOn) return StorageFault.None;
            lock (random)
                return random.Next(10) switch
                {
                    0 => StorageFault.TimeoutWithoutEffect,
                    1 => StorageFault.LoseResponse,
                    _ => StorageFault.None
                };
        };

        async Task<EventLogOwnerLane> NewLane() => await EventLogOwnerLane.CreateCandidateAsync(storage,
            new(storage, Topic), Topic, options, new TestScheduler(), Owners[random.Next(Owners.Length)], default);

        var lane = await NewLane();
        faultsOn = true;
        var log = new List<string>();
        ulong resumeFrom = 0;
        try
        {
            for (var i = 0; i < 60; i++)
            {
                var sequence = (ulong)i + 1;
                var dispatched = false;
                for (var attempt = 0; ; attempt++)
                {
                    Assert.True(attempt < 50, "Too many attempts");
                    if (random.Next(15) == 0)
                    {
                        log.Add($"{i}: takeover");
                        lane = await NewLane();
                    }

                    try
                    {
                        dispatched = true;
                        var offset = await lane.SubmitAsync(1, sequence, Bytes(i, 20), sequence, attempt > 0 || dispatched,
                            resumeFrom, CancellationToken.None);
                        Assert.Equal((ulong)i, offset);
                        resumeFrom = offset + 1;
                        log.Add($"{i}: committed at {offset} after {attempt} retries");
                        break;
                    }
                    catch (EventLogOwnerLostException ex)
                    {
                        log.Add($"{i}: {ex.GetType().Name}");
                        if (lane.IsStopped) lane = await NewLane();
                    }
                }

                if (random.Next(8) == 0)
                {
                    await new EventLogMerger(storage, Topic, options, new TestScheduler()).RunPassAsync(default);
                }
            }
        }
        catch (Exception ex)
        {
            throw new Exception($"Seed {seed}:{Environment.NewLine}{string.Join(Environment.NewLine, log.TakeLast(10))}",
                ex);
        }

        faultsOn = false;
        var records = await new EventLogStorageReader(storage, Topic).ReadAllAsync();
        Assert.Equal(Enumerable.Range(0, 60).Select(i => (ulong)i), records.Select(r => r.Offset));
        Assert.Equal(Enumerable.Range(0, 60).Select(i => Bytes(i, 20)), records.Select(r => r.Payload.ToArray()));
    }
}
