using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;
using Xunit;
using static BTDB.Replication.Test.EventLog.EventLogTestExtensions;

namespace BTDB.Replication.Test.EventLog;

public class EventLogLateCreateTest
{
    const string Topic = "t";

    /// An owner sealed its split and then took long to create the successor with its batch. Meanwhile another owner
    /// filled that successor and later splits, and merging deleted it, as a short DeletionDelay allows. The late create
    /// then landed on the free name. Before the 2026-10-28 fix the first owner acknowledged its batch, which no reader
    /// sees, as merged objects cover that range; now it is fenced, so the record is published again elsewhere.
    [Fact]
    public async Task LateSuccessorCreateOnADeletedSplitNameIsNotAcknowledged()
    {
        var storage = new FaultyEventLogStorage();
        var options = new EventLogOptions
        {
            SplitCap = 256, SealFreeSpace = 64, MergeFanOut = 2, MergeLevels = 2, DeletionDelay = TimeSpan.Zero
        };
        Task<EventLogOwnerLane> NewLane(string name) => EventLogOwnerLane.CreateCandidateAsync(storage,
            new(storage, Topic), Topic, options, new TestScheduler(), new(name, "http://" + name), default).AsTask();

        var a = await NewLane("a");
        Assert.Equal(0ul, await a.SubmitAsync(1, 1, Bytes(0, 12), 1, false, 0, default));
        var armed = 1;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        string? blockedKey = null;
        storage.BeforeMutation = operation =>
        {
            if (operation.Kind != "create" || Interlocked.CompareExchange(ref armed, 0, 1) != 1) return default;
            blockedKey = operation.Key;
            return new(gate.Task);
        };
        Task<ulong>? late = null;
        var submitted = 1;
        for (var i = 1; i < 100 && late == null; i++, submitted++)
        {
            var submit = a.SubmitAsync(1, (ulong)i + 1, Bytes(i, 12), (ulong)i + 1, false, 0, default).AsTask();
            await Task.WhenAny(submit, Task.Delay(200));
            if (blockedKey != null) late = submit;
            else await submit;
        }

        Assert.NotNull(late);
        var b = await NewLane("b");
        for (var i = 0; i < 60; i++)
            await b.SubmitAsync(2, (ulong)i + 1, Bytes(1000 + i, 12), (ulong)i + 1, true, 0, default);
        for (var pass = 0; pass < 20 && storage.Inner.Keys.Contains(blockedKey); pass++)
            await new EventLogMerger(storage, Topic, options, new TestScheduler()).RunPassAsync(default);
        Assert.DoesNotContain(blockedKey, storage.Inner.Keys);
        gate.SetResult();
        await Assert.ThrowsAsync<EventLogOwnerLostException>(() => late!.WaitAsync(TimeSpan.FromSeconds(10)));
        var records = await new EventLogStorageReader(storage, Topic).ReadAllAsync();
        Assert.Equal(Enumerable.Range(0, submitted - 1).Select(i => Bytes(i, 12))
                .Concat(Enumerable.Range(0, 60).Select(i => Bytes(1000 + i, 12))),
            records.Select(r => r.Payload.ToArray()));
    }
}
