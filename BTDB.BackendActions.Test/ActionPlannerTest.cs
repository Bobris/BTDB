using Xunit;

namespace BTDB.BackendActions.Test;

public class ActionPlannerTest
{
    static readonly TimeSpan Second = TimeSpan.FromSeconds(1);
    readonly ActionPlannerOptions _options = new() { Lease = 10 * Second, WorkerTimeout = 5 * Second, MaxAttempts = 2 };
    readonly Dictionary<QualifiedActionId, QueuedAction> _rows = new();
    ActionPlanner? _planner;
    ActionPlanner Planner => _planner ??= new(_options, generation: 3);
    ulong _order;

    static ActionType Type(string name, int cpu = 100, int maxConcurrent = 0, string? group = null,
        bool singlePerTenant = false) => new()
    {
        Name = name, InputType = typeof(object), Cpu = cpu, MaxConcurrent = maxConcurrent,
        ConcurrencyGroup = group, SingleRunningPerTenant = singlePerTenant,
        Execute = (_, _) => Task.FromResult(new object()), Finished = (_, _, _, _, _) => { }, BeforeStart = null
    };

    static readonly ActionType Report = Type("report");
    static readonly ActionType Heavy = Type("heavy", cpu: 300);

    QualifiedActionId Add(ActionType type, ulong tenant = 0, ActionPriority priority = ActionPriority.Normal,
        TimeSpan now = default, Action<QueuedAction>? change = null)
    {
        var row = new QueuedAction
        {
            TenantId = tenant, ActionId = ++_order, Name = type.Name, Priority = priority, Order = _order
        };
        change?.Invoke(row);
        var id = new QualifiedActionId(tenant, row.ActionId);
        _rows[id] = row;
        Planner.Update(id, row, type, now);
        return id;
    }

    // Applies the planned starts as their consume would and returns the started IDs in assignment order.
    List<QualifiedActionId> PlanAndStart(TimeSpan now, ActionType? type = null)
    {
        var started = new List<QualifiedActionId>();
        foreach (var assignment in Planner.Plan(now))
        {
            var row = _rows[assignment.Id];
            row.IsStarted = true;
            row.RunAttempts = assignment.Attempt;
            row.WorkerSession = assignment.Worker;
            row.WorkerNode = assignment.Node;
            row.StartEventId = 100 + assignment.Attempt;
            Planner.Update(assignment.Id, row, type ?? Planner.Actions[assignment.Id].Type, now);
            started.Add(assignment.Id);
        }
        return started;
    }

    List<WorkerCommand> Exchange(Guid worker, TimeSpan now, int capacity = 100, string node = "node",
        ReportedAttempt[]? running = null, ReportedAttempt[]? finished = null, ulong generation = 3) =>
        Planner.Exchange(worker, node, capacity, generation, true,
            running ?? [], finished ?? [], now);

    [Fact]
    public void PriorityThenOrderWithinATenantAndOneActionPerTenantPerRound()
    {
        var worker = Guid.NewGuid();
        Exchange(worker, default, capacity: 1000);
        var low = Add(Report, 1, ActionPriority.Low);
        var realTime = Add(Report, 1, ActionPriority.RealTime);
        var other = Add(Report, 2);
        var normal = Add(Report, 1);
        // Rounds alternate tenants; tenant 1 starts in priority order.
        Assert.Equal([realTime, other, normal, low], PlanAndStart(default));
    }

    [Fact]
    public void SuspendedTenantsStartNothingWhileOtherTenantsRunOn()
    {
        Exchange(Guid.NewGuid(), default, capacity: 1000);
        var suspended = Add(Report, 1);
        var other = Add(Report, 2);
        Planner.SuspendedTenants = new HashSet<ulong> { 1 };
        Assert.Equal([other], PlanAndStart(default));
        Planner.SuspendedTenants = new HashSet<ulong>();
        Assert.Equal([suspended], PlanAndStart(default));
    }

    [Fact]
    public void LessConsumingTenantsGoFirstAndConsumptionDecays()
    {
        var worker = Guid.NewGuid();
        Exchange(worker, default, capacity: 100);
        var first = Add(Report, 1);
        Assert.Equal([first], PlanAndStart(default));
        var second = Add(Report, 1);
        var other = Add(Report, 2);
        Planner.Update(first, null, null, default); // finished: the worker is free again
        Exchange(worker, Second);
        Assert.Equal([other], PlanAndStart(Second)); // tenant 1 consumed more
        Planner.Update(other, null, null, Second);
        Assert.Equal([second], PlanAndStart(2 * Second));
    }

    [Fact]
    public void CapacitySharesKeepRoomForUrgentWorkAndAnIdleWorkerTakesAnyAction()
    {
        var worker = Guid.NewGuid();
        Exchange(worker, default, capacity: 400);
        var heavy = Add(Heavy, priority: ActionPriority.Idle); // above the 60 % share, but the worker is idle
        Assert.Equal([heavy], PlanAndStart(default));
        var idle = Add(Report, priority: ActionPriority.Idle);
        var realTime = Add(Report, priority: ActionPriority.RealTime);
        Assert.Equal([realTime], PlanAndStart(default)); // 300 + 100 fit only the real-time share
        Assert.False(Planner.Actions[idle].Worker.HasValue);
        Assert.Equal(400, Planner.Workers[worker].Used);
    }

    [Fact]
    public void ConcurrencyGroupsAndSingleRunningTenantsLimitStarts()
    {
        Exchange(Guid.NewGuid(), default, capacity: 10_000);
        var a = Type("a", maxConcurrent: 2, group: "shared");
        var b = Type("b", group: "shared", maxConcurrent: 2);
        var c = Type("c", singlePerTenant: true);
        var ids = new[] { Add(a), Add(b), Add(a), Add(c, 1), Add(c, 1), Add(c, 2) };
        Assert.Equal([ids[0], ids[3], ids[5], ids[1]], PlanAndStart(default));
        Planner.Update(ids[0], null, null, default);
        Planner.Update(ids[3], null, null, default);
        Assert.Equal([ids[2], ids[4]], PlanAndStart(default).OrderBy(i => i.ActionId));
    }

    [Fact]
    public void WorkersRotateAndAnOlderGenerationStartsNothingNew()
    {
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        var old = Guid.NewGuid();
        Exchange(first, default, capacity: 1000, node: "1");
        Exchange(old, default, capacity: 1000, node: "old", generation: 2);
        Exchange(second, default, capacity: 1000, node: "2", generation: 4); // a newer worker during the upgrade
        var ids = new[] { Add(Report), Add(Report), Add(Report), Add(Report) };
        PlanAndStart(default);
        Assert.Equal([first, second, first, second], ids.Select(id => _rows[id].WorkerSession));

        // An attempt the older worker already runs, from before the upgrade, stays with it.
        var running = Add(Report, change: row =>
        {
            row.IsStarted = true;
            row.RunAttempts = 1;
            row.WorkerSession = old;
            row.WorkerNode = "old";
        });
        Assert.Empty(Exchange(old, Second, capacity: 1000, node: "old", generation: 2, running: [new(running, 1)]));
        Assert.Equal(old, Planner.Actions[running].Worker);
    }

    [Fact]
    public void StartIsDeliveredAfterTheStartEventAndRenewedByReports()
    {
        var worker = Guid.NewGuid();
        Exchange(worker, default);
        var id = Add(Report);
        var assignment = Assert.Single(Planner.Plan(default));
        Assert.Empty(Exchange(worker, default)); // the start event is not applied yet
        var row = _rows[id];
        row.IsStarted = true;
        row.RunAttempts = assignment.Attempt;
        row.WorkerSession = worker;
        row.StartEventId = 42;
        Planner.Update(id, row, Report, default);
        Assert.Equal([new WorkerCommand(WorkerCommandKind.Start, id, 1, 42)], Exchange(worker, default));
        Assert.Empty(Exchange(worker, 9 * Second, running: [new(id, 1)]));
        Assert.Empty(Planner.Expire(13 * Second)); // the report renewed the lease
        Assert.Equal(worker, Planner.Actions[id].Worker);
    }

    [Fact]
    public void ARejectedStartReleasesTheWorker()
    {
        var worker = Guid.NewGuid();
        Exchange(worker, default);
        var id = Add(Report);
        Assert.Single(Planner.Plan(default));
        Planner.Update(id, _rows[id], Report, default); // the consume left the row unstarted
        Assert.Null(Planner.Actions[id].Worker);
        Assert.Equal(0, Planner.Workers[worker].Used);
        Assert.Equal([id], PlanAndStart(default));
    }

    [Fact]
    public void LostAttemptsRunAgainUntilTheirAttemptsAreExhausted()
    {
        var worker = Guid.NewGuid();
        Exchange(worker, default);
        var id = Add(Report);
        PlanAndStart(default);
        Assert.Empty(Planner.Expire(10 * Second)); // lease passed: runnable again
        Exchange(worker, 10 * Second);
        Assert.Equal([id], PlanAndStart(10 * Second));
        Assert.Equal(2ul, _rows[id].RunAttempts);
        Assert.Equal([new Abandonment(id, 2, ActionOutcomeKind.Interrupted, "AttemptsExhausted")],
            Planner.Expire(30 * Second)); // the worker timed out too
        Assert.Empty(Planner.Expire(40 * Second)); // abandoned once
        Assert.Empty(Planner.Plan(40 * Second));
    }

    [Fact]
    public void ARestartedNodeLosesItsPreviousProcessAttempts()
    {
        var old = Guid.NewGuid();
        Exchange(old, default, node: "n");
        var id = Add(Report);
        PlanAndStart(default);
        var replacement = Guid.NewGuid();
        Exchange(replacement, Second, node: "n");
        Assert.False(Planner.Workers.ContainsKey(old));
        Assert.Empty(Planner.Expire(Second));
        Assert.Equal([id], PlanAndStart(Second));
        Assert.Equal(replacement, _rows[id].WorkerSession);
    }

    [Fact]
    public void ANewCoordinatorProtectsStartedRowsUntilTheirWorkerReportsThem()
    {
        var worker = Guid.NewGuid();
        var id = Add(Report, change: row =>
        {
            row.IsStarted = true;
            row.RunAttempts = 1;
            row.WorkerSession = worker;
            row.StartEventId = 7;
        });
        var waiting = Add(Report);
        Exchange(Guid.NewGuid(), default, node: "other");
        Assert.Equal([waiting], PlanAndStart(default));
        Assert.Empty(Exchange(worker, Second, capacity: 1000, running: [new(id, 1)])); // adopted
        Assert.Equal(worker, Planner.Actions[id].Worker);

        var unreported = Add(Report, change: row => { row.IsStarted = true; row.RunAttempts = 1; });
        Assert.Empty(PlanAndStart(Second));
        Exchange(worker, 11 * Second, capacity: 1000, running: [new(id, 1)]);
        Assert.Empty(Planner.Expire(11 * Second)); // the protection passed
        Planner.Update(waiting, null, null, 11 * Second);
        Assert.Equal([unreported], PlanAndStart(11 * Second));
        Assert.Equal(2ul, _rows[unreported].RunAttempts);
    }

    [Fact]
    public void ANewProcessOnTheRecordedNodeEndsTheProtectionAtOnce()
    {
        var old = Guid.NewGuid();
        var recorded = Add(Report, change: row =>
        {
            row.IsStarted = true;
            row.RunAttempts = 1;
            row.WorkerSession = old;
            row.WorkerNode = "n";
        });
        var elsewhere = Add(Report, change: row =>
        {
            row.IsStarted = true;
            row.RunAttempts = 1;
            row.WorkerSession = Guid.NewGuid();
            row.WorkerNode = "m";
        });
        var replacement = Guid.NewGuid();
        Exchange(replacement, Second, capacity: 1000, node: "n");
        Assert.Equal([recorded], PlanAndStart(Second)); // the other node's attempt may still run
        Assert.Equal((2ul, replacement), (_rows[recorded].RunAttempts, _rows[recorded].WorkerSession));
        Assert.True(Planner.Actions[elsewhere].Protected);
        var late = Add(Report, change: row =>
        {
            row.IsStarted = true;
            row.RunAttempts = 3;
            row.WorkerSession = old;
            row.WorkerNode = "n";
        });
        Assert.False(Planner.Actions[late].Protected); // loaded after the new process already exchanged
    }

    [Fact]
    public void ReportedAdjustmentsAndOverridesChangeTheAccountedCpu()
    {
        _options.CpuOverride = (name, cpu) => name == "heavy" ? 50 : cpu;
        var worker = Guid.NewGuid();
        Exchange(worker, default, capacity: 1000);
        var heavy = Add(Heavy);
        var report = Add(Report);
        PlanAndStart(default);
        Assert.Equal(150, Planner.Workers[worker].Used);
        Exchange(worker, Second, capacity: 1000, running: [new(heavy, 1, 200), new(report, 1, -500)]);
        Assert.Equal((250, 10), (Planner.Actions[heavy].Cpu, Planner.Actions[report].Cpu)); // never below 10
        Assert.Equal(260, Planner.Workers[worker].Used);
        Planner.Update(heavy, null, null, Second);
        Assert.Equal(10, Planner.Workers[worker].Used);
    }

    [Fact]
    public void StaleAttemptsAreForgottenAndCancelledOnesAreCancelledOrAbandoned()
    {
        _options.WorkerTimeout = 30 * Second;
        var worker = Guid.NewGuid();
        Exchange(worker, default, capacity: 1000);
        var running = Add(Report);
        var lost = Add(Report);
        PlanAndStart(default);
        Assert.Equal([new WorkerCommand(WorkerCommandKind.Forget, running, 7), new(WorkerCommandKind.Forget, new(9, 9), 1)],
            Exchange(worker, default, running: [new(running, 7), new(new(9, 9), 1), new(lost, 1)]));

        _rows[running].IsCancelled = true;
        Planner.Update(running, _rows[running], Report, Second);
        Assert.Contains(new WorkerCommand(WorkerCommandKind.Cancel, running, 1),
            Exchange(worker, Second, running: [new(running, 1)]));
        _rows[lost].IsCancelled = true;
        Planner.Update(lost, _rows[lost], Report, Second);
        Assert.Equal([new Abandonment(lost, 1, ActionOutcomeKind.Cancelled, "WorkerLost")], Planner.Expire(10 * Second));
    }

    [Fact]
    public void AFinishedReportStopsRestartsUntilItsEventApplies()
    {
        var worker = Guid.NewGuid();
        Exchange(worker, default);
        var id = Add(Report);
        PlanAndStart(default);
        Assert.Empty(Exchange(worker, Second, finished: [new(id, 1)]));
        Assert.Equal(0, Planner.Workers[worker].Used);
        Assert.Empty(PlanAndStart(Second));
        Assert.Empty(Exchange(worker, 2 * Second));
        Planner.Update(id, null, null, 2 * Second);
        Assert.Empty(Planner.Actions);
    }

    [Fact]
    public void ProtectedAttemptsCountTowardConcurrencyLimits()
    {
        var single = Type("single", maxConcurrent: 1, singlePerTenant: true);
        var recorded = Add(single, 5, change: row =>
        {
            row.IsStarted = true;
            row.RunAttempts = 1;
            row.WorkerSession = Guid.NewGuid();
            row.WorkerNode = "old";
        });
        var waiting = Add(single, 5);
        var worker = Guid.NewGuid();
        Exchange(worker, default, capacity: 1000);
        Assert.True(Planner.Actions[recorded].Protected);
        Assert.Empty(PlanAndStart(default)); // the protected attempt may still run on node old
        Assert.Empty(Planner.Expire(10 * Second)); // the protection passed: the recorded attempt runs again first
        Exchange(worker, 10 * Second, capacity: 1000);
        Assert.Equal([recorded], PlanAndStart(10 * Second));
        Planner.Update(recorded, null, null, 10 * Second);
        Assert.Equal([waiting], PlanAndStart(10 * Second));
    }

    [Fact]
    public void ACancelIsNotRepeatedWhileTheWorkerEndsTheAttempt()
    {
        var worker = Guid.NewGuid();
        Exchange(worker, default);
        var id = Add(Report);
        PlanAndStart(default);
        _rows[id].IsCancelled = true;
        Planner.Update(id, _rows[id], Report, default);
        Assert.Equal([new WorkerCommand(WorkerCommandKind.Cancel, id, 1)], Exchange(worker, default, running: [new(id, 1)]));
        Assert.Empty(Exchange(worker, default, running: [new(id, 1, Cancelling: true)]));
    }

    [Fact]
    public void ADrainingWorkerLeavesAndItsAttemptsRunElsewhere()
    {
        var leaving = Guid.NewGuid();
        Exchange(leaving, default, node: "1");
        var id = Add(Report);
        PlanAndStart(default);
        var queued = Add(Report);
        Assert.Empty(Planner.Exchange(leaving, "1", 100, 3, false, [], [], Second));
        Assert.False(Planner.Workers.ContainsKey(leaving));
        Assert.Null(Planner.Actions[id].Worker);
        Exchange(Guid.NewGuid(), Second, capacity: 1000, node: "2");
        Assert.Equal([id, queued], PlanAndStart(Second).OrderBy(i => i.ActionId));
        Assert.Equal(2ul, _rows[id].RunAttempts);
    }

    [Fact]
    public void ActionsCostAtLeastTenCpu()
    {
        _options.CpuOverride = (_, _) => 0;
        var worker = Guid.NewGuid();
        Exchange(worker, default, capacity: 100);
        for (var i = 0; i < 12; i++) Add(Report);
        Assert.Equal(10, PlanAndStart(default).Count); // a worker's first 100 CPU, at 10 each
        Assert.Equal(100, Planner.Workers[worker].Used);
    }

    [Fact]
    public void ConsumptionDecaysOverManySecondsAtOnce()
    {
        var worker = Guid.NewGuid();
        Exchange(worker, default, capacity: 1000);
        var running = Add(Report, 1);
        Assert.Equal([running], PlanAndStart(default));
        Planner.Plan(3600 * Second); // an hour of running 100 CPU converges to 100 / (1 - 0.95)
        var first = Add(Report, 1);
        var other = Add(Report, 3);
        Assert.Equal([other, first], PlanAndStart(3600 * Second));
    }
}
