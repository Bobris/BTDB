using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB.BackendActions;

namespace DBBenchmark.BackendActions;

public static class PlannerScenarios
{
    /// <summary>The scenario whose cost is known to grow, documented in Doc/BackendActionsPlan.md and kept out of
    /// PlannerComplexityTest: a tenant waiting in many open classes repairs its entries in them when served.</summary>
    public static readonly string[] Growing = ["many-open-lanes"];

    public static readonly string[] All =
    [
        "group-limited", "many-tenants", "single-running-tenants", "suspended-backlog", "capacity-blocked",
        "many-running", "overview", "many-workers", "many-types", "many-lanes"
    ];
}

/// <summary>One steady-state backlog shape driven through the planner as the coordinator drives it.</summary>
public sealed class PlannerScenario
{
    readonly ActionPlanner _planner;
    readonly Dictionary<QualifiedActionId, QueuedAction> _rows = new();
    readonly Queue<QualifiedActionId> _running = new();
    readonly List<Guid> _workers = new();
    readonly Action _cycle;
    readonly TimeSpan _now = TimeSpan.FromSeconds(1); // fixed: no lease or worker expires while measuring
    ulong _next;
    ulong _tenantCursor;
    int _overview;

    PlannerScenario(ActionPlannerOptions options, Func<PlannerScenario, Action> setup)
    {
        _planner = new(options);
        _cycle = setup(this);
    }

    internal ActionPlanner Planner => _planner;

    public void Cycle() => _cycle();

    static ActionType Type(string name, int cpu = 100, int maxConcurrent = 0, bool singlePerTenant = false) => new()
    {
        Name = name, InputType = typeof(object), Cpu = cpu, MaxConcurrent = maxConcurrent, ConcurrencyGroup = null,
        SingleRunningPerTenant = singlePerTenant, Priority = ActionPriority.Normal,
        Execute = (_, _) => Task.FromResult<(ActionResultKind, object?)>(default), Finished = (_, _, _, _, _) => { },
        BeforeStart = null
    };

    public static PlannerScenario Create(string name, int n) => name switch
    {
        // One type limited to 4 running in the cluster, all N waiting for it.
        "group-limited" => new(new(), s =>
        {
            var report = Type("report", maxConcurrent: 4);
            s.AddWorker(capacity: 1_000_000);
            for (var i = 0; i < n; i++) s.Queue(report, 0);
            s.PlanAndStart();
            return () => s.FinishQueueAndPlan(report, 0);
        }),
        // N actions of N / 10 tenants; the worker has room for 3 at a time.
        "many-tenants" => new(new(), s =>
        {
            var report = Type("report");
            var tenants = (ulong)Math.Max(1, n / 10);
            s.AddWorker(capacity: 400);
            for (var i = 0; i < n; i++) s.Queue(report, (ulong)i % tenants);
            s.PlanAndStart();
            return () => s.FinishQueueAndPlan(report, s._tenantCursor++ % tenants);
        }),
        // N tenants each running one action and waiting with the next of a single-running type.
        "single-running-tenants" => new(new(), s =>
        {
            var sync = Type("sync", singlePerTenant: true);
            s.AddWorker(capacity: 100 * (n + 10));
            for (var i = 0; i < n; i++)
            {
                s.Queue(sync, (ulong)i);
                s.Queue(sync, (ulong)i);
            }
            s.PlanAndStart();
            return () =>
            {
                var done = s._running.Dequeue();
                s.Finish(done);
                s.Queue(sync, done.TenantId);
                s.ExpireAndPlan();
            };
        }),
        // N actions of a suspended type queued before a stream of another type; room for 3 at a time.
        "suspended-backlog" => new(new(), s =>
        {
            var frozen = Type("frozen");
            var live = Type("live");
            s.AddWorker(capacity: 400);
            s._planner.Suspended = new HashSet<string> { "frozen" };
            for (var i = 0; i < n; i++) s.Queue(frozen, 0);
            for (var i = 0; i < 10; i++) s.Queue(live, 0);
            s.PlanAndStart();
            return () => s.FinishQueueAndPlan(live, 0);
        }),
        // N actions too large for a busy worker's share, next to a stream of small ones; no reservations.
        "capacity-blocked" => new(new() { ReserveAfter = TimeSpan.MaxValue }, s =>
        {
            var big = Type("big", cpu: 400);
            var small = Type("small");
            s.AddWorker(capacity: 400);
            for (var i = 0; i < 10; i++) s.Queue(small, 2);
            s.PlanAndStart();
            for (var i = 0; i < n; i++) s.Queue(big, 1);
            s.PlanAndStart();
            return () => s.FinishQueueAndPlan(small, 2);
        }),
        // N running actions on N / 50 workers, nothing waiting: one worker's exchange per cycle.
        "many-running" => new(new(), s =>
        {
            var report = Type("report");
            var workers = Math.Max(1, n / 50);
            for (var w = 0; w < workers; w++) s.AddWorker(capacity: 5_000);
            for (var i = 0; i < workers * 50; i++) s.Queue(report, (ulong)i % 100);
            s.PlanAndStart();
            var held = s._workers.ToDictionary(w => w, w => s._planner.Workers[w].Actions
                .Select(id => new ReportedAttempt(id, 1)).ToList());
            var cursor = 0;
            return () =>
            {
                var worker = s._workers[cursor++ % s._workers.Count];
                s._planner.Exchange(worker, worker.ToString(), 5_000, 0, true, held[worker], [], s._now);
                s.ExpireAndPlan();
            };
        }),
        // The coordinator overview's per-priority counts over the many-tenants backlog.
        "overview" => new(new(), s =>
        {
            var report = Type("report");
            s.AddWorker(capacity: 400);
            for (var i = 0; i < n; i++) s.Queue(report, (ulong)i % (ulong)Math.Max(1, n / 10));
            s.PlanAndStart();
            return () => s._overview += s._planner.PriorityCounts().Count;
        }),
        // N / 10 workers, each full with one action but one; each step frees a worker far from the round robin's place.
        "many-workers" => new(new(), s =>
        {
            var report = Type("report");
            var workers = Math.Max(2, n / 10);
            for (var w = 0; w < workers; w++) s.AddWorker(capacity: 100);
            for (var i = 0; i < workers + 10; i++) s.Queue(report, (ulong)i % 100);
            s.PlanAndStart();
            return () => s.FinishQueueAndPlan(report, s._tenantCursor++ % 100);
        }),
        // N / 10 action types of their own tenants, each limited to one running and waiting with more, next to a
        // stream of another type: many classes, each tenant in one.
        "many-types" => new(new(), s =>
        {
            var types = Math.Max(1, n / 10);
            var stream = Type("stream");
            s.AddWorker(capacity: 200 * (types + 10)); // room in the Normal share for all of them
            for (var t = 0; t < types; t++)
            {
                var limited = Type("limited" + t, maxConcurrent: 1);
                for (var i = 0; i < 10; i++) s.Queue(limited, 1_000_000 + (ulong)t);
            }
            s.PlanAndStart();
            s._running.Clear(); // the limited ones keep running; only the stream's finish
            for (var i = 0; i < 3; i++) s.Queue(stream, 0);
            s.PlanAndStart();
            return () => s.FinishQueueAndPlan(stream, 0);
        }),
        // One tenant waiting in N / 10 classes that are full, and served through a stream of another type: a tenant in
        // many classes, which only the classes consulted pay for.
        "many-lanes" => new(new(), s =>
        {
            var types = Math.Max(1, n / 10);
            var stream = Type("stream");
            s.AddWorker(capacity: 200 * (types + 10));
            for (var t = 0; t < types; t++)
            {
                var limited = Type("limited" + t, maxConcurrent: 1);
                for (var i = 0; i < 10; i++) s.Queue(limited, 0);
            }
            s.PlanAndStart();
            s._running.Clear();
            for (var i = 0; i < 3; i++) s.Queue(stream, 0);
            s.PlanAndStart();
            return () => s.FinishQueueAndPlan(stream, 0);
        }),
        // One tenant waiting in N / 10 open classes, with room for one more start per step: every service makes its
        // entries in all those classes stale, and the next plan repairs them.
        "many-open-lanes" => new(new(), s =>
        {
            var types = Math.Max(1, n / 10);
            var classes = Enumerable.Range(0, types).Select(t => Type("open" + t)).ToArray();
            s.AddWorker(capacity: 400);
            for (var i = 0; i < 10; i++)
                foreach (var type in classes) s.Queue(type, 0);
            s.PlanAndStart();
            var next = 0;
            return () => s.FinishQueueAndPlan(classes[next++ % classes.Length], 0);
        }),
        _ => throw new ArgumentException("Unknown scenario " + name)
    };

    void AddWorker(int capacity)
    {
        var session = Guid.NewGuid();
        _workers.Add(session);
        _planner.Exchange(session, session.ToString(), capacity, 0, true, [], [], _now);
    }

    void Queue(ActionType type, ulong tenant)
    {
        var id = new QualifiedActionId(tenant, ++_next);
        var row = new QueuedAction
        {
            TenantId = tenant, ActionId = id.ActionId, Name = type.Name, Priority = ActionPriority.Normal, Order = _next
        };
        _rows[id] = row;
        _planner.Update(id, row, type, _now);
    }

    void Finish(QualifiedActionId id)
    {
        _rows.Remove(id);
        _planner.Update(id, null, null, _now);
    }

    void FinishQueueAndPlan(ActionType type, ulong tenant)
    {
        Finish(_running.Dequeue());
        Queue(type, tenant);
        ExpireAndPlan();
    }

    void ExpireAndPlan()
    {
        _planner.Expire(_now);
        PlanAndStart();
    }

    // Applies each start as its ActionsStarted consume does.
    void PlanAndStart()
    {
        foreach (var assignment in _planner.Plan(_now))
        {
            var row = _rows[assignment.Id];
            row.IsStarted = true;
            row.RunAttempts = assignment.Attempt;
            row.WorkerSession = assignment.Worker;
            row.WorkerNode = assignment.Node;
            _planner.Update(assignment.Id, row, _planner.Actions[assignment.Id].Type, _now);
            _running.Enqueue(assignment.Id);
        }
    }
}
