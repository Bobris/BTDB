using Xunit;

namespace BTDB.BackendActions.Test;

/// <summary>
/// Drives the indexed <see cref="ActionPlanner"/> and the scanning <see cref="ReferencePlanner"/> through the same
/// random histories of a coordinator, its consumes and its workers, and requires the same decisions after every step:
/// assignments in order, worker commands, abandonments, and every action's and worker's accounting. Reservations are
/// off, because the indexed planner looks only at the first action of each lane; their own tests cover them.
/// </summary>
public class PlannerEquivalenceTest
{
    sealed class TieException : Exception;

    sealed class SimWorker(Guid session, string node, int capacity, ulong generation)
    {
        internal Guid Session = session;
        internal readonly string Node = node;
        internal readonly int Capacity = capacity;
        internal readonly ulong Generation = generation;
        internal readonly Dictionary<QualifiedActionId, (ulong Attempt, bool Cancelling, int CpuDelta)> Running = new();
        internal readonly List<ReportedAttempt> Finished = new();
        internal readonly List<ReportedAttempt> Lost = new();
    }

    static ActionType Type(string name, int cpu, int maxConcurrent = 0, string? group = null, bool single = false) => new()
    {
        Name = name, InputType = typeof(object), Cpu = cpu, MaxConcurrent = maxConcurrent, ConcurrencyGroup = group,
        SingleRunningPerTenant = single, Priority = ActionPriority.Normal,
        Execute = (_, _) => Task.FromResult<(ActionResultKind, object?)>(default), Finished = (_, _, _, _, _) => { },
        BeforeStart = null
    };

    static readonly ActionType[] Types =
    [
        Type("plain", 100), Type("grouped", 300, maxConcurrent: 2, group: "g"), Type("small", 50, maxConcurrent: 3, group: "g"),
        Type("single", 100, single: true), Type("big", 500), Type("singleGrouped", 200, maxConcurrent: 4, group: "s", single: true),
        Type("tiny", 5)
    ];

    static readonly ActionPriority[] Priorities =
        [ActionPriority.RealTime, ActionPriority.Normal, ActionPriority.Low, ActionPriority.Idle];

    [Fact]
    public void TheIndexedPlannerDecidesLikeTheScanningOne()
    {
        var ties = 0;
        for (var seed = 0; seed < 400; seed++)
        {
            try { Run(seed, steps: 500); }
            catch (TieException) { ties++; } // the histories part there, so this one ends
        }
        Assert.True(ties <= 8, $"{ties} of 400 histories reached a consumption tie");
    }

    static void Run(int seed, int steps)
    {
        var random = new Random(seed);
        var options = new ActionPlannerOptions
        {
            Lease = TimeSpan.FromSeconds(5), WorkerTimeout = TimeSpan.FromSeconds(3), MaxAttempts = 3,
            ReserveAfter = TimeSpan.MaxValue, ConsumptionDecay = seed % 5 == 4 ? 1 : 0.95,
            CpuOverride = seed % 3 == 0 ? (name, cpu) => name == "plain" ? 150 : cpu : null
        };
        const ulong generation = 1;
        var reference = new ReferencePlanner(options, generation);
        var indexed = new ActionPlanner(options, generation);
        var rows = new Dictionary<QualifiedActionId, QueuedAction>();
        var workers = new List<SimWorker>();
        var now = TimeSpan.FromSeconds(1);
        ulong nextId = 0, nextOrder = 0, nextEvent = 0;
        var nodes = 0;
        var step = 0;

        void Check(bool condition, string what)
        {
            if (!condition) throw new Xunit.Sdk.XunitException($"seed {seed}, step {step}: {what}");
        }

        void Update(QualifiedActionId id)
        {
            var row = rows.GetValueOrDefault(id);
            var type = row == null ? null : Types.Single(t => t.Name == row.Name);
            reference.Update(id, row, type, now);
            indexed.Update(id, row, type, now);
        }

        void Remove(QualifiedActionId id)
        {
            rows.Remove(id);
            Update(id);
        }

        void AddWorker()
        {
            var worker = new SimWorker(Guid.NewGuid(), "n" + nodes++, random.Next(1, 8) * 100, random.Next(4) == 0 ? 0ul : 1ul);
            workers.Add(worker);
            Exchange(worker, ready: true);
        }

        void Exchange(SimWorker worker, bool ready)
        {
            var running = worker.Running.Select(r => new ReportedAttempt(r.Key, r.Value.Attempt, r.Value.CpuDelta,
                r.Value.Cancelling)).ToList();
            var finished = worker.Finished.ToList();
            var lost = worker.Lost.ToList();
            var expected = reference.Exchange(worker.Session, worker.Node, worker.Capacity, worker.Generation, ready,
                running, finished, now, lost);
            var actual = indexed.Exchange(worker.Session, worker.Node, worker.Capacity, worker.Generation, ready,
                running, finished, now, lost);
            static string Describe(IEnumerable<WorkerCommand> commands) =>
                string.Join(",", commands.Select(c => $"{c.Kind}:{c.Id.TenantId}/{c.Id.ActionId}#{c.Attempt}").Order());
            Check(Describe(expected) == Describe(actual), $"commands {Describe(expected)} vs {Describe(actual)}");
            worker.Finished.Clear(); // acknowledged
            worker.Lost.Clear();
            foreach (var command in expected)
                switch (command.Kind)
                {
                    case WorkerCommandKind.Start:
                        worker.Running.TryAdd(command.Id, (command.Attempt, false, 0));
                        break;
                    case WorkerCommandKind.Cancel:
                        if (worker.Running.TryGetValue(command.Id, out var cancelled))
                            worker.Running[command.Id] = cancelled with { Cancelling = true };
                        break;
                    case WorkerCommandKind.Forget:
                        worker.Running.Remove(command.Id);
                        break;
                }
        }

        void Loop()
        {
            var expected = reference.Expire(now).OrderBy(a => a.Id.TenantId).ThenBy(a => a.Id.ActionId).ToList();
            var actual = indexed.Expire(now);
            Check(expected.SequenceEqual(actual), $"abandonments {string.Join(",", expected)} vs {string.Join(",", actual)}");
            foreach (var abandonment in expected)
                if (rows.TryGetValue(abandonment.Id, out var row) && row.IsStarted && row.RunAttempts == abandonment.Attempt)
                    Remove(abandonment.Id);
            // Consumption before this plan, as both planners order by it; decay applies first, so it is read after a
            // plan that starts nothing would apply it too.
            var before = Enumerable.Range(0, 6).Select(t => (Reference: reference.ConsumptionOf((ulong)t),
                Indexed: indexed.ConsumptionOf((ulong)t))).ToArray();
            var plan = reference.Plan(now);
            var planned = indexed.Plan(now);
            if (!plan.SequenceEqual(planned))
            {
                // Tenants of equal consumption have no fair order. The planners reach equal consumption by different
                // floating point sums, so the last bit may order such a tie differently: allowed, nothing else is.
                var at = plan.Zip(planned).TakeWhile(p => p.First == p.Second).Count();
                Check(at < plan.Count && at < planned.Count, $"plans {string.Join(",", plan)} vs {string.Join(",", planned)}");
                var (one, other) = (plan[at].Id.TenantId, planned[at].Id.TenantId);
                static bool Tied(double a, double b) => Math.Abs(a - b) <= 1e-9 * Math.Max(1, Math.Max(a, b));
                Check(one != other && Tied(before[one].Reference, before[other].Reference) &&
                      Tied(before[one].Indexed, before[other].Indexed),
                    $"plans {string.Join(",", plan)} vs {string.Join(",", planned)}");
                throw new TieException();
            }
            var eventId = ++nextEvent;
            var rejected = random.Next(20) == 0; // a late activation of another session rejects the whole event
            foreach (var assignment in plan)
            {
                if (!rejected && rows.TryGetValue(assignment.Id, out var row) && !row.IsCancelled &&
                    assignment.Attempt == row.RunAttempts + 1)
                {
                    row.IsStarted = true;
                    row.RunAttempts = assignment.Attempt;
                    row.WorkerSession = assignment.Worker;
                    row.WorkerNode = assignment.Node;
                    row.StartEventId = eventId;
                }
                Update(assignment.Id);
            }
        }

        void Compare()
        {
            Check(reference.Actions.Count == indexed.Actions.Count, "action count");
            foreach (var (id, expected) in reference.Actions)
            {
                Check(indexed.Actions.TryGetValue(id, out var actual), $"action {id} missing");
                Check((expected.Worker, expected.Starting, expected.Protected, expected.Finished, expected.Abandoned,
                        expected.Counted, expected.HeldCpu, expected.LeaseUntil) ==
                    (actual!.Worker, actual.Starting, actual.Protected, actual.Finished, actual.Abandoned, actual.Counted,
                        actual.HeldCpu, actual.LeaseUntil), $"action {id} state");
            }
            Check(reference.Workers.Count == indexed.Workers.Count, "worker count");
            foreach (var (session, expected) in reference.Workers)
            {
                Check(indexed.Workers.TryGetValue(session, out var actual), $"worker {session} missing");
                Check((expected.Used, expected.Ready, expected.Actions.Count) ==
                      (actual!.Used, actual.Ready, actual.Actions.Count), $"worker {expected.NodeId} state");
            }
            var counts = reference.Actions.Values.GroupBy(a => a.Priority).OrderBy(g => g.Key)
                .Select(g => new PriorityOverview(g.Key, g.Count(a => a.Runnable), g.Count(a => a.Worker != null),
                    g.Count(a => a.Protected))).ToList();
            Check(counts.SequenceEqual(indexed.PriorityCounts()),
                $"counts {string.Join(",", counts)} vs {string.Join(",", indexed.PriorityCounts())}");
        }

        AddWorker();
        AddWorker();
        for (step = 0; step < steps; step++)
        {
            var roll = random.Next(100);
            if (roll < 28)
            {
                var type = Types[random.Next(Types.Length)];
                var id = new QualifiedActionId((ulong)random.Next(6), ++nextId);
                rows[id] = new QueuedAction
                {
                    TenantId = id.TenantId, ActionId = id.ActionId, Name = type.Name,
                    Priority = Priorities[random.Next(Priorities.Length)], Order = ++nextOrder
                };
                Update(id);
            }
            else if (roll < 52) Loop();
            else if (roll < 74)
            {
                if (workers.Count != 0) Exchange(workers[random.Next(workers.Count)], ready: true);
            }
            else if (roll < 84)
            {
                // A worker ends an attempt: it publishes the outcome, loses it, or interrupts it while stopping.
                var busy = workers.Where(w => w.Running.Count != 0).ToList();
                if (busy.Count == 0) continue;
                var worker = busy[random.Next(busy.Count)];
                var (id, run) = worker.Running.ElementAt(random.Next(worker.Running.Count));
                worker.Running.Remove(id);
                var current = rows.TryGetValue(id, out var row) && row.IsStarted && row.RunAttempts == run.Attempt;
                var kind = random.Next(10);
                if (kind < 7)
                {
                    worker.Finished.Add(new(id, run.Attempt));
                    if (current) Remove(id);
                }
                else if (kind < 9) worker.Lost.Add(new(id, run.Attempt));
                else if (current)
                {
                    row!.IsStarted = false;
                    row.InterruptedAttempts++;
                    Update(id);
                }
            }
            else if (roll < 88)
            {
                if (rows.Count == 0) continue;
                var (id, row) = rows.ElementAt(random.Next(rows.Count));
                if (!row.IsStarted) Remove(id);
                else
                {
                    row.IsCancelled = true;
                    Update(id);
                }
            }
            else if (roll < 92)
            {
                now += TimeSpan.FromMilliseconds(random.Next(800));
                // Running attempts adjust their cost now and then.
                foreach (var worker in workers)
                    foreach (var id in worker.Running.Keys.ToList())
                        if (random.Next(10) == 0)
                            worker.Running[id] = worker.Running[id] with { CpuDelta = random.Next(-50, 150) };
            }
            else if (roll < 94)
            {
                if (random.Next(2) == 0)
                {
                    var names = Types.Where(_ => random.Next(4) == 0).Select(t => t.Name).ToHashSet(StringComparer.Ordinal);
                    reference.Suspended = names;
                    indexed.Suspended = names.ToHashSet(StringComparer.Ordinal);
                }
                else
                {
                    var tenants = Enumerable.Range(0, 6).Where(_ => random.Next(4) == 0).Select(t => (ulong)t).ToHashSet();
                    reference.SuspendedTenants = tenants;
                    indexed.SuspendedTenants = tenants.ToHashSet();
                }
            }
            else if (roll < 99)
            {
                var change = random.Next(4);
                if (change == 0 || workers.Count == 0) AddWorker();
                else if (change == 1)
                {
                    // The node's process restarts: a new session that runs nothing yet.
                    var worker = workers[random.Next(workers.Count)];
                    worker.Session = Guid.NewGuid();
                    worker.Running.Clear();
                    worker.Finished.Clear();
                    worker.Lost.Clear();
                    Exchange(worker, ready: true);
                }
                else if (change == 2)
                {
                    var worker = workers[random.Next(workers.Count)];
                    worker.Running.Clear();
                    Exchange(worker, ready: false); // its last exchange while stopping
                    workers.Remove(worker);
                }
                else
                {
                    var worker = workers[random.Next(workers.Count)];
                    workers.Remove(worker); // dies silently: the worker timeout ends it
                }
            }
            else
            {
                // A new coordinator: it loads every row, then hears from the workers.
                var suspended = reference.Suspended;
                var suspendedTenants = reference.SuspendedTenants;
                reference = new ReferencePlanner(options, generation)
                    { Suspended = suspended, SuspendedTenants = suspendedTenants };
                indexed = new ActionPlanner(options, generation)
                    { Suspended = suspended, SuspendedTenants = suspendedTenants };
                foreach (var id in rows.Keys.OrderBy(i => i.TenantId).ThenBy(i => i.ActionId).ToList()) Update(id);
            }
            Compare();
        }
    }
}
