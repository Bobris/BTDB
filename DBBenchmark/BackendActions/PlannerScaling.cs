using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using BTDB.BackendActions;

namespace DBBenchmark.BackendActions;

/// <summary>
/// How the coordinator's planner scales with its backlog. Each scenario keeps N actions in a steady state, and one
/// cycle is one coordinator loop after one change: typically an action finishes, another is queued, and the coordinator
/// expires, plans and records the starts.
/// <para>
/// Besides the time per step it reports what the step does: the elements it examines and its operations on ordered
/// sets and queues, which must both stay constant in N. The only cost left that grows is then that of one ordered
/// operation, logarithmic in its set's size. To show nothing else grows, the last columns divide the step's time by
/// its ordered operations and by the measured time of one ordered operation (remove and re-insert) on a sorted set of N
/// elements: a flat or falling ratio means the step grows no faster than one ordered operation.
/// </para>
/// </summary>
public static class PlannerScaling
{
    public static void Run(string[] args)
    {
        if (args.Length > 0 && args[0] == "profile")
        {
            Profile(args[1], int.Parse(args[2]));
            return;
        }
        var sizes = args.Length > 0 ? args.Select(int.Parse).ToArray() : [1_000, 10_000, 100_000];
        var reference = sizes.ToDictionary(n => n, OrderedOperationNanoseconds);
        Console.WriteLine("one ordered operation on N elements: " +
                          string.Join(", ", sizes.Select(n => $"N={n} {reference[n]:0} ns")));
        Console.WriteLine();
        Console.WriteLine($"{"scenario",-24}{"N",8}{"per step",12}{"examined",10}{"ordered",9}{"ns/ordered",12}{"ratio",8}");
        foreach (var name in PlannerScenarios.All.Concat(PlannerScenarios.Growing))
        {
            foreach (var n in sizes)
            {
                var (time, work, ordered) = Measure(name, n);
                var perOrdered = ordered == 0 ? 0 : time / ordered;
                var ratio = perOrdered / reference[n];
                Console.WriteLine($"{name,-24}{n,8}{Format(time),12}{work,10:0.0}{ordered,9:0.0}" +
                                  $"{(ordered == 0 ? "-" : $"{perOrdered:0}"),12}{(ordered == 0 ? "-" : $"{ratio:0.00}"),8}");
            }
        }
        Console.WriteLine();
        Console.WriteLine("Comparisons per step, against the proven lower bound for serving T tenants in consumption order,");
        Console.WriteLine("log2(T!) / T comparisons per served tenant (Doc/proofs/PlannerLowerBound.lean):");
        Console.WriteLine($"{"scenario",-24}{"N",8}{"tenants",9}{"compared",10}{"bound",8}{"compared/log2 T",17}");
        foreach (var name in PlannerScenarios.All.Concat(PlannerScenarios.Growing))
            foreach (var n in sizes)
            {
                var (comparisons, tenants) = Comparisons(name, n);
                var bound = tenants < 2 ? 0 : LogFactorial(tenants) / tenants;
                Console.WriteLine($"{name,-24}{n,8}{tenants,9}{comparisons,10:0.0}{bound,8:0.0}" +
                                  $"{(tenants < 2 ? "-" : $"{comparisons / Math.Log2(tenants):0.00}"),17}");
            }
    }

    // Where one scenario's step spends: allocation and collections per step, and the time of each phase.
    static void Profile(string name, int n)
    {
        var scenario = PlannerScenario.Create(name, n);
        var watch = Stopwatch.StartNew();
        while (watch.Elapsed.TotalMilliseconds < 500) scenario.Cycle();
        GC.Collect();
        const int steps = 20_000;
        var allocated = GC.GetAllocatedBytesForCurrentThread();
        var (gen0, gen1, gen2) = (GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));
        watch.Restart();
        for (var i = 0; i < steps; i++) scenario.Cycle();
        var elapsed = watch.Elapsed.TotalNanoseconds / steps;
        Console.WriteLine($"{name} N={n}: {elapsed:0} ns per step, " +
                          $"{(GC.GetAllocatedBytesForCurrentThread() - allocated) / (double)steps:0} bytes allocated per step, " +
                          $"collections gen0 {GC.CollectionCount(0) - gen0}, gen1 {GC.CollectionCount(1) - gen1}, " +
                          $"gen2 {GC.CollectionCount(2) - gen2} in {steps} steps, heap {GC.GetTotalMemory(false) / 1_000_000} MB");
    }

    static double LogFactorial(int n)
    {
        var sum = 0.0;
        for (var i = 2; i <= n; i++) sum += Math.Log2(i);
        return sum;
    }

    // Comparisons of the planner's ordered sets per step, counted through wrapped comparers, and the scenario's tenants.
    static (double PerStep, int Tenants) Comparisons(string name, int n)
    {
        var (byConsumption, startOrder) = (ActionLane.BySnapshot, ActionPlanner.StartOrder);
        long count = 0;
        ActionLane.BySnapshot = Comparer<ActionLane>.Create((a, b) =>
        {
            count++;
            return byConsumption.Compare(a, b);
        });
        ActionPlanner.StartOrder = Comparer<PlannedAction>.Create((a, b) =>
        {
            count++;
            return startOrder.Compare(a, b);
        });
        try
        {
            var scenario = PlannerScenario.Create(name, n);
            for (var i = 0; i < 1_000; i++) scenario.Cycle();
            var before = count;
            const int steps = 5_000;
            for (var i = 0; i < steps; i++) scenario.Cycle();
            return ((double)(count - before) / steps, scenario.Planner.Tenants);
        }
        finally
        {
            (ActionLane.BySnapshot, ActionPlanner.StartOrder) = (byConsumption, startOrder);
        }
    }

    static string Format(double nanoseconds) => nanoseconds >= 1_000_000 ? $"{nanoseconds / 1_000_000:0.00} ms"
        : nanoseconds >= 1_000 ? $"{nanoseconds / 1_000:0.0} us" : $"{nanoseconds:0} ns";

    // Nanoseconds per cycle, the median of five batches each long enough to measure, and the planner's examined
    // elements and ordered operations per cycle.
    static (double Nanoseconds, double Work, double Ordered) Measure(string name, int n)
    {
        var scenario = PlannerScenario.Create(name, n);
        // Long enough for tiered compilation to reach its optimized code, which takes about 100 ms of calls.
        var watch = Stopwatch.StartNew();
        while (watch.Elapsed.TotalMilliseconds < 500) scenario.Cycle();
        var batch = 1;
        while (true)
        {
            watch.Restart();
            for (var i = 0; i < batch; i++) scenario.Cycle();
            if (watch.Elapsed.TotalMilliseconds >= 50) break;
            batch *= 2;
        }
        var samples = new List<double>();
        var work = scenario.Planner.Work;
        var ordered = scenario.Planner.OrderedOperations;
        for (var s = 0; s < 5; s++)
        {
            watch.Restart();
            for (var i = 0; i < batch; i++) scenario.Cycle();
            samples.Add(watch.Elapsed.TotalNanoseconds / batch);
        }
        samples.Sort();
        var cycles = 5.0 * batch;
        return (samples[2], (scenario.Planner.Work - work) / cycles, (scenario.Planner.OrderedOperations - ordered) / cycles);
    }

    // Removing an element of a sorted set of n and inserting it again with a new key: the planner's typical ordered
    // operation, on a set as large as the whole backlog.
    static double OrderedOperationNanoseconds(int n)
    {
        var random = new Random(1);
        var set = new SortedSet<(double Key, ulong Id)>();
        var keys = new (double Key, ulong Id)[n];
        for (var i = 0; i < n; i++) set.Add(keys[i] = (random.NextDouble(), (ulong)i));
        var samples = new List<double>();
        var watch = new Stopwatch();
        for (var s = 0; s < 7; s++)
        {
            const int operations = 200_000;
            watch.Restart();
            for (var i = 0; i < operations; i++)
            {
                var at = random.Next(n);
                set.Remove(keys[at]);
                set.Add(keys[at] = (random.NextDouble(), keys[at].Id));
            }
            samples.Add(watch.Elapsed.TotalNanoseconds / (2.0 * operations));
        }
        samples.Sort();
        return samples[3];
    }
}
