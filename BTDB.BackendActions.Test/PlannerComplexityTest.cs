using DBBenchmark.BackendActions;
using Xunit;

namespace BTDB.BackendActions.Test;

/// <summary>
/// The coordinator's work per change must not grow with its backlog. Each scenario of the planner scaling benchmark
/// (<c>dotnet run -c Release --project DBBenchmark -- backend-actions</c>) runs at two backlog sizes, 1000 and 20,000,, and both the
/// elements the planner examines per step and its operations on ordered sets and queues must stay the same. Only the
/// cost of one ordered operation, logarithmic in its set's size, may grow: the lower bound for keeping tenants in
/// consumption order. Unlike timings, the counts are exact, so this holds in any build and on any machine.
/// </summary>
[Collection(nameof(PlannerComparisonCount))] // it wraps process-wide comparers: no other test may run meanwhile
public class PlannerComplexityTest
{
    public static TheoryData<string> Scenarios => new(PlannerScenarios.All);

    [Theory]
    [MemberData(nameof(Scenarios))]
    public void WorkPerCoordinatorStepDoesNotGrowWithTheBacklog(string scenario)
    {
        var small = PerStep(scenario, 1_000);
        var large = PerStep(scenario, 20_000);
        Assert.True(large.Work <= small.Work * 1.25 + 2,
            $"{scenario}: {small.Work:0.0} elements examined per step with 1000 actions, {large.Work:0.0} with 20000");
        Assert.True(large.Ordered <= small.Ordered * 1.25 + 2,
            $"{scenario}: {small.Ordered:0.0} ordered operations per step with 1000 actions, {large.Ordered:0.0} with 20000");
    }

    static (double Work, double Ordered) PerStep(string name, int n)
    {
        var scenario = PlannerScenario.Create(name, n);
        for (var i = 0; i < 50; i++) scenario.Cycle();
        var (work, ordered) = (scenario.Planner.Work, scenario.Planner.OrderedOperations);
        const int steps = 500;
        for (var i = 0; i < steps; i++) scenario.Cycle();
        return ((double)(scenario.Planner.Work - work) / steps, (double)(scenario.Planner.OrderedOperations - ordered) / steps);
    }

    /// <summary>Comparisons per step: a constant where nothing needs reordering; where tenants are served in
    /// consumption order, proportional to log₂ of their number, the lower bound proven in
    /// Doc/proofs/PlannerLowerBound.lean.</summary>
    [Theory]
    [MemberData(nameof(Scenarios))]
    public void ComparisonsPerStepGrowOnlyLikeTheLowerBound(string scenario)
    {
        var small = Comparisons(scenario, 1_000);
        var large = Comparisons(scenario, 20_000);
        if (large.Tenants <= small.Tenants * 2)
            Assert.True(large.PerStep <= small.PerStep * 1.1 + 1,
                $"{scenario}: {small.PerStep:0.0} comparisons per step with 1000 actions, {large.PerStep:0.0} with 20000");
        else
        {
            var perLogSmall = small.PerStep / Math.Log2(small.Tenants);
            var perLogLarge = large.PerStep / Math.Log2(large.Tenants);
            Assert.True(perLogLarge <= perLogSmall * 1.1,
                $"{scenario}: {perLogSmall:0.00} comparisons per log2 of {small.Tenants} tenants, {perLogLarge:0.00} of {large.Tenants}");
        }
    }

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
            for (var i = 0; i < 200; i++) scenario.Cycle();
            var before = count;
            const int steps = 1_000;
            for (var i = 0; i < steps; i++) scenario.Cycle();
            return ((double)(count - before) / steps, scenario.Planner.Tenants);
        }
        finally
        {
            (ActionLane.BySnapshot, ActionPlanner.StartOrder) = (byConsumption, startOrder);
        }
    }
}

[CollectionDefinition(nameof(PlannerComparisonCount), DisableParallelization = true)]
public class PlannerComparisonCount;

