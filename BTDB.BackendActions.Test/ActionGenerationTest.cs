using Xunit;

namespace BTDB.BackendActions.Test;

[BackendAction(Name = "generated-report", Cpu = 40, MaxConcurrent = 3, ConcurrencyGroup = "reports",
    SingleRunningPerTenant = true)]
[DailyScheduled(90)]
public class GeneratedReport : ActionInput<OrderResult>
{
    public int Pages { get; set; }
}

public class GeneratedPlain : ActionInput<OrderResult>
{
    public GeneratedPlain(int seed) => Seed = seed;
    public int Seed { get; set; }
}

public class GeneratedReportHandler : IActionHandler<GeneratedReport, OrderResult>
{
    public Task<OrderResult> ExecuteAsync(GeneratedReport input, IActionContext context) => Task.FromResult(new OrderResult());
    public void OnFinished(GeneratedReport input, ActionOutcome<OrderResult> outcome) { }
}

/// <summary>Needs a factory: it has no parameterless constructor.</summary>
public class GeneratedPlainHandler(string prefix) : IActionHandler<GeneratedPlain, OrderResult>
{
    public string Prefix { get; } = prefix;
    public Task<OrderResult> ExecuteAsync(GeneratedPlain input, IActionContext context) =>
        Task.FromResult(new OrderResult { Text = Prefix });
    public void OnFinished(GeneratedPlain input, ActionOutcome<OrderResult> outcome) { }
}

public class ActionGenerationTest
{
    static bool Generated(Type handler) => handler == typeof(GeneratedReportHandler) || handler == typeof(GeneratedPlainHandler);

    [Fact]
    public void RegisterGeneratedUsesTheGeneratedMetadataAndFactories()
    {
        var actions = new BackendActions().RegisterGenerated(
            type => type == typeof(GeneratedPlainHandler) ? new GeneratedPlainHandler("factory") : null!, Generated);
        var report = actions.TypeOf("generated-report")!;
        Assert.Equal((typeof(GeneratedReport), 40, 3, "reports", true),
            (report.InputType, report.Cpu, report.MaxConcurrent, report.ConcurrencyGroup, report.SingleRunningPerTenant));
        Assert.Equal(90, Assert.IsType<DailyScheduledAttribute>(report.Schedule).MinuteOfDay);
        Assert.IsType<GeneratedReport>(report.Create!());
        var plain = actions.TypeOf(typeof(GeneratedPlain).FullName!)!;
        Assert.Equal((100, 0, (string?)null, (object?)null), (plain.Cpu, plain.MaxConcurrent, plain.ConcurrencyGroup, (object?)plain.Schedule));
        Assert.Equal(2, actions.Types.Count);
    }

    [Fact]
    public void HandRegistrationReadsTheGeneratedMetadata()
    {
        var actions = new BackendActions().Register(new GeneratedReportHandler());
        Assert.Equal(40, actions.TypeOf("generated-report")!.Cpu);
    }

    [Fact]
    public void AHandlerWithoutAParameterlessConstructorNeedsAFactory()
    {
        var error = Assert.Throws<InvalidOperationException>(() =>
            new BackendActions().RegisterGenerated(include: type => type == typeof(GeneratedPlainHandler)));
        Assert.Contains(nameof(GeneratedPlainHandler), error.Message);
    }

    [Fact]
    public void InaccessibleAndGenericHandlersAreNotGenerated()
    {
        var handlers = GeneratedActions.All.Select(a => a.HandlerType).ToHashSet();
        Assert.Contains(typeof(GeneratedReportHandler), handlers);
        Assert.Contains(typeof(Recorder.ExportProbe), handlers); // internal is accessible to generated code
        Assert.DoesNotContain(handlers, h => h.IsGenericType || h.IsNestedPrivate);
    }
}
