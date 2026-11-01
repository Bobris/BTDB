using System.Threading.Tasks;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class BackendActionsGeneratorTests : GeneratorTestsBase
{
    [Fact]
    public Task VerifyHandlersAreRegisteredWithTheirInputsMetadata()
    {
        return VerifySourceGenerator("""
            using System;
            using System.Threading.Tasks;
            using BTDB;
            using BTDB.BackendActions;

            namespace TestNamespace;

            [Generate]
            public class Result
            {
                public string Text { get; set; } = "";
            }

            [BackendAction(Name = "reports", Cpu = 40, MaxConcurrent = 3, ConcurrencyGroup = "heavy", SingleRunningPerTenant = true)]
            [MonthlyScheduled(31, 90)]
            public class Report : ActionInput<Result>
            {
                public int Pages { get; set; }
            }

            public class Mail : ActionInput<Result>
            {
                public Mail(string to) => To = to;
                public string To { get; set; }
            }

            public class ReportHandler : IActionHandler<Report, Result>
            {
                public Task<Result> ExecuteAsync(Report input, IActionContext context) => Task.FromResult(new Result());
                public void OnFinished(Report input, ActionOutcome<Result> outcome) { }
            }

            // Two inputs, and no parameterless constructor: RegisterGenerated needs a handler factory.
            internal sealed class MailHandler(string server) : IActionHandler<Mail, Result>, IActionHandler<Report, Result>
            {
                public Task<Result> ExecuteAsync(Mail input, IActionContext context) => Task.FromResult(new Result { Text = server });
                public void OnFinished(Mail input, ActionOutcome<Result> outcome) { }
                public Task<Result> ExecuteAsync(Report input, IActionContext context) => Task.FromResult(new Result());
                public void OnFinished(Report input, ActionOutcome<Result> outcome) { }
            }
            """);
    }

    [Fact]
    public Task VerifyAbstractGenericPrivateAndInheritedHandlersAreSkipped()
    {
        return VerifySourceGenerator("""
            using System.Threading.Tasks;
            using BTDB.BackendActions;

            namespace TestNamespace;

            public class Result
            {
            }

            public class Job : ActionInput<Result>
            {
            }

            public abstract class HandlerBase<TInput> : IActionHandler<TInput, Result> where TInput : ActionInput<Result>
            {
                public Task<Result> ExecuteAsync(TInput input, IActionContext context) => Task.FromResult(new Result());
                public void OnFinished(TInput input, ActionOutcome<Result> outcome) { }
            }

            // Registered: the interface comes from a generic base.
            public class JobHandler : HandlerBase<Job>
            {
            }

            // Not registered: its concrete base registers the same input.
            public class DerivedJobHandler : JobHandler
            {
            }

            public class GenericHandler<TInput> : HandlerBase<TInput> where TInput : ActionInput<Result>
            {
            }

            public class Outer
            {
                class PrivateHandler : HandlerBase<Job>
                {
                }
            }
            """);
    }
}
