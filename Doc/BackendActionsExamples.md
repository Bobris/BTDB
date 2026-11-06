# Backend actions examples

Recipes for [`BTDB.BackendActions`](BackendActions.md): timeouts, interruptions, cancellation, partial results,
retries, and the other hooks a handler can use. The examples leave out the application's own services (`IRenderer`,
`ISearchIndex`, `HttpClient` and so on) and assume these usings:

```csharp
using BTDB;
using BTDB.BackendActions;
using BTDB.ODBLayer;
using BTDB.Replication.EventSourcing;
```

## How an attempt ends

`ExecuteAsync` can end in five ways. What happens next depends on how it ended and, if the attempt's token was
cancelled, on `context.CancellationReason`. The first reason wins: a timeout that fires before the worker stops stays
`Timeout`.

| Handler ends with | Not cancelled | `Cancelled` | `Timeout` | `Interrupted` (worker stops) |
| --- | --- | --- | --- | --- |
| returns a result | `Completed` | `Completed` | `Completed` | `Completed` |
| `ActionResult.Cancelled` | `Cancelled` | `Cancelled` | `Cancelled`, code `Timeout` | `Cancelled` |
| throws `OperationCanceledException` | `Failed`, code `Unexpected` | `Cancelled` | `Cancelled`, code `Timeout` | runs again, not counted |
| `ActionResult.Interrupted` | runs again, counted | finishes as `Cancelled` | runs again, counted | runs again, not counted |
| `ActionResult.InterruptedWith(p)` | like `Interrupted`, after `OnFinished` gets `p` as `Progress` | same | same | same |
| throws `BackendActionException` | `Failed` with its code | `Failed` with its code | `Failed` with its code | `Failed` with its code |
| throws anything else | `Failed`, code `Unexpected` | `Failed`, code `Unexpected` | `Failed`, code `Unexpected` | `Failed`, code `Unexpected` |

* "Counted" means the attempt counts toward `Planner.MaxAttempts` (5). When the limit is reached, the coordinator
  finishes the action as `Interrupted` with code `AttemptsExhausted`.
* An interruption must be intentional: the action runs again only when the handler acknowledges it, by throwing
  `OperationCanceledException` or by returning `Interrupted` or `InterruptedWith`. Any other exception fails the
  action, also while the worker stops. A library that wraps the cancellation in its own exception therefore needs a
  catch that rethrows `OperationCanceledException` or returns `ActionResult.Interrupted`.
* An `OperationCanceledException` counts as an acknowledgement only when the attempt's own token was cancelled. If it
  comes from somewhere else, such as an `HttpClient` timeout, the attempt fails; see
  [timeout of one call](#timeout-of-one-call).
* An attempt that a newer one replaced (after a lost worker, an expired lease or a leader change) is superseded:
  whatever it returns is ignored, including progress.
* `ActionResult.Cancelled` cancels the action for good, even when the reason is `Interrupted`. Return it only when
  the reason is `Cancelled` or `Timeout`, or when the handler itself decides the work should not happen.

## Timeouts

`context.CancelAfter(delay)` starts a timer for the current attempt. When it fires, `context.Cancellation` is cancelled
with reason `Timeout`. Each attempt starts its own timer, so a rerun gets the full delay again. Calling it again
replaces the timer.

### Hard timeout

To end the attempt at the deadline, let the token cancel the work. The action then finishes as `Cancelled` with
error code `Timeout`:

```csharp
[BackendAction(Cpu = 200)]
public class RenderThumbnails : ActionInput<ThumbnailResult>
{
    public ulong DocumentId { get; set; }
}

[Generate]
public class ThumbnailResult
{
    public List<string> BlobNames { get; set; } = new();
}

public class RenderThumbnailsHandler(IRenderer renderer) : IActionHandler<RenderThumbnails, ThumbnailResult>
{
    public async Task<ActionResult<ThumbnailResult>> ExecuteAsync(RenderThumbnails input, IActionContext context)
    {
        context.CancelAfter(TimeSpan.FromMinutes(10));
        var blobs = await renderer.RenderAsync(input.DocumentId, context.Cancellation); // throws at the deadline
        return new ThumbnailResult { BlobNames = blobs };
    }

    public void OnFinished(RenderThumbnails input, ActionOutcome<ThumbnailResult> outcome)
    {
        if (outcome is { Kind: ActionOutcomeKind.Cancelled, Error.Code: "Timeout" })
        {
            // Mark the document so the UI can offer a retry; a user's cancellation has no error.
        }
    }
}
```

### Soft timeout: return what is done so far

Returning a result completes the attempt, whatever the cancellation reason. A best-effort action can therefore stop
at the deadline and return what it has so far. Catch only `Timeout`, so that a cancellation or an interruption still
propagates and is acknowledged:

```csharp
public class SearchArchive : ActionInput<SearchResult>
{
    public string Query { get; set; } = "";
}

[Generate]
public class SearchResult
{
    public List<ulong> DocumentIds { get; set; } = new();
    public bool Truncated { get; set; }
}

public class SearchArchiveHandler(ISearchIndex index) : IActionHandler<SearchArchive, SearchResult>
{
    public async Task<ActionResult<SearchResult>> ExecuteAsync(SearchArchive input, IActionContext context)
    {
        context.CancelAfter(TimeSpan.FromSeconds(30));
        var result = new SearchResult();
        try
        {
            await foreach (var id in index.SearchAsync(input.Query, context.Cancellation))
                result.DocumentIds.Add(id);
        }
        catch (OperationCanceledException) when (context.CancellationReason == ActionCancellationReason.Timeout)
        {
            result.Truncated = true;
        }
        return result;
    }

    public void OnFinished(SearchArchive input, ActionOutcome<SearchResult> outcome) { /* store outcome.Result */ }
}
```

### Retry after a timeout

If a timeout means "this node is slow, try again", return `ActionResult.Interrupted`. The action runs again at once,
possibly on another node, and the attempt counts toward `MaxAttempts`:

```csharp
catch (OperationCanceledException) when (context.CancellationReason == ActionCancellationReason.Timeout)
{
    return ActionResult.Interrupted; // after MaxAttempts: Interrupted with code AttemptsExhausted
}
```

### Timeout of one call

To limit a single call instead of the whole attempt, link a token to the attempt's token. If the call's own timeout
fires, the attempt's token is not cancelled, so the resulting `OperationCanceledException` is not an acknowledgement:
without the catch below, the attempt would fail with code `Unexpected`. Convert it to an error code the application
recognizes:

```csharp
using var call = CancellationTokenSource.CreateLinkedTokenSource(context.Cancellation);
call.CancelAfter(TimeSpan.FromSeconds(20));
HttpResponseMessage response;
try
{
    response = await http.PostAsync(input.Url, new StringContent(input.Body), call.Token);
}
catch (OperationCanceledException) when (!context.Cancellation.IsCancellationRequested)
{
    throw new BackendActionException("UpstreamTimeout", "The partner API did not answer within 20 s.");
}
```

`HttpClient.Timeout` also throws `TaskCanceledException`, which is an `OperationCanceledException`, so the same catch
handles it.

## Interruptions

An attempt is interrupted when its worker stops (deploy, scale-in, host shutdown) or when a newer attempt supersedes
it. If the handler acknowledges a stopping worker's interruption, the action runs again on another node at once, and
the attempt does not count toward `MaxAttempts`. Execution is at least once, so make external effects idempotent:
they may run again.

### Do nothing special

Pass `context.Cancellation` to every awaited call. The resulting `OperationCanceledException` acknowledges the
interruption, and the next attempt starts over:

```csharp
public async Task<ActionResult<ThumbnailResult>> ExecuteAsync(RenderThumbnails input, IActionContext context)
{
    var blobs = await renderer.RenderAsync(input.DocumentId, context.Cancellation);
    return new ThumbnailResult { BlobNames = blobs };
}
```

### Finish anyway

If the attempt is almost done, it can ignore the interruption and return its result, which completes the action. A
stopping worker waits for its running handlers before it stops, so use this only for work that ends quickly:

```csharp
foreach (var page in pages)
    await RenderPageAsync(page, CancellationToken.None); // a few seconds; not worth restarting
return new ThumbnailResult { BlobNames = names };
```

### Checkpoint on interruption

`ActionResult.InterruptedWith(progress)` interrupts the attempt and commits `progress`, a `TResult`, in the same
event. `OnFinished` receives it as a `Progress` outcome, but only while the attempt is current, so a superseded
attempt can never overwrite newer progress. The application stores the progress, and the next attempt resumes from
it:

```csharp
[BackendAction(Cpu = 50, SingleRunningPerTenant = true)]
public class ImportCustomers : ActionInput<ImportResult>
{
    public string BlobName { get; set; } = "";
}

[Generate]
public class ImportResult
{
    public long RowsDone { get; set; }
}

public class ImportCheckpoint
{
    [PrimaryKey(1)] public ulong TenantId { get; set; }
    [PrimaryKey(2)] public ulong ActionId { get; set; }
    public long RowsDone { get; set; }
}

public interface IImportCheckpoints : IRelation<ImportCheckpoint>
{
    ImportCheckpoint? FindByIdOrDefault(ulong tenantId, ulong actionId);
    bool RemoveById(ulong tenantId, ulong actionId);
}

public class ImportCustomersHandler(ICustomerSource source) : IActionHandler<ImportCustomers, ImportResult>
{
    public async Task<ActionResult<ImportResult>> ExecuteAsync(ImportCustomers input, IActionContext context)
    {
        long done;
        using (context.StartQuery())
            done = Ambient.Relation<IImportCheckpoints>().FindByIdOrDefault(input.TenantId, input.ActionId)?.RowsDone
                   ?? 0;
        try
        {
            await foreach (var batch in source.ReadAsync(input.BlobName, skip: done, context.Cancellation))
            {
                await source.UpsertAsync(input.TenantId, batch, context.Cancellation); // idempotent
                done += batch.Count;
            }
        }
        catch (OperationCanceledException) when (context.CancellationReason == ActionCancellationReason.Interrupted)
        {
            return ActionResult.InterruptedWith(new ImportResult { RowsDone = done });
        }
        return new ImportResult { RowsDone = done };
    }

    public void OnFinished(ImportCustomers input, ActionOutcome<ImportResult> outcome)
    {
        var checkpoints = Ambient.Relation<IImportCheckpoints>();
        if (outcome.Kind == ActionOutcomeKind.Progress)
            checkpoints.Upsert(new()
            {
                TenantId = input.TenantId, ActionId = input.ActionId, RowsDone = outcome.Result!.RowsDone
            });
        else
            checkpoints.RemoveById(input.TenantId, input.ActionId); // final outcome: the checkpoint is done
    }
}
```

Only a graceful stop gets to checkpoint. After a crash or a lost worker, the next attempt resumes from the last
checkpoint. To limit how much work a crash can lose, also commit progress as you go with
[partial results](#partial-results).

Catch only `Interrupted`. If `InterruptedWith` is returned for a cancellation or a timeout, the attempt counts toward
`MaxAttempts`; for a timeout the action then runs again, and for a cancellation it finishes as `Cancelled`.

### Retry a transient failure at once

A handler can return `ActionResult.Interrupted` on its own, for example after a deadlock or a 503 from a dependency.
The action runs again at once and the attempt counts. For a delay between retries, use
[retry with backoff](#retry-with-backoff) instead:

```csharp
catch (HttpRequestException e) when (e.StatusCode == HttpStatusCode.ServiceUnavailable)
{
    return ActionResult.Interrupted;
}
```

## Cancellation

`Cancel(id)` in a consume, or `CancelAsync(id)` in a query scope, cancels an action. A delayed action is removed and
gets no outcome. A queued action finishes as `Cancelled` at once, with `outcome.Attempt == 0`. A started action's
token is cancelled with reason `Cancelled`:

```csharp
// API endpoint
using (publisher.StartQuery())
{
    if (!await actions.CancelAsync(new QualifiedActionId(tenantId, actionId)))
        return Results.NotFound(); // already finished
}
```

Clean up external effects in the handler, using a token that is not already cancelled. Clean up application state in
`OnFinished`, which runs deterministically on every replica:

```csharp
public async Task<ActionResult<ExportResult>> ExecuteAsync(ExportOrders input, IActionContext context)
{
    var blob = storage.Create($"export-{input.TenantId}-{input.ActionId}");
    try
    {
        await WriteAsync(blob, input, context.Cancellation);
        return new ExportResult { BlobName = blob.Name };
    }
    catch (OperationCanceledException) when (context.CancellationReason is ActionCancellationReason.Cancelled
                                                 or ActionCancellationReason.Timeout)
    {
        await blob.DeleteIfExistsAsync(CancellationToken.None); // not on Interrupted: the next attempt overwrites it
        throw;
    }
}
```

## Partial results

`context.SubmitPartialAsync(ev)` publishes one of the application's own events and returns after this node has
consumed it. Use it to:

* stream results larger than `EventPublisherOptions.MaximumEventSize` (50 MiB by default),
* make progress durable on every replica as it happens, so a crash loses at most one part,
* show progress in the UI while the action runs.

Put `context.Id` and `context.Attempt` in the event. A superseded attempt may still submit parts, so the consume
ignores a part unless the action is started with `RunAttempts` equal to the part's attempt:

```csharp
[BackendAction(Cpu = 100, MaxConcurrent = 2, ConcurrencyGroup = "exports")]
public class ExportOrders : ActionInput<ExportResult>
{
    public DateTime From { get; set; }
}

[Generate]
public class ExportResult
{
    public int Chunks { get; set; }
}

public class ExportChunk : PureEvent
{
    public ulong TenantId { get; set; }
    public ulong ActionId { get; set; }
    public ulong Attempt { get; set; }
    public int Index { get; set; }
    public ulong LastOrderId { get; set; }
    public List<string> Lines { get; set; } = new();
}

public class ExportChunkRow
{
    [PrimaryKey(1)] public ulong TenantId { get; set; }
    [PrimaryKey(2)] public ulong ActionId { get; set; }
    [PrimaryKey(3)] public int Index { get; set; }
    public ulong LastOrderId { get; set; }
    public List<string> Lines { get; set; } = new();
}

public interface IExportChunks : IRelation<ExportChunkRow>
{
    IEnumerable<ExportChunkRow> FindById(ulong tenantId, ulong actionId);
    int RemoveById(ulong tenantId, ulong actionId);
}

// The application's handler of the part, registered with the EventDispatcher like any other.
public class ExportChunkHandler(BackendActions actions) : IEventHandler<ExportChunk>
{
    public void Validate(ExportChunk ev) { }

    public void Consume(ExportChunk ev)
    {
        if (actions.FindAction(new(ev.TenantId, ev.ActionId)) is not { State: ActionState.Started } action ||
            action.RunAttempts != ev.Attempt)
            return; // a part of a superseded attempt
        Ambient.Relation<IExportChunks>().Upsert(new()
        {
            TenantId = ev.TenantId, ActionId = ev.ActionId, Index = ev.Index, LastOrderId = ev.LastOrderId,
            Lines = ev.Lines
        });
    }
}

public class ExportOrdersHandler(IOrderReader orders) : IActionHandler<ExportOrders, ExportResult>
{
    public async Task<ActionResult<ExportResult>> ExecuteAsync(ExportOrders input, IActionContext context)
    {
        // Resume after the last committed chunk, which a previous attempt may have left.
        int index;
        ulong after;
        using (context.StartQuery())
        {
            var last = Ambient.Relation<IExportChunks>().FindById(input.TenantId, input.ActionId).LastOrDefault();
            (index, after) = last is null ? (0, 0UL) : (last.Index + 1, last.LastOrderId);
        }
        await foreach (var page in orders.ReadPagesAsync(input.TenantId, input.From, after, context.Cancellation))
        {
            await context.SubmitPartialAsync(new ExportChunk
            {
                TenantId = context.Id.TenantId, ActionId = context.Id.ActionId, Attempt = context.Attempt,
                Index = index++, LastOrderId = page.LastId, Lines = page.Lines
            });
        }
        return new ExportResult { Chunks = index }; // small: the data travelled in the parts
    }

    public void OnFinished(ExportOrders input, ActionOutcome<ExportResult> outcome)
    {
        if (outcome.Kind == ActionOutcomeKind.Completed) return; // keep the chunks for download
        Ambient.Relation<IExportChunks>().RemoveById(input.TenantId, input.ActionId); // failed or cancelled
    }
}
```

`SubmitPartialAsync` waits until this node has consumed the part, so parts stay in order, and a slow consumer slows
the action down instead of piling up events. When the attempt is cancelled, the wait throws
`OperationCanceledException`, which acknowledges the cancellation like any other.

The same pattern works for progress shown in the UI: submit an event with a percentage and store it in a small row
that the UI reads in a query scope.

## Failures

Throw `BackendActionException(code, message, details)` for failures the application expects. `OnFinished` receives
`Failed` with `outcome.Error.Code`, `Message` and `Details`. Any other exception fails with code `Unexpected`, with the
exception's `ToString()` in `Details`. A failed action is not retried automatically.

```csharp
if (!await storage.ExistsAsync(input.SourceBlob, context.Cancellation))
    throw new BackendActionException("SourceMissing", $"Blob {input.SourceBlob} does not exist.");
```

### Retry with backoff

To retry later, enqueue a new input from `OnFinished`. It runs inside the consume of the finishing event, so it can
call `Enqueue` with a `startAt` derived from that event's time, which keeps the retry deterministic on every replica.
Create a new input object: the finishing input still carries its `ActionId`, and reusing it would collide with the
action being finished.

```csharp
[BackendAction(Cpu = 10, MaxConcurrent = 20)]
public class SendWebhook : ActionInput<WebhookResult>
{
    public string Url { get; set; } = "";
    public string Body { get; set; } = "";
    public int Retry { get; set; }
}

[Generate]
public class WebhookResult
{
    public int StatusCode { get; set; }
}

public class SendWebhookHandler(BackendActions actions, HttpClient http) : IActionHandler<SendWebhook, WebhookResult>
{
    const int MaxRetries = 6;

    public async Task<ActionResult<WebhookResult>> ExecuteAsync(SendWebhook input, IActionContext context)
    {
        context.CancelAfter(TimeSpan.FromMinutes(1));
        HttpResponseMessage response;
        try
        {
            response = await http.PostAsync(input.Url, new StringContent(input.Body), context.Cancellation);
        }
        catch (HttpRequestException e)
        {
            throw new BackendActionException("Transient", e.Message);
        }
        if ((int)response.StatusCode >= 500)
            throw new BackendActionException("Transient", $"HTTP {(int)response.StatusCode}");
        return new WebhookResult { StatusCode = (int)response.StatusCode };
    }

    public void OnFinished(SendWebhook input, ActionOutcome<WebhookResult> outcome)
    {
        var transient = outcome is { Kind: ActionOutcomeKind.Failed, Error.Code: "Transient" } ||
                        outcome is { Kind: ActionOutcomeKind.Cancelled, Error.Code: "Timeout" };
        if (!transient || input.Retry >= MaxRetries) return;
        var when = Ambient.CurrentEvent!.When + TimeSpan.FromSeconds(30 << input.Retry); // 30 s, 1 min, 2 min, ...
        actions.Enqueue(new SendWebhook
        {
            TenantId = input.TenantId, Url = input.Url, Body = input.Body, Retry = input.Retry + 1
        }, startAt: when);
    }
}
```

The retry is a new action with a new ID. If callers track the original ID, store a mapping in `OnFinished`, or key
the application's status row by a business ID instead of the action ID.

## Keep the outcome for later

`OnFinished` is the only place that sees the outcome, and the action's row is gone after it. To show the result to a
user or an API caller, store it in the application's own relation:

```csharp
public class ReportStatus
{
    [PrimaryKey(1)] public ulong TenantId { get; set; }
    [PrimaryKey(2)] public ulong ReportId { get; set; }
    public ActionOutcomeKind Kind { get; set; }
    public string? BlobName { get; set; }
    public string? ErrorCode { get; set; }
    public DateTime FinishedAt { get; set; }
}

public interface IReportStatuses : IRelation<ReportStatus>
{
    ReportStatus? FindByIdOrDefault(ulong tenantId, ulong reportId);
}

public void OnFinished(GenerateReport input, ActionOutcome<ReportResult> outcome)
{
    if (outcome.Kind == ActionOutcomeKind.Progress) return;
    Ambient.Relation<IReportStatuses>().Upsert(new()
    {
        TenantId = input.TenantId, ReportId = input.ReportId, Kind = outcome.Kind,
        BlobName = outcome.Result?.BlobName, ErrorCode = outcome.Error?.Code,
        FinishedAt = Ambient.CurrentEvent!.When // deterministic, unlike DateTime.UtcNow
    });
}
```

```csharp
// API endpoint, on any node
using (publisher.StartQuery())
{
    if (Ambient.Relation<IReportStatuses>().FindByIdOrDefault(tenantId, reportId) is { } done)
        return Results.Ok(done);
    if (actions.FindAction(id) is { } pending) // Delayed, Queued or Started, with RunAttempts and WorkerNode
        return Results.Accepted(value: pending.State.ToString());
    return Results.NotFound();
}
```

## Chain actions into a workflow

An input enqueued from `OnFinished` belongs to the same transaction as the outcome, so the next step is queued
exactly once, on every replica:

```csharp
public void OnFinished(ImportCustomers input, ActionOutcome<ImportResult> outcome)
{
    if (outcome.Kind != ActionOutcomeKind.Completed) return;
    actions.Enqueue(new RebuildCustomerIndex { TenantId = input.TenantId });
    actions.Enqueue(new SendWebhook { TenantId = input.TenantId, Url = "...", Body = "imported" },
        ActionPriority.Low);
}
```

The finishing action's unification key is already free inside `OnFinished`, so an input with the same key starts a
new action instead of merging into the one that is finishing.

## Merge repeated requests

### Unificator: one pending run per tenant

When many events ask for the same work, merge them into one queued action. An empty key merges every input of the
type within one tenant. A different key per entity gives one pending run per entity:

```csharp
public class RefreshTenantStatistics : ActionInput<StatisticsResult>, IActionInputUnificator
{
    public byte[]? GetUniqueKey() => []; // one pending refresh per tenant
}
```

`Enqueue` returns the ID of the queued action the input merged into. After that action starts, a new input creates a
new action, so a request never gets a result computed before it was made.

### Aggregator: batch the requests

An aggregator merges the content of the new input into the queued one. Returning `false` closes the batch, and the
new input starts the next one:

```csharp
public class ReindexDocuments : ActionInput<ReindexResult>, IActionInputAggregator
{
    public List<ulong> DocumentIds { get; set; } = new();

    public byte[]? GetUniqueKey() => [];

    public bool TryAggregate(IActionInputAggregator other)
    {
        var ids = ((ReindexDocuments)other).DocumentIds;
        if (DocumentIds.Count + ids.Count > 10_000) return false; // keep batches bounded
        DocumentIds.AddRange(ids);
        return true;
    }
}
```

`GetUniqueKey` and `TryAggregate` run inside consumes, so they must be deterministic.

## Validate and mark state when the action starts

`IActionWithBeforeStart<TInput>.OnBeforeStart` runs in the consume that records the start, before any worker
executes it. If it throws, the action finishes as `Failed` with code `BeforeStartFailed`, and writes made before the
exception stay, so validate before writing:

```csharp
public class ConvertDocumentHandler : IActionHandler<ConvertDocument, ConvertResult>,
    IActionWithBeforeStart<ConvertDocument>
{
    public void OnBeforeStart(ConvertDocument input)
    {
        var docs = Ambient.Relation<IDocuments>();
        var doc = docs.FindByIdOrDefault(input.TenantId, input.DocumentId)
                  ?? throw new InvalidOperationException("The document was deleted.");
        doc.State = DocumentState.Converting; // visible on every replica, together with the start
        docs.Update(doc);
    }

    // ExecuteAsync and OnFinished as usual; OnFinished sets the final state.
}
```

## Adjust accounted CPU per phase

`[BackendAction(Cpu)]` is what the coordinator reserves on a worker when the action starts. If one phase is much
lighter than the others, lower the accounted CPU during it so other actions can use the room. `AdjustResources`
deltas add up, the result never goes below 10, and it takes effect with the worker's next exchange. It changes only
the accounting; raising it again does not wait for room.

```csharp
[BackendAction(Cpu = 400)] // four cores while compressing
public class ArchiveTenant : ActionInput<ArchiveResult> { }

// in ExecuteAsync
context.AdjustResources(-390);                         // downloading: mostly waiting on I/O
var files = await DownloadAllAsync(input, context.Cancellation);
context.AdjustResources(+390);                         // back to 400
return await CompressAsync(files, context.Cancellation);
```

## Scheduled actions with a gate

A scheduled type is started by the coordinator with tenant 0. Its handler can skip occurrences with
`IScheduledActionGate`. The gate is evaluated on the leader, outside any consume:

```csharp
[DailyScheduled(150)] // 02:30 UTC
[BackendAction(Priority = ActionPriority.Idle, MaxConcurrent = 1)]
public class PurgeExpiredSessions : ActionInput<PurgeResult> { }

public class PurgeExpiredSessionsHandler : IActionHandler<PurgeExpiredSessions, PurgeResult>, IScheduledActionGate
{
    public bool CanStartScheduled(DateTime now) => now.DayOfWeek is not (DayOfWeek.Saturday or DayOfWeek.Sunday);

    // ExecuteAsync and OnFinished as usual.
}
```

When a scheduled job has work for every tenant, let its `OnFinished` read the tenants from the database and enqueue
one action per tenant. Fairness and `SingleRunningPerTenant` then apply per tenant.

## Test timeouts, interruptions and parts

`TestActionContext` runs `ExecuteAsync` without a cluster. `Cancel(reason)` simulates any cancellation reason,
`Timeout` returns the delay passed to `CancelAfter`, and `Partials` lists the submitted events:

```csharp
[Fact]
public async Task SoftTimeoutReturnsWhatWasFound()
{
    var context = new TestActionContext(new QualifiedActionId(1, 7));
    var run = new SearchArchiveHandler(new SlowIndex()).ExecuteAsync(new SearchArchive { Query = "x" }, context);
    Assert.Equal(TimeSpan.FromSeconds(30), context.Timeout);
    context.Cancel(ActionCancellationReason.Timeout);
    var result = await run;
    Assert.Equal(ActionResultKind.Completed, result.Kind);
    Assert.True(result.Result!.Truncated);
}

[Fact]
public async Task InterruptedImportCommitsItsProgress()
{
    var context = new TestActionContext(new QualifiedActionId(1, 8), publisher: publisher); // StartQuery needs it
    var run = new ImportCustomersHandler(source).ExecuteAsync(new ImportCustomers { TenantId = 1, ActionId = 8 },
        context);
    await source.FirstBatchRead;
    context.Cancel(ActionCancellationReason.Interrupted);
    var result = await run;
    Assert.Equal(ActionResultKind.Interrupted, result.Kind);
    Assert.True(result.Result!.RowsDone > 0);
}
```

Test `OnFinished`, the handlers of submitted parts and the merging of inputs through an `EventPublisher`, like any
other consume.
