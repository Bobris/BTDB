using System.Text.Json;
using System.Text.Json.Serialization;
using BTDB.Replication;

namespace BTDB.BackendActions;

/// <summary>Leader sessions and the request channel to the active leader; <see cref="ReplicationCluster"/> in
/// production, an in-process fake in tests.</summary>
internal interface IBackendActionsCluster
{
    Task RunWhileLeaderAsync(Func<CancellationToken, Task> action, CancellationToken cancellation);
    IDisposable Serve(string channel, Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> handler);
    ValueTask<ReadOnlyMemory<byte>> SendAsync(string channel, ReadOnlyMemory<byte> request, CancellationToken cancellation);
}

internal sealed class ReplicationCluster(ReplicationLeadership leadership, ReplicationLeaderChannel channel)
    : IBackendActionsCluster
{
    public Task RunWhileLeaderAsync(Func<CancellationToken, Task> action, CancellationToken cancellation) =>
        leadership.RunWhileLeaderAsync(action, cancellation);

    public IDisposable Serve(string name,
        Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> handler) =>
        channel.Serve(name, handler);

    public ValueTask<ReadOnlyMemory<byte>> SendAsync(string name, ReadOnlyMemory<byte> request,
        CancellationToken cancellation) => channel.SendAsync(name, request, cancellation);
}

internal sealed class AttemptMessage
{
    public ulong TenantId { get; set; }
    public ulong ActionId { get; set; }
    public ulong Attempt { get; set; }
    /// <summary>The running attempt's resource adjustment, in CPU units.</summary>
    public int CpuDelta { get; set; }

    internal ReportedAttempt ToAttempt() => new(new(TenantId, ActionId), Attempt, CpuDelta);
    internal static AttemptMessage From(QualifiedActionId id, ulong attempt, int cpuDelta = 0) =>
        new() { TenantId = id.TenantId, ActionId = id.ActionId, Attempt = attempt, CpuDelta = cpuDelta };
}

/// <summary>A worker's long poll: its state, which the coordinator reconciles with its own view.</summary>
internal sealed class ExchangeRequest
{
    public Guid WorkerSession { get; set; }
    public string NodeId { get; set; } = "";
    public int Cpu { get; set; }
    public ulong Generation { get; set; }
    public bool Draining { get; set; }
    public List<AttemptMessage> Running { get; set; } = new();
    /// <summary>Attempts whose ActionFinished this worker published and applied, until acknowledged.</summary>
    public List<AttemptMessage> Finished { get; set; } = new();
}

internal sealed class CommandMessage
{
    public WorkerCommandKind Kind { get; set; }
    public ulong TenantId { get; set; }
    public ulong ActionId { get; set; }
    public ulong Attempt { get; set; }
    public ulong StartEventId { get; set; }
}

internal sealed class ExchangeResponse
{
    public Guid CoordinatorSession { get; set; }
    public List<CommandMessage> Commands { get; set; } = new();
    public List<AttemptMessage> Acknowledged { get; set; } = new();
}

[JsonSerializable(typeof(ExchangeRequest))]
[JsonSerializable(typeof(ExchangeResponse))]
[JsonSerializable(typeof(ActionsOverview))]
internal sealed partial class ExchangeJson : JsonSerializerContext
{
    internal const string Channel = "btdb-backend-actions";
    internal const string OverviewChannel = "btdb-backend-actions-overview";

    internal static byte[] Encode(ActionsOverview overview) =>
        JsonSerializer.SerializeToUtf8Bytes(overview, Default.ActionsOverview);

    internal static ActionsOverview DecodeOverview(ReadOnlyMemory<byte> bytes)
    {
        var overview = JsonSerializer.Deserialize(bytes.Span, Default.ActionsOverview) ??
                       throw new JsonException("Empty overview.");
        // A leader older than tenant suspension sends no SuspendedTenants.
        return overview.SuspendedTenants == null ? overview with { SuspendedTenants = [] } : overview;
    }

    internal static byte[] Encode(ExchangeRequest request) =>
        JsonSerializer.SerializeToUtf8Bytes(request, Default.ExchangeRequest);

    internal static byte[] Encode(ExchangeResponse response) =>
        JsonSerializer.SerializeToUtf8Bytes(response, Default.ExchangeResponse);

    internal static ExchangeRequest DecodeRequest(ReadOnlyMemory<byte> bytes) =>
        JsonSerializer.Deserialize(bytes.Span, Default.ExchangeRequest) ?? throw new JsonException("Empty request.");

    internal static ExchangeResponse DecodeResponse(ReadOnlyMemory<byte> bytes) =>
        JsonSerializer.Deserialize(bytes.Span, Default.ExchangeResponse) ?? throw new JsonException("Empty response.");
}
