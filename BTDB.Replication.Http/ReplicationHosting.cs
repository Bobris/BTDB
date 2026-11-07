using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace BTDB.Replication.Http;

/// <summary>Registers one hosted replication node. The application registers IReplicationNodeHost and
/// IReplicationLeaderStorage as singleton services; IReplicationScheduler defaults to
/// <see cref="SystemReplicationScheduler"/> unless the application registers another qualified one.</summary>
public static class ReplicationHosting
{
    public static IServiceCollection AddBTDBReplication(this IServiceCollection services, ReplicationNodeOptions options,
        int maximumClockDriftPpm, TimeSpan safetyMargin, int maximumConcurrentPeerRequests = 32)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        HttpReplicationPeerTransport.ValidateEndpoint(options.Endpoint);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumConcurrentPeerRequests);
        return AddNode(services, options, maximumClockDriftPpm, safetyMargin,
            _ => new HttpReplicationPeerTransport(maximumConcurrentPeerRequests));
    }

    /// <summary>Registers a node that is the whole cluster, for local development and tests: the same services as
    /// <see cref="AddBTDBReplication"/>, but no peer endpoint, so it needs no ASP.NET Core server and no
    /// <see cref="MapBTDBReplication"/>. Pair it with storage that one process owns, such as
    /// <see cref="FileSystemLeaderStorage"/>; on storage shared with other nodes it could lead without followers being
    /// able to reach it. <see cref="ReplicationNodeOptions.Endpoint"/> is only an identity here, for example
    /// "single-node". Moving to a cluster replaces this call with <see cref="AddBTDBReplication"/>.</summary>
    public static IServiceCollection AddBTDBReplicationSingleNode(this IServiceCollection services,
        ReplicationNodeOptions options, int maximumClockDriftPpm, TimeSpan safetyMargin)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Endpoint);
        return AddNode(services, options, maximumClockDriftPpm, safetyMargin,
            _ => new SingleNodeReplicationPeerTransport());
    }

    static IServiceCollection AddNode(IServiceCollection services, ReplicationNodeOptions options,
        int maximumClockDriftPpm, TimeSpan safetyMargin, Func<IServiceProvider, IReplicationPeerTransport> transport)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maximumClockDriftPpm);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(maximumClockDriftPpm, 1_000_000);
        ArgumentOutOfRangeException.ThrowIfNegative(safetyMargin.Ticks);
        if (services.Any(d => d.ServiceType == typeof(ReplicationNodeCoordinator)))
            throw new InvalidOperationException("Register only one replication node per host.");

        services.AddSingleton(options);
        services.TryAddSingleton<IReplicationScheduler, SystemReplicationScheduler>();
        services.AddSingleton<ReplicationStatus>();
        services.AddSingleton<ReplicationLeadership>();
        services.AddSingleton<ReplicationPresence>();
        services.AddSingleton<ReplicationLeaderChannel>();
        services.AddMetrics();
        services.AddSingleton<ReplicationMetrics>();
        services.AddHealthChecks().AddCheck<ReplicationReadinessCheck>("btdb-replication", tags: new[] { "ready" });
        services.AddSingleton(transport);
        services.AddSingleton(sp => new LeaseSessionController(sp.GetRequiredService<IReplicationLeaderStorage>(),
            sp.GetRequiredService<IReplicationScheduler>(), maximumClockDriftPpm, safetyMargin));
        services.AddSingleton(sp => new ReplicationNodeCoordinator(options, sp.GetRequiredService<IReplicationNodeHost>(),
            sp.GetRequiredService<IReplicationLeaderStorage>(), sp.GetRequiredService<LeaseSessionController>(),
            sp.GetRequiredService<IReplicationPeerTransport>(), sp.GetRequiredService<IReplicationScheduler>(),
            sp.GetRequiredService<ReplicationStatus>(), sp.GetRequiredService<ReplicationLeadership>(),
            sp.GetRequiredService<ReplicationPresence>(), sp.GetRequiredService<ReplicationLeaderChannel>()));
        services.AddSingleton(sp => new ReplicationApplicationData(sp.GetRequiredService<IReplicationLeaderStorage>(),
            options.ClusterId, sp.GetRequiredService<LeaseSessionController>(),
            () => sp.GetRequiredService<ReplicationNodeCoordinator>().ApplicationDataLeadership()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, ReplicationHostedService>());
        return services;
    }

    public static IEndpointConventionBuilder MapBTDBReplication(this IEndpointRouteBuilder endpoints) =>
        endpoints.ServiceProvider.GetService<IReplicationPeerTransport>() is HttpReplicationPeerTransport transport
            ? transport.Map(endpoints)
            : throw new InvalidOperationException(
                "MapBTDBReplication needs AddBTDBReplication; a single-node registration has no peer endpoint.");
}

/// <summary>Starts only after the host started (an HTTP host is then listening). ApplicationStopping fences synchronously even if a
/// provider ignores cancellation; the hosted task then joins coordinator cleanup without owning application databases.</summary>
internal sealed class ReplicationHostedService(ReplicationNodeCoordinator coordinator, LeaseSessionController leases,
    IHostApplicationLifetime lifetime, IReplicationPeerTransport transport, ReplicationStatus status, ReplicationMetrics metrics) : BackgroundService
{
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _ = metrics; // Instantiate the host-scoped instruments with the worker.
        if (transport is HttpReplicationPeerTransport { IsMapped: false })
            throw new InvalidOperationException("Call MapBTDBReplication before starting the host.");
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, lifetime.ApplicationStopping);
        using var fence = stopping.Token.Register(() => { status.Stop(); leases.Close(); });
        try
        {
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using (lifetime.ApplicationStarted.Register(() => started.TrySetResult()))
                await started.Task.WaitAsync(stopping.Token).ConfigureAwait(false);
            await coordinator.RunAsync(stopping.Token).ConfigureAwait(false);
            // A requested canonical rebuild ends this node session. Do not leave a healthy-looking HTTP host behind.
            if (!stopping.IsCancellationRequested) lifetime.StopApplication();
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested) { }
        catch
        {
            leases.Close();
            lifetime.StopApplication();
            throw; // Remains visible through BackgroundService, including when the host's policy is Ignore.
        }
        finally { leases.Close(); }
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        status.Stop();
        leases.Close();
        return base.StopAsync(cancellationToken);
    }
}

/// <summary>Local availability only; remote publication and live confirmation are not readiness prerequisites.</summary>
internal sealed class ReplicationReadinessCheck(ReplicationStatus status) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,
        CancellationToken cancellationToken = default) => Task.FromResult(status.Current.Ready
        ? HealthCheckResult.Healthy()
        : HealthCheckResult.Unhealthy("Replication restore, activation, detachment or shutdown prevents readiness."));
}
