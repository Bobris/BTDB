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

/// <summary>Registers one hosted replication node. The application registers IReplicationNodeHost,
/// ILeaderRecordStorage, IReplicationLeaseStorage and a qualified IReplicationScheduler as singleton services.</summary>
public static class ReplicationHosting
{
    public static IServiceCollection AddBTDBReplication(this IServiceCollection services, ReplicationNodeOptions options,
        int maximumClockDriftPpm, TimeSpan safetyMargin, int maximumConcurrentPeerRequests = 32)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);
        options.ProgressTimeouts?.Validate();
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ClusterId);
        HttpReplicationPeerTransport.ValidateEndpoint(options.Endpoint);
        foreach (var duration in new[] { options.PollInterval, options.LeaseRetryInterval, options.RequestTimeout,
                     options.ConfirmationDuration, options.CompactionInterval ?? TimeSpan.FromMinutes(5) })
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(duration.Ticks);
        ArgumentOutOfRangeException.ThrowIfNegative(maximumClockDriftPpm);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(maximumClockDriftPpm, 1_000_000);
        ArgumentOutOfRangeException.ThrowIfNegative(safetyMargin.Ticks);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumConcurrentPeerRequests);
        if (services.Any(d => d.ServiceType == typeof(ReplicationNodeCoordinator)))
            throw new InvalidOperationException("Register only one replication node per host.");

        services.AddSingleton(options);
        services.AddSingleton<ReplicationStatus>();
        services.AddMetrics();
        services.AddSingleton<ReplicationMetrics>();
        services.AddHealthChecks().AddCheck<ReplicationReadinessCheck>("btdb-replication", tags: new[] { "ready" });
        services.AddSingleton(_ => new HttpReplicationPeerTransport(maximumConcurrentPeerRequests));
        services.AddSingleton(sp => new LeaseSessionController(sp.GetRequiredService<IReplicationLeaseStorage>(),
            sp.GetRequiredService<IReplicationScheduler>(), maximumClockDriftPpm, safetyMargin));
        services.AddSingleton(sp => new ReplicationNodeCoordinator(options, sp.GetRequiredService<IReplicationNodeHost>(),
            sp.GetRequiredService<ILeaderRecordStorage>(), sp.GetRequiredService<LeaseSessionController>(),
            sp.GetRequiredService<HttpReplicationPeerTransport>(), sp.GetRequiredService<IReplicationScheduler>(),
            sp.GetRequiredService<ReplicationStatus>()));
        services.AddSingleton(sp => new ReplicationApplicationData(sp.GetRequiredService<ILeaderRecordStorage>(),
            options.ClusterId, sp.GetRequiredService<LeaseSessionController>(),
            () => sp.GetRequiredService<ReplicationNodeCoordinator>().ApplicationDataLeadership()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, ReplicationHostedService>());
        return services;
    }

    public static IEndpointConventionBuilder MapBTDBReplication(this IEndpointRouteBuilder endpoints) =>
        endpoints.ServiceProvider.GetRequiredService<HttpReplicationPeerTransport>().Map(endpoints);
}

/// <summary>Starts only after the HTTP host is listening. ApplicationStopping fences synchronously even if a
/// provider ignores cancellation; the hosted task then joins coordinator cleanup without owning application databases.</summary>
internal sealed class ReplicationHostedService(ReplicationNodeCoordinator coordinator, LeaseSessionController leases,
    IHostApplicationLifetime lifetime, HttpReplicationPeerTransport transport, ReplicationStatus status, ReplicationMetrics metrics) : BackgroundService
{
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _ = metrics; // Instantiate the host-scoped instruments with the worker.
        if (!transport.IsMapped)
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
