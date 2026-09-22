using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace BTDB.Replication.Http;

/// <summary>Internal composition while the application/storage contracts are still internal. The host registers
/// IReplicationNodeHost, ILeaderRecordStorage, IReplicationLeaseStorage and a qualified IReplicationScheduler.</summary>
internal static class ReplicationHosting
{
    public static IServiceCollection AddBTDBReplication(this IServiceCollection services, ReplicationNodeOptions options,
        int maximumClockDriftPpm, TimeSpan safetyMargin, int maximumConcurrentPeerRequests = 32)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);
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
        services.AddSingleton(_ => new HttpReplicationPeerTransport(maximumConcurrentPeerRequests));
        services.AddSingleton(sp => new LeaseSessionController(sp.GetRequiredService<IReplicationLeaseStorage>(),
            sp.GetRequiredService<IReplicationScheduler>(), maximumClockDriftPpm, safetyMargin));
        services.AddSingleton(sp => new ReplicationNodeCoordinator(options, sp.GetRequiredService<IReplicationNodeHost>(),
            sp.GetRequiredService<ILeaderRecordStorage>(), sp.GetRequiredService<LeaseSessionController>(),
            sp.GetRequiredService<HttpReplicationPeerTransport>(), sp.GetRequiredService<IReplicationScheduler>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, ReplicationHostedService>());
        return services;
    }

    public static IEndpointConventionBuilder MapBTDBReplication(this IEndpointRouteBuilder endpoints) =>
        endpoints.ServiceProvider.GetRequiredService<HttpReplicationPeerTransport>().Map(endpoints);
}

/// <summary>Starts only after the HTTP host is listening. ApplicationStopping fences synchronously even if a
/// provider ignores cancellation; the hosted task then joins coordinator cleanup without owning application databases.</summary>
internal sealed class ReplicationHostedService(ReplicationNodeCoordinator coordinator, LeaseSessionController leases,
    IHostApplicationLifetime lifetime, HttpReplicationPeerTransport transport) : BackgroundService
{
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        if (!transport.IsMapped)
            throw new InvalidOperationException("Call MapBTDBReplication before starting the host.");
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, lifetime.ApplicationStopping);
        using var fence = stopping.Token.Register(leases.Close);
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
        leases.Close();
        return base.StopAsync(cancellationToken);
    }
}
