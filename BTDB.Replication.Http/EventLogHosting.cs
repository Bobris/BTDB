using System;
using System.Linq;
using BTDB.Replication.EventLog;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace BTDB.Replication.Http;

/// <summary>Registers this node's event log. The application registers an <see cref="IEventLogStorage"/> singleton, for
/// example AzureEventLogStorage; IReplicationScheduler defaults to <see cref="SystemReplicationScheduler"/>.</summary>
public static class EventLogHosting
{
    /// <param name="endpoint">This node's HTTPS origin as peers reach it (HTTP only on loopback).</param>
    /// <param name="apiKey">Bearer key shared by all nodes of the cluster.</param>
    public static IServiceCollection AddBTDBEventLog(this IServiceCollection services, string endpoint, string apiKey,
        EventLogOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        HttpReplicationPeerTransport.ValidateEndpoint(endpoint);
        options ??= new EventLogOptions();
        if (services.Any(d => d.ServiceType == typeof(EventLogService)))
            throw new InvalidOperationException("Register only one event log per host.");
        services.TryAddSingleton<IReplicationScheduler, SystemReplicationScheduler>();
        var maximumMessageBytes = (int)Math.Min(int.MaxValue - 1024L, options.MaxRecordSize + 8L * 1024 * 1024);
        services.AddSingleton(_ => new HttpEventLogPeerTransport(apiKey, maximumMessageBytes));
        services.AddSingleton(sp => new EventLogService(sp.GetRequiredService<IEventLogStorage>(),
            sp.GetRequiredService<HttpEventLogPeerTransport>(), endpoint, sp.GetRequiredService<IReplicationScheduler>(),
            options));
        services.AddSingleton<IEventLog>(sp => sp.GetRequiredService<EventLogService>());
        return services;
    }

    public static IEndpointConventionBuilder MapBTDBEventLog(this IEndpointRouteBuilder endpoints) =>
        endpoints.ServiceProvider.GetRequiredService<HttpEventLogPeerTransport>()
            .Map(endpoints, endpoints.ServiceProvider.GetRequiredService<EventLogService>());
}
