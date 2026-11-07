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
        var maximumMessageBytes = HttpEventLogPeerTransport.MaximumMessageBytes(options);
        return AddService(services, endpoint, options, _ => new HttpEventLogPeerTransport(apiKey, maximumMessageBytes));
    }

    /// <summary>Registers the event log of a node that is the whole cluster, for local development and tests: no peer
    /// endpoint, no ASP.NET Core server and no <see cref="MapBTDBEventLog"/>. Pair it with storage that one process
    /// owns, such as <see cref="FileSystemEventLogStorage"/>. Its fixed identity takes over the topics of its previous
    /// process at once. Moving to a cluster replaces this call with <see cref="AddBTDBEventLog"/>.</summary>
    public static IServiceCollection AddBTDBEventLogSingleNode(this IServiceCollection services,
        EventLogOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        return AddService(services, SingleNode.Endpoint, options ?? new EventLogOptions(),
            _ => new SingleNodeEventLogPeerTransport());
    }

    static IServiceCollection AddService(IServiceCollection services, string endpoint, EventLogOptions options,
        Func<IServiceProvider, IEventLogPeerTransport> transport)
    {
        if (services.Any(d => d.ServiceType == typeof(EventLogService)))
            throw new InvalidOperationException("Register only one event log per host.");
        services.TryAddSingleton<IReplicationScheduler, SystemReplicationScheduler>();
        services.AddSingleton(transport);
        services.AddSingleton(sp => new EventLogService(sp.GetRequiredService<IEventLogStorage>(),
            sp.GetRequiredService<IEventLogPeerTransport>(), endpoint, sp.GetRequiredService<IReplicationScheduler>(),
            options));
        services.AddSingleton<IEventLog>(sp => sp.GetRequiredService<EventLogService>());
        return services;
    }

    public static IEndpointConventionBuilder MapBTDBEventLog(this IEndpointRouteBuilder endpoints) =>
        endpoints.ServiceProvider.GetService<IEventLogPeerTransport>() is HttpEventLogPeerTransport transport
            ? transport.Map(endpoints, endpoints.ServiceProvider.GetRequiredService<EventLogService>())
            : throw new InvalidOperationException(
                "MapBTDBEventLog needs AddBTDBEventLog; a single-node registration has no peer endpoint.");
}
