using System.Net;
using System.Net.Sockets;
using Azure.Storage;
using Azure.Storage.Blobs;
using BTDB.Replication;
using BTDB.Replication.Azure;
using BTDB.Replication.EventLog;
using BTDB.Replication.Http;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace BTDB.BackendActions.ProcessTests;

/// <summary>A loopback-only qualification node: replication, EventLog, EventSourcing and BackendActions over Azurite,
/// with test controls under /test.</summary>
internal static class Program
{
    public static async Task Main(string[] args)
    {
        if (args.Length != 2 || args[0] != "--node" || !Uri.TryCreate(args[1], UriKind.Absolute, out var containerUri) ||
            !(containerUri.IsLoopback && containerUri.Scheme == "http"))
            throw new ArgumentException("This test executable accepts only --node <loopback Azurite container URI>.");
        using var reservation = new TcpListener(IPAddress.Loopback, 0);
        reservation.Start();
        var endpoint = $"http://127.0.0.1:{((IPEndPoint)reservation.LocalEndpoint).Port}";
        reservation.Stop();
        var nodeId = Environment.GetEnvironmentVariable("BTDB_TEST_NODE_ID") ??
            throw new ArgumentException("BTDB_TEST_NODE_ID must name the node.");
        var directory = Environment.GetEnvironmentVariable("BTDB_TEST_DATA_DIRECTORY") ??
            throw new ArgumentException("BTDB_TEST_DATA_DIRECTORY must name the node's local storage directory.");
        var options = new BlobClientOptions();
        options.Retry.MaxRetries = 0;
        var key = new StorageSharedKeyCredential("test", Convert.ToBase64String(new byte[32]));
        BlobContainerClient Connect() => new(containerUri, key, options);
        const string initial = """{"format":1,"clusterId":"cluster","term":0,"revision":0,"applicationGeneration":0,"databaseNames":[]} """;
        var leaderStorage = new AzureLeaderStorage(Connect().GetBlobClient("leader.json"), TimeSpan.FromSeconds(15), initial);
        var actionOptions = new BackendActionsOptions
        {
            NodeId = nodeId, WorkerCpu = 400, ApplicationGeneration = 1, PollTimeout = TimeSpan.FromSeconds(1),
            RetryDelay = TimeSpan.FromMilliseconds(100), PlanInterval = TimeSpan.FromMilliseconds(50),
            Planner = new() { Lease = TimeSpan.FromSeconds(6), WorkerTimeout = TimeSpan.FromSeconds(3) }
        };
        await using var node = new ActionsNode(nodeId, endpoint, Connect(), directory, actionOptions);
        node.RegisterActions();

        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole().SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.UseKestrel().UseUrls(endpoint);
        builder.Services.AddSingleton<IReplicationNodeHost>(node);
        builder.Services.AddSingleton<IReplicationScheduler>(new SystemReplicationScheduler());
        builder.Services.AddSingleton<IReplicationLeaderStorage>(leaderStorage);
        builder.Services.AddSingleton<IEventLogStorage>(new AzureEventLogStorage(Connect(), "events"));
        builder.Services.AddBTDBReplication(new("cluster", endpoint, TimeSpan.FromMilliseconds(50),
            TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1), 1),
            1000, TimeSpan.FromMilliseconds(250));
        builder.Services.AddBTDBEventLog(endpoint, "cluster-event-log-key");
        await using var app = builder.Build();
        app.MapBTDBReplication();
        app.MapBTDBEventLog();
        app.MapGet("/test/state", (ReplicationStatus status) => node.State(status));
        app.MapPost("/test/enqueue/{count:int}/{milliseconds:int}", async (int count, int milliseconds) =>
            (await node.EnqueueAsync(count, milliseconds)).Select(id => id.ActionId).ToArray());
        app.MapGet("/test/finishes", () => node.Finishes());
        app.MapGet("/test/executions", () => node.Executions.ToArray());
        node.Attach(app.Services);
        await app.StartAsync();
        Console.WriteLine("READY " + endpoint);
        await app.WaitForShutdownAsync();
    }
}
