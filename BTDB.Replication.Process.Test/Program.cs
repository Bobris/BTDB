using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Blobs;
using BTDB.Replication.Azure;
using BTDB.Replication.Http;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;

namespace BTDB.Replication.ProcessTests;

/// <summary>Test-only network partition: Storage fails every Blob request of this node before it is sent, Peers drops
/// every incoming replication connection.</summary>
sealed class Partition : HttpPipelinePolicy
{
    public volatile bool Storage, Peers;
    // BTDB_TEST_READ_DELAY_MILLISECONDS slows every Blob read, so a test can stop a node in the middle of a restore.
    readonly TimeSpan _readDelay = TimeSpan.FromMilliseconds(
        int.Parse(Environment.GetEnvironmentVariable("BTDB_TEST_READ_DELAY_MILLISECONDS") ?? "0"));

    public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
    {
        if (Storage) throw new IOException("Test partition: Blob storage is unreachable.");
        if (_readDelay > TimeSpan.Zero && message.Request.Method == RequestMethod.Get) await Task.Delay(_readDelay);
        await ProcessNextAsync(message, pipeline);
    }

    public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline) =>
        throw new NotSupportedException("The adapters use only asynchronous requests.");
}

internal static class Program
{
    public static async Task Main(string[] args)
    {
        if (args.Length != 2 || args[0] != "--node" || !Uri.TryCreate(args[1], UriKind.Absolute, out var containerUri) ||
            (containerUri.Scheme != "https" && !(containerUri.IsLoopback && containerUri.Scheme == "http")))
            throw new ArgumentException(
                "This test executable accepts only --node <loopback Azurite or live Azure https container URI>.");
        using var reservation = new TcpListener(IPAddress.Loopback, 0);
        reservation.Start();
        var endpoint = $"http://127.0.0.1:{((IPEndPoint)reservation.LocalEndpoint).Port}";
        reservation.Stop();
        var options = new BlobClientOptions();
        options.Retry.MaxRetries = 0;
        var partition = new Partition();
        options.AddPolicy(partition, HttpPipelinePosition.PerCall);
        // Live Azure (https) authenticates like a production node; Azurite uses the fixture's fixed account key.
        var live = containerUri.Scheme == "https" ? new DefaultAzureCredential() : null;
        var key = new StorageSharedKeyCredential("test", Convert.ToBase64String(new byte[32]));
        BlobContainerClient Connect() => live != null ? new(containerUri, live, options) : new(containerUri, key, options);
        var container = Connect();
        var authorityContainer = Connect();
        const string initial = """{"format":1,"clusterId":"cluster","term":0,"revision":0,"applicationGeneration":0,"databaseNames":[]} """;
        var storage = new AzureLeaderStorage(authorityContainer.GetBlobClient("leader.json"), TimeSpan.FromSeconds(15), initial);
        var dataDirectory = Environment.GetEnvironmentVariable("BTDB_TEST_DATA_DIRECTORY") ??
            throw new ArgumentException("BTDB_TEST_DATA_DIRECTORY must name the node's local storage directory.");
        var generation = ulong.Parse(Environment.GetEnvironmentVariable("BTDB_TEST_GENERATION") ?? "1");
        var names = (Environment.GetEnvironmentVariable("BTDB_TEST_DATABASES") ?? "main").Split(',');
        await using var node = new TestNodeHost(endpoint, container, dataDirectory, generation, names,
            Environment.GetEnvironmentVariable("BTDB_TEST_APPLICATION") == "objectdb");
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole().SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.UseKestrel().UseUrls(endpoint);
        builder.Services.AddSingleton<IReplicationNodeHost>(node);
        builder.Services.AddSingleton<IReplicationScheduler>(new SystemReplicationScheduler());
        builder.Services.AddSingleton<IReplicationLeaseStorage>(storage);
        builder.Services.AddSingleton<ILeaderRecordStorage>(storage);
        var progressMilliseconds = Environment.GetEnvironmentVariable("BTDB_TEST_PROGRESS_TIMEOUT_MILLISECONDS");
        var progressTimeouts = progressMilliseconds == null ? null : new ReplicationProgressTimeouts(
            TimeSpan.FromSeconds(20), TimeSpan.FromMilliseconds(int.Parse(progressMilliseconds)), TimeSpan.FromSeconds(1));
        builder.Services.AddBTDBReplication(new("cluster", endpoint, TimeSpan.FromMilliseconds(50),
            TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1), generation, ProgressTimeouts: progressTimeouts),
            1000, TimeSpan.FromMilliseconds(250)); // The recommended production drift bound and margin.
        await using var app = builder.Build();
        // A peer partition drops every replication connection to this node; test controls stay reachable.
        app.Use(async (context, next) =>
        {
            if (partition.Peers && !context.Request.Path.StartsWithSegments("/test")) context.Abort();
            else await next(context);
        });
        app.MapBTDBReplication();
        // Test-only controls exist only in this non-packable executable, bound to loopback.
        app.MapGet("/test/state", (ReplicationStatus status, string? db) => node.Status(status, db ?? names[0]));
        app.MapPost("/test/apply/{id:long}/{value:int}", async (long id, int value, int? kb, string? db) =>
        {
            await node.ApplyAsync(db ?? names[0], checked((ulong)id), checked((byte)value), kb ?? 0);
            return Results.NoContent();
        });
        app.MapPost("/test/orders/{from:long}/{count:int}", async (long from, int count, string? db) =>
        {
            await node.ApplyOrdersAsync(db ?? names[0], checked((ulong)from), count);
            return Results.NoContent();
        });
        app.MapGet("/test/orders", (string? db) => node.OrdersAsync(db ?? names[0]));
        app.MapPost("/test/pause-publication", () => { node.PausePublication(); return Results.NoContent(); });
        app.MapPost("/test/prepare-upgrade", () => { node.PrepareUpgrade(); return Results.NoContent(); });
        app.MapPost("/test/partition/{target}/{enabled:bool}", (string target, bool enabled) =>
        {
            if (target == "storage") partition.Storage = enabled;
            else if (target == "peers") partition.Peers = enabled;
            else return Results.NotFound();
            return Results.NoContent();
        });
        await app.StartAsync();
        Console.WriteLine("READY " + endpoint);
        await app.WaitForShutdownAsync();
        // Report why the replication worker stopped the host; its logger may not flush before exit.
        foreach (var service in app.Services.GetServices<IHostedService>())
            if (service is BackgroundService { ExecuteTask: { IsFaulted: true } failed })
                Console.Error.WriteLine("WORKER FAILED " + failed.Exception!.GetBaseException());
    }
}
