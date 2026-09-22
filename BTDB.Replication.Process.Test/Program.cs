using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
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

internal static class Program
{
    public static async Task Main(string[] args)
    {
        if (args.Length != 2 || args[0] != "--node" || !Uri.TryCreate(args[1], UriKind.Absolute, out var containerUri) ||
            !containerUri.IsLoopback || containerUri.Scheme != "http")
            throw new ArgumentException("This test executable accepts only --node <loopback Azurite container URI>.");
        using var reservation = new TcpListener(IPAddress.Loopback, 0);
        reservation.Start();
        var endpoint = $"http://127.0.0.1:{((IPEndPoint)reservation.LocalEndpoint).Port}";
        reservation.Stop();
        var options = new BlobClientOptions();
        options.Retry.MaxRetries = 0;
        var credential = new StorageSharedKeyCredential("test", Convert.ToBase64String(new byte[32]));
        var container = new BlobContainerClient(containerUri, credential, options);
        var authorityContainer = new BlobContainerClient(containerUri, credential, options);
        const string initial = """{"format":1,"clusterId":"cluster","term":0,"revision":0,"applicationGeneration":0,"databaseNames":[]} """;
        var storage = new AzureLeaderStorage(authorityContainer.GetBlobClient("leader.json"), TimeSpan.FromSeconds(15), initial);
        await using var node = new TestNodeHost(endpoint, new AzureCanonicalTrlStorage(container, "main"));
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole().SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.UseKestrel().UseUrls(endpoint);
        builder.Services.AddSingleton<IReplicationNodeHost>(node);
        builder.Services.AddSingleton<IReplicationScheduler>(new TestScheduler());
        builder.Services.AddSingleton<IReplicationLeaseStorage>(storage);
        builder.Services.AddSingleton<ILeaderRecordStorage>(storage);
        builder.Services.AddBTDBReplication(new("cluster", endpoint, TimeSpan.FromMilliseconds(50),
            TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1), 1),
            0, TimeSpan.FromMilliseconds(100));
        await using var app = builder.Build();
        app.MapBTDBReplication();
        // Test-only controls exist only in this non-packable executable, bound to loopback.
        app.MapGet("/test/state", () => node.Status(app.Services.GetRequiredService<ReplicationNodeCoordinator>().Role));
        app.MapPost("/test/apply/{id:long}/{value:int}", async (long id, int value) =>
        {
            await node.ApplyAsync(checked((ulong)id), checked((byte)value));
            return Results.NoContent();
        });
        app.MapPost("/test/pause-publication", () => { node.PausePublication(); return Results.NoContent(); });
        await app.StartAsync();
        Console.WriteLine("READY " + endpoint);
        await app.WaitForShutdownAsync();
    }
}
