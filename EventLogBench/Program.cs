using System.Security.Cryptography.X509Certificates;
using Azure.Identity;
using Azure.Storage.Blobs;
using BTDB.Replication.Azure;
using BTDB.Replication.EventLog;
using BTDB.Replication.Http;

namespace EventLogBench;

/// <summary>
/// EventLog benchmark for a multi-node deployment. "node" hosts one EventLog node (peer HTTPS on 8443) plus a plain HTTP
/// control API on 8080; "driver" runs a scenario matrix against the nodes and writes aggregated JSON. See README.md.
/// </summary>
public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "driver") return await Driver.RunAsync(args[1..]);
        await RunNodeAsync();
        return 0;
    }

    static string Env(string name, string? fallback = null) =>
        Environment.GetEnvironmentVariable(name) ?? fallback ??
        throw new InvalidOperationException($"Set the {name} environment variable.");

    static async Task RunNodeAsync()
    {
        // POD_NAME is "<statefulset>-<ordinal>"; peers reach each other through the headless service.
        var podName = Env("POD_NAME");
        var index = int.Parse(podName[(podName.LastIndexOf('-') + 1)..]);
        var endpoint = string.Format(Env("PEER_ENDPOINT_FORMAT"), index);
        var blobOptions = new BlobClientOptions();
        blobOptions.Retry.MaxRetries = 0; // the event log resolves ambiguous conditional writes itself
        // BLOB_CONNECTION_STRING is for a local Azurite smoke run; deployments use BLOB_ENDPOINT and workload identity.
        var service = Environment.GetEnvironmentVariable("BLOB_CONNECTION_STRING") is { } connection
            ? new BlobServiceClient(connection, blobOptions)
            : new BlobServiceClient(new Uri(Env("BLOB_ENDPOINT")), new DefaultAzureCredential(), blobOptions);
        var container = service.GetBlobContainerClient(Env("BLOB_CONTAINER", "eventlog-bench"));
        await container.CreateIfNotExistsAsync();
        var storage = new TimedStorage(new AzureEventLogStorage(container, Env("STORAGE_PREFIX")));
        var dataRoot = Env("DATA_ROOT", Path.GetTempPath());

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole().SetMinimumLevel(LogLevel.Warning);
        var peerPort = int.Parse(Env("PEER_PORT", "8443"));
        var controlPort = int.Parse(Env("CONTROL_PORT", "8080"));
        // Without TLS_CERT the peer port is plain HTTP, which the transport accepts only for loopback endpoints.
        var certificate = Environment.GetEnvironmentVariable("TLS_CERT") is { } cert
            ? X509Certificate2.CreateFromPemFile(cert, Env("TLS_KEY")) : null;
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.ListenAnyIP(peerPort, listen => { if (certificate != null) listen.UseHttps(certificate); });
            kestrel.ListenAnyIP(controlPort);
            kestrel.Limits.MinResponseDataRate = null; // live subscriptions are long, sparse responses
        });
        builder.Services.ConfigureHttpJsonOptions(json =>
            json.SerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter()));
        builder.Services.AddSingleton<IEventLogStorage>(storage);
        var options = new EventLogOptions();
        if (Environment.GetEnvironmentVariable("MAX_PUBLISH_ROUNDS") is { } rounds)
            options = options with { MaxPublishRoundsInFlight = int.Parse(rounds) };
        if (Environment.GetEnvironmentVariable("COMMIT_LINGER_US") is { Length: > 0 } linger)
            options = options with { CommitLinger = TimeSpan.FromMicroseconds(long.Parse(linger)) };
        if (Environment.GetEnvironmentVariable("ADAPTIVE_LINGER_MAX_US") is { Length: > 0 } adaptive)
            options = options with { AdaptiveCommitLingerMax = TimeSpan.FromMicroseconds(long.Parse(adaptive)) };
        builder.Services.AddBTDBEventLog(endpoint, Env("API_KEY"), options);
        var app = builder.Build();
        app.MapBTDBEventLog();
        var log = app.Services.GetRequiredService<EventLogService>();
        NodeScenario? current = null;
        var gate = new SemaphoreSlim(1);

        app.MapGet("/ctl/info", () => new NodeInfo(index, endpoint, Env("NODE_NAME", "?"), Environment.ProcessorCount));
        app.MapGet("/ctl/clock", () => Results.Text(BenchClock.NowUs.ToString()));
        app.MapPost("/ctl/prepare", async (ScenarioSpec spec, CancellationToken cancellation) =>
        {
            await gate.WaitAsync(cancellation);
            try
            {
                if (current != null) await current.DisposeAsync();
                current = new NodeScenario(spec, index, log, dataRoot);
                await current.PrepareAsync(cancellation);
                return Results.Ok(current.Status);
            }
            finally { gate.Release(); }
        });
        app.MapPost("/ctl/start", (StartRequest request) =>
        {
            storage.Reset();
            current!.Start(request);
            return Results.Ok();
        });
        app.MapGet("/ctl/status", () => current?.Status);
        app.MapGet("/ctl/storage", () => new { storage = storage.Snapshot(), breakdown = current?.Breakdown });
        app.MapGet("/ctl/report", () => Results.Bytes(current!.Report(), "application/octet-stream"));
        app.MapPost("/ctl/cleanup", async () =>
        {
            await gate.WaitAsync();
            try
            {
                if (current != null) await current.DisposeAsync();
                current = null;
                return Results.Ok();
            }
            finally { gate.Release(); }
        });
        await app.RunAsync();
    }
}
