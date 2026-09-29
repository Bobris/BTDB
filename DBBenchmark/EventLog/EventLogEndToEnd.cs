using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage.Blobs;
using BTDB.Replication.Azure;
using BTDB.Replication.EventLog;
using BTDB.Replication.Http;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DBBenchmark.EventLog;

/// Publish-to-all-nodes latency of the event log (EventLogImplementationPlan.md, E0): N nodes in this process talk over
/// loopback HTTP and share one storage, publications arrive open-loop at a fixed rate round-robin over the nodes, and
/// every node follows the topic. A record's latency is its invocation-to-last-node-delivery time on one monotonic clock;
/// the invocation-to-receipt time is reported separately. Storage is in memory unless --account names an Azure account
/// (DefaultAzureCredential). One process on loopback cannot qualify multi-VM or cross-zone deployments.
/// Options: --nodes 3 --rate 500 --seconds 10 --size 1024 --account name --container eventlog-bench.
static class EventLogEndToEnd
{
    public static async Task RunAsync(string[] args)
    {
        int nodes = 3, rate = 500, seconds = 10, size = 1024;
        string? account = null;
        var containerName = "eventlog-bench";
        for (var i = 0; i + 1 < args.Length; i += 2)
        {
            var value = args[i + 1];
            switch (args[i])
            {
                case "--nodes": nodes = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "--rate": rate = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "--seconds": seconds = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "--size": size = Math.Max(8, int.Parse(value, CultureInfo.InvariantCulture)); break;
                case "--account": account = value; break;
                case "--container": containerName = value; break;
                default: throw new ArgumentException("Unknown option " + args[i]);
            }
        }
        IEventLogStorage storage;
        if (account != null)
        {
            var options = new BlobClientOptions();
            options.Retry.MaxRetries = 0;
            var container = new BlobServiceClient(new Uri($"https://{account}.blob.core.windows.net"),
                new DefaultAzureCredential(), options).GetBlobContainerClient(containerName);
            await container.CreateIfNotExistsAsync();
            storage = new AzureEventLogStorage(container, "bench-" + Guid.NewGuid().ToString("N"));
        }
        else storage = new InMemoryEventLogStorage();

        var apps = new List<WebApplication>();
        var logs = new List<IEventLog>();
        for (var n = 0; n < nodes; n++)
        {
            var endpoint = $"http://127.0.0.1:{FreePort()}";
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseKestrel().UseUrls(endpoint);
            builder.Services.AddSingleton(storage);
            builder.Services.AddBTDBEventLog(endpoint, "benchmark");
            var app = builder.Build();
            app.MapBTDBEventLog();
            await app.StartAsync();
            apps.Add(app);
            logs.Add(app.Services.GetRequiredService<IEventLog>());
        }
        const string topicName = "bench";
        await logs[0].GetTopic(topicName).PublishAsync(new byte[size]); // create the topic and pick its owner
        var total = rate * seconds;
        var start = new long[total];
        var acked = new long[total];
        var delivered = new long[total];
        var remaining = new int[total];
        Array.Fill(remaining, nodes);
        var all = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var outstanding = total;
        using var stop = new CancellationTokenSource();
        var first = (await logs[0].GetTopic(topicName).GetBoundsAsync()).Next;
        var readers = logs.Select(log => Task.Run(async () =>
        {
            try
            {
                await foreach (var record in log.GetTopic(topicName).ReadAsync(first, null, stop.Token))
                {
                    var k = BitConverter.ToInt32(record.Payload.Span);
                    if (Interlocked.Decrement(ref remaining[k]) != 0) continue;
                    delivered[k] = Stopwatch.GetTimestamp();
                    if (Interlocked.Decrement(ref outstanding) == 0) all.TrySetResult();
                }
            }
            catch (OperationCanceledException) { }
        })).ToArray();
        await Task.Delay(500); // let subscriptions reach the owner

        var errors = 0;
        var origin = Stopwatch.GetTimestamp();
        var publications = new Task[total];
        for (var k = 0; k < total; k++)
        {
            var due = origin + (long)(k * (double)Stopwatch.Frequency / rate);
            while (Stopwatch.GetTimestamp() < due)
                if (due - Stopwatch.GetTimestamp() > Stopwatch.Frequency / 500) await Task.Delay(1);
                else Thread.SpinWait(50);
            var payload = new byte[size];
            BitConverter.TryWriteBytes(payload, k);
            var index = k;
            // Open loop: the invocation time is the scheduled time, so queueing behind slow commits is included.
            start[k] = due;
            publications[k] = logs[k % nodes].GetTopic(topicName).PublishAsync(payload).AsTask().ContinueWith(t =>
            {
                if (t.IsCompletedSuccessfully) acked[index] = Stopwatch.GetTimestamp();
                else Interlocked.Increment(ref errors);
            }, TaskScheduler.Default);
        }
        await Task.WhenAll(publications);
        await all.Task.WaitAsync(TimeSpan.FromSeconds(60));
        stop.Cancel();
        await Task.WhenAll(readers);
        foreach (var app in apps) await app.DisposeAsync();

        double Ms(long ticks) => ticks * 1000.0 / Stopwatch.Frequency;
        var endToEnd = Enumerable.Range(0, total).Select(k => Ms(delivered[k] - start[k])).Order().ToArray();
        var receipts = Enumerable.Range(0, total).Where(k => acked[k] != 0).Select(k => Ms(acked[k] - start[k])).Order().ToArray();
        Console.WriteLine(JsonSerializer.Serialize(new
        {
            kind = "eventlog-e2e", nodes, rate, seconds, size, storage = account != null ? "azure" : "memory", errors,
            offeredSeconds = Ms(Stopwatch.GetTimestamp() - origin) / 1000,
            endToEnd = Summary(endToEnd), receipt = Summary(receipts)
        }));
    }

    static object Summary(double[] ordered)
    {
        double Q(double q) => ordered[Math.Min(ordered.Length - 1, (int)Math.Ceiling(q * ordered.Length) - 1)];
        return ordered.Length == 0 ? new { count = 0 } : new
        {
            count = ordered.Length, p50 = Q(.5), p95 = Q(.95), p99 = Q(.99), p999 = Q(.999), max = ordered[^1]
        };
    }

    static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
