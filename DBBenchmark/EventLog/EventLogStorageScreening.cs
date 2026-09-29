using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace DBBenchmark.EventLog;

/// Closed-loop Azure Blob storage primitives behind the event log storage decision (EventLogImplementationPlan.md,
/// section 16): bounded Put Blob splits, stage+commit variants, immutable blobs, lease handoffs and ETag takeover.
/// Run it on same-region Azure compute with a managed identity against a dedicated Standard_GZRS account:
/// BENCH_ACCOUNT (required), BENCH_CONTAINER (default eventlog-latency), BENCH_SAMPLES (default 2000),
/// BENCH_FILTER (case-name substring), BENCH_TOKEN (optional bearer token instead of the IMDS managed identity).
/// Every sample waits for the storage response; results exclude failure detection, routing and application work.
static class EventLogStorageScreening
{
    public static async Task RunAsync()
    {

        // Storage primitive benchmark, not a production event-log implementation.
        // No automatic application retries; errors terminate the current case and remain visible.
        var account = Environment.GetEnvironmentVariable("BENCH_ACCOUNT") ?? throw new InvalidOperationException("BENCH_ACCOUNT required");
        var container = Environment.GetEnvironmentVariable("BENCH_CONTAINER") ?? "eventlog-latency";
        var samples = int.Parse(Environment.GetEnvironmentVariable("BENCH_SAMPLES") ?? "2000");
        var prefix = DateTime.UtcNow.ToString("yyyyMMddTHHmmss") + "-" + Guid.NewGuid().ToString("N");
        using var handler = new SocketsHttpHandler { MaxConnectionsPerServer = 32, PooledConnectionLifetime = TimeSpan.FromMinutes(10) };
        using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(30) };
        var token = Environment.GetEnvironmentVariable("BENCH_TOKEN");
        if (token == null)
        {
            using var auth = new HttpRequestMessage(HttpMethod.Get,
                "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F");
            auth.Headers.Add("Metadata", "true");
            using var response = await http.SendAsync(auth);
            response.EnsureSuccessStatusCode();
            token = JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.GetProperty("access_token").GetString();
        }
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        http.DefaultRequestHeaders.Add("x-ms-version", "2023-11-03");
        var root = $"https://{account}.blob.core.windows.net/{container}";
        var results = new List<object>();
        Console.WriteLine(JsonSerializer.Serialize(new { kind = "environment", account, container, prefix, samples,
            machine = Environment.MachineName, cpuCount = Environment.ProcessorCount,
            runtime = Environment.Version.ToString(), utc = DateTime.UtcNow,
            qualification = "closed-loop storage primitives; not application end-to-end SLO" }));
        await Request("PUT", "?restype=container", ReadOnlyMemory<byte>.Empty, allowConflict: true);

        foreach (var size in new[] { 1024, 16 * 1024, 64 * 1024 })
        {
            foreach (var cap in new[] { 64 * 1024, 256 * 1024, 1024 * 1024 })
                await Run($"putblob-grow-{size}-cap-{cap}", async record =>
                {
                    var body = RandomNumberGenerator.GetBytes(cap);
                    string? etag = null;
                    var length = 0;
                    var split = 0;
                    for (var i = -100; i < samples; i++)
                    {
                        if (length + size > cap) { length = 0; etag = null; split++; }
                        length += size;
                        var start = Stopwatch.GetTimestamp();
                        etag = await Request("PUT", $"/{prefix}/grow-{size}-{cap}-{split}", body.AsMemory(0, length),
                            new() { ["x-ms-blob-type"] = "BlockBlob", [etag == null ? "If-None-Match" : "If-Match"] = etag ?? "*" });
                        if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, 0, 0, length, length == size));
                    }
                    await Verify($"/{prefix}/grow-{size}-{cap}-{split}", body.AsMemory(0, length));
                });
            foreach (var initialBlocks in new[] { 0, 1000, 4000 })
                await Run($"stage-commit-{size}-initial-{initialBlocks}", async record =>
                {
                    var path = $"/{prefix}/blocks-{size}-{initialBlocks}";
                    var body = RandomNumberGenerator.GetBytes(size);
                    var blocks = new List<string>();
                    // Preseed a real block list; every block ID is unique and stable for this case.
                    var ids = Enumerable.Range(0, initialBlocks).Select(BlockId).ToArray();
                    await Parallel.ForEachAsync(ids, new ParallelOptions { MaxDegreeOfParallelism = 16 }, async (id, _) =>
                        await Request("PUT", path + "?comp=block&blockid=" + Uri.EscapeDataString(id), body));
                    blocks.AddRange(ids);
                    var etag = await Commit(path, blocks, null);
                    for (var i = -100; i < samples; i++)
                    {
                        var id = BlockId(blocks.Count);
                        var start = Stopwatch.GetTimestamp();
                        await Request("PUT", path + "?comp=block&blockid=" + Uri.EscapeDataString(id), body);
                        var stage = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
                        blocks.Add(id);
                        var commitStart = Stopwatch.GetTimestamp();
                        etag = await Commit(path, blocks, etag);
                        var commit = Stopwatch.GetElapsedTime(commitStart).TotalMilliseconds;
                        if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, stage, commit, size, false));
                    }
                    await VerifyBlockEnds(path, body, blocks.Count);
                });
        }

        // Same small-split byte caps as the Put Blob variants; avoids growing XML lists.
        foreach (var size in new[] { 1024, 16 * 1024 })
            foreach (var cap in new[] { 64 * 1024, 256 * 1024 })
                await Run($"stage-rotate-{size}-cap-{cap}", async record =>
                {
                    var body = RandomNumberGenerator.GetBytes(size);
                    var blocks = new List<string>();
                    string? etag = null;
                    var split = 0;
                    var path = "";
                    for (var i = -100; i < samples; i++)
                    {
                        if ((blocks.Count + 1) * size > cap) { blocks.Clear(); etag = null; split++; }
                        path = $"/{prefix}/rotate-{size}-{cap}-{split}";
                        var id = BlockId(blocks.Count);
                        var start = Stopwatch.GetTimestamp();
                        await Request("PUT", path + "?comp=block&blockid=" + Uri.EscapeDataString(id), body);
                        var stage = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
                        blocks.Add(id);
                        var commitStart = Stopwatch.GetTimestamp();
                        etag = await Commit(path, blocks, etag);
                        var commit = Stopwatch.GetElapsedTime(commitStart).TotalMilliseconds;
                        if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, stage, commit, size, blocks.Count == 1));
                    }
                    await VerifyBlockEnds(path, body, blocks.Count);
                });

        // Bounded tail-block rewrite: small block list without creating a blob per small split.
        foreach (var size in new[] { 1024, 16 * 1024 })
            foreach (var cap in new[] { 64 * 1024, 256 * 1024 })
                await Run($"stage-tail-{size}-block-{cap}", async record =>
                {
                    var unit = RandomNumberGenerator.GetBytes(size);
                    var tail = new byte[cap];
                    var tailLength = 0;
                    var committed = new List<string>();
                    string? etag = null;
                    var path = $"/{prefix}/tail-block-{size}-{cap}";
                    for (var i = -100; i < samples; i++)
                    {
                        unit.CopyTo(tail, tailLength);
                        tailLength += size;
                        var id = BlockId(i + 100);
                        var start = Stopwatch.GetTimestamp();
                        await Request("PUT", path + "?comp=block&blockid=" + Uri.EscapeDataString(id), tail.AsMemory(0, tailLength));
                        var stage = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
                        var candidate = new List<string>(committed) { id };
                        var commitStart = Stopwatch.GetTimestamp();
                        etag = await Commit(path, candidate, etag);
                        var commit = Stopwatch.GetElapsedTime(commitStart).TotalMilliseconds;
                        if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, stage, commit, tailLength, tailLength == size));
                        if (tailLength == cap) { committed.Add(id); tailLength = 0; }
                    }
                    await VerifyBlockEnds(path, unit, samples + 100);
                });

        // Lower bound: each commit is a fresh immutable blob. No fencing or catalog cost included.
        foreach (var size in new[] { 256, 1024, 4096 })
            await Run($"putblob-create-{size}", async record =>
            {
                var body = RandomNumberGenerator.GetBytes(size);
                for (var i = -100; i < samples; i++)
                {
                    var start = Stopwatch.GetTimestamp();
                    await Request("PUT", $"/{prefix}/create-{size}-{i + 100}", body,
                        new() { ["x-ms-blob-type"] = "BlockBlob", ["If-None-Match"] = "*" });
                    if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, 0, 0, size, true));
                }
                await Verify($"/{prefix}/create-{size}-{samples + 99}", body);
            });

        // Measure a best-case planned ownership transfer with fresh client lease IDs.
        foreach (var strategy in new[] { "change-renew", "release-acquire", "change-renew-metadata" })
            await Run("handoff-" + strategy, async record =>
            {
                var path = $"/{prefix}/leader-{strategy}";
                var leaderEtag = await Request("PUT", path, new byte[16], new() { ["x-ms-blob-type"] = "BlockBlob", ["If-None-Match"] = "*" });
                var lease = Guid.NewGuid().ToString();
                await Lease(path, "acquire", lease);
                var tail = $"/{prefix}/tail-{strategy}";
                var body = new byte[1024];
                var etag = await Request("PUT", tail, body, new() { ["x-ms-blob-type"] = "BlockBlob", ["If-None-Match"] = "*" });
                for (var i = -20; i < 200; i++)
                {
                    var next = Guid.NewGuid().ToString();
                    var start = Stopwatch.GetTimestamp();
                    if (strategy.StartsWith("change-renew", StringComparison.Ordinal))
                    {
                        await Lease(path, "change", lease, next);
                        await Lease(path, "renew", next);
                    }
                    else
                    {
                        await Lease(path, "release", lease);
                        await Lease(path, "acquire", next);
                    }
                    lease = next;
                    leaderEtag = await Request("PUT", path, Encoding.UTF8.GetBytes(next), new() { ["x-ms-blob-type"] = "BlockBlob", ["If-Match"] = leaderEtag, ["x-ms-lease-id"] = lease });
                    var acquired = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
                    // Tail adoption changes ETag before the new leader's first durable mutation.
                    var beforeAdopt = etag;
                    etag = strategy.EndsWith("-metadata", StringComparison.Ordinal)
                        ? await Request("PUT", tail + "?comp=metadata", ReadOnlyMemory<byte>.Empty, new() { ["If-Match"] = etag })
                        : await Request("PUT", tail, body, new() { ["x-ms-blob-type"] = "BlockBlob", ["If-Match"] = etag });
                    if (etag == beforeAdopt) throw new Exception("Adoption did not change ETag");
                    var adopted = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
                    var previous = body;
                    body = new byte[previous.Length + 1024];
                    previous.CopyTo(body, 0);
                    body[^1] = (byte)i;
                    etag = await Request("PUT", tail, body, new() { ["x-ms-blob-type"] = "BlockBlob", ["If-Match"] = etag });
                    if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, acquired, adopted - acquired, body.Length, false));
                }
                await Verify(tail, body);
                await Lease(path, "release", lease);
            });

        // Per-topic ETag takeover: no global leader blob and no finite lease.
        // Measures the storage transition only; failure detection and peer redirection are outside this case.
        foreach (var tailCap in new[] { 64 * 1024, 256 * 1024 })
            await Run($"handoff-etag-only-cap-{tailCap}", async record =>
            {
                var path = $"/{prefix}/etag-only-{tailCap}";
                var body = new byte[tailCap];
                var length = 1024;
                var split = 0;
                var currentPath = path + "-0";
                var etag = await Request("PUT", currentPath, body.AsMemory(0, length),
                    new() { ["x-ms-blob-type"] = "BlockBlob", ["If-None-Match"] = "*" });
                for (var i = -100; i < samples; i++)
                {
                    if (length + 1024 > tailCap)
                    {
                        currentPath = path + "-" + ++split;
                        length = 1024;
                        etag = await Request("PUT", currentPath, body.AsMemory(0, length),
                            new() { ["x-ms-blob-type"] = "BlockBlob", ["If-None-Match"] = "*" });
                    }
                    var oldEtag = etag;
                    var start = Stopwatch.GetTimestamp();
                    etag = await Request("PUT", currentPath + "?comp=metadata", ReadOnlyMemory<byte>.Empty,
                        new() { ["If-Match"] = oldEtag, ["x-ms-meta-owner"] = "owner" + (i + 100) });
                    if (etag == oldEtag) throw new Exception("Takeover did not change ETag");
                    var takeover = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
                    length += 1024;
                    etag = await Request("PUT", currentPath, body.AsMemory(0, length),
                        new() { ["x-ms-blob-type"] = "BlockBlob", ["If-Match"] = etag,
                            ["x-ms-meta-owner"] = "owner" + (i + 100) });
                    var elapsed = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
                    if (i >= 0) record(new(i, elapsed, takeover, elapsed - takeover, length, false));
                    // Verify service-side fencing outside the timed interval once per case.
                    if (i == 0)
                    {
                        try
                        {
                            await Request("PUT", currentPath, body.AsMemory(0, length),
                                new() { ["x-ms-blob-type"] = "BlockBlob", ["If-Match"] = oldEtag });
                            throw new Exception("Stale writer was accepted");
                        }
                        catch (StorageError error) when (error.Status == 412) { }
                    }
                }
                await Verify(currentPath, body.AsMemory(0, length));
            });

        // Fold takeover and the first durable append into one conditional write.
        foreach (var tailCap in new[] { 64 * 1024, 256 * 1024 })
            await Run($"handoff-etag-combined-cap-{tailCap}", async record =>
            {
                var path = $"/{prefix}/etag-combined-{tailCap}";
                var body = RandomNumberGenerator.GetBytes(tailCap);
                var length = 1024;
                var split = 0;
                var currentPath = path + "-0";
                var etag = await Request("PUT", currentPath, body.AsMemory(0, length),
                    new() { ["x-ms-blob-type"] = "BlockBlob", ["If-None-Match"] = "*" });
                for (var i = -100; i < samples; i++)
                {
                    if (length + 1024 > tailCap)
                    {
                        currentPath = path + "-" + ++split;
                        length = 1024;
                        etag = await Request("PUT", currentPath, body.AsMemory(0, length),
                            new() { ["x-ms-blob-type"] = "BlockBlob", ["If-None-Match"] = "*" });
                    }
                    var oldEtag = etag;
                    length += 1024;
                    var start = Stopwatch.GetTimestamp();
                    etag = await Request("PUT", currentPath, body.AsMemory(0, length),
                        new() { ["x-ms-blob-type"] = "BlockBlob", ["If-Match"] = oldEtag,
                            ["x-ms-meta-owner"] = "owner" + (i + 100) });
                    if (etag == oldEtag) throw new Exception("Combined takeover did not change ETag");
                    if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, 0, 0, length, false));
                    if (i == 0)
                    {
                        try
                        {
                            await Request("PUT", currentPath, body.AsMemory(0, length),
                                new() { ["x-ms-blob-type"] = "BlockBlob", ["If-Match"] = oldEtag });
                            throw new Exception("Stale writer was accepted after combined takeover");
                        }
                        catch (StorageError error) when (error.Status == 412) { }
                    }
                }
                await Verify(currentPath, body.AsMemory(0, length));
            });

        // Hard loss: no lease release; retain the 15-second service-enforced safety boundary.
        await Run("crash-expire-15s", async record =>
        {
            for (var i = 0; i < 5; i++)
            {
                var path = $"/{prefix}/expiry-{i}";
                await Request("PUT", path, new byte[16], new() { ["x-ms-blob-type"] = "BlockBlob", ["If-None-Match"] = "*" });
                await Lease(path, "acquire", Guid.NewGuid().ToString());
                var start = Stopwatch.GetTimestamp();
                var acquired = false;
                while (!acquired)
                {
                    try { await Lease(path, "acquire", Guid.NewGuid().ToString()); acquired = true; }
                    catch (StorageError ex) when (ex.Status == 409) { await Task.Delay(50); }
                }
                record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, 0, 0, 0, false));
            }
        });
        await File.WriteAllTextAsync("summary.json", JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true }));

        async Task Run(string name, Func<Action<Sample>, Task> action)
        {
            var filter = Environment.GetEnvironmentVariable("BENCH_FILTER");
            if (filter != null && !name.Contains(filter, StringComparison.OrdinalIgnoreCase)) return;
            var values = new List<Sample>();
            var start = Stopwatch.GetTimestamp();
            string? error = null;
            try { await action(values.Add); }
            catch (Exception ex) { error = ex.Message; }
            var ordered = values.Select(x => x.TotalMs).Order().ToArray();
            double Q(double q) => ordered.Length == 0 ? double.NaN : ordered[Math.Min(ordered.Length - 1, (int)Math.Ceiling(q * ordered.Length) - 1)];
            var result = new { kind = "case", name, count = values.Count, seconds = Stopwatch.GetElapsedTime(start).TotalSeconds,
                error, p50 = ordered.Length == 0 ? (double?)null : Q(.5), p95 = ordered.Length == 0 ? (double?)null : Q(.95),
                p99 = ordered.Length == 0 ? (double?)null : Q(.99), p999 = ordered.Length == 0 ? (double?)null : Q(.999),
                max = ordered.Length == 0 ? (double?)null : ordered[^1], utc = DateTime.UtcNow };
            results.Add(result);
            Console.WriteLine(JsonSerializer.Serialize(result));
            await File.WriteAllTextAsync(name + ".json", JsonSerializer.Serialize(values));
        }

        async Task<string> Commit(string path, List<string> blocks, string? etag)
        {
            var xml = Encoding.UTF8.GetBytes("<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>" +
                string.Concat(blocks.Select(id => "<Latest>" + id + "</Latest>")) + "</BlockList>");
            return await Request("PUT", path + "?comp=blocklist", xml,
                new() { [etag == null ? "If-None-Match" : "If-Match"] = etag ?? "*" });
        }

        async Task Lease(string path, string action, string id, string? proposed = null)
        {
            var headers = new Dictionary<string, string> { ["x-ms-lease-action"] = action };
            if (action == "acquire") { headers["x-ms-lease-duration"] = "15"; headers["x-ms-proposed-lease-id"] = id; }
            else headers["x-ms-lease-id"] = id;
            if (proposed != null) headers["x-ms-proposed-lease-id"] = proposed;
            await Request("PUT", path + "?comp=lease", ReadOnlyMemory<byte>.Empty, headers);
        }

        async Task<string> Request(string method, string path, ReadOnlyMemory<byte> bytes,
            Dictionary<string, string>? headers = null, bool allowConflict = false)
        {
            using var request = new HttpRequestMessage(new HttpMethod(method), root + path);
            request.Headers.TryAddWithoutValidation("x-ms-date", DateTime.UtcNow.ToString("R"));
            request.Content = new ReadOnlyMemoryContent(bytes);
            if (headers != null) foreach (var h in headers) request.Headers.TryAddWithoutValidation(h.Key, h.Value);
            using var response = await http.SendAsync(request);
            if (!response.IsSuccessStatusCode && !(allowConflict && (int)response.StatusCode == 409))
                throw new StorageError((int)response.StatusCode, await response.Content.ReadAsStringAsync());
            return response.Headers.ETag?.ToString() ?? "";
        }

        async Task Verify(string path, ReadOnlyMemory<byte> expected)
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, root + path);
            request.Headers.TryAddWithoutValidation("x-ms-date", DateTime.UtcNow.ToString("R"));
            using var response = await http.SendAsync(request);
            response.EnsureSuccessStatusCode();
            var actual = await response.Content.ReadAsByteArrayAsync();
            if (!actual.AsSpan().SequenceEqual(expected.Span)) throw new Exception("Read-back mismatch: " + path);
        }

        async Task VerifyBlockEnds(string path, byte[] block, int count)
        {
            foreach (var offset in new long[] { 0, (long)(count - 1) * block.Length })
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, root + path);
                request.Headers.TryAddWithoutValidation("x-ms-date", DateTime.UtcNow.ToString("R"));
                request.Headers.Range = new RangeHeaderValue(offset, offset + block.Length - 1);
                using var response = await http.SendAsync(request);
                response.EnsureSuccessStatusCode();
                if (response.Content.Headers.ContentRange?.Length != (long)block.Length * count)
                    throw new Exception("Committed length mismatch");
                var actual = await response.Content.ReadAsByteArrayAsync();
                if (!actual.AsSpan().SequenceEqual(block)) throw new Exception("Block read-back mismatch");
            }
        }

        static string BlockId(int index) => Convert.ToBase64String(Encoding.ASCII.GetBytes(index.ToString("D12")));
    }

    record Sample(int Index, double TotalMs, double StageMs, double CommitMs, int Bytes, bool Rotation);
    sealed class StorageError(int status, string message) : Exception($"HTTP {status}: {message}")
    {
        public int Status { get; } = status;
    }
}
