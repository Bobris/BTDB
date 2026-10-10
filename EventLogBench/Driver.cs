using System.Globalization;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace EventLogBench;

/// <summary>
/// Runs scenarios against nodes reachable at --control "http://elb-{0}.elb:8080". Before each scenario it estimates
/// every node's clock offset from the minimum-RTT sample of repeated clock reads, so delivery times measured on one node
/// and publication times measured on another share one time base. The error bound is half the minimum RTT, reported
/// with the drift observed over the scenario.
/// Options: --nodes N --control format --seconds 20 --out results.json --suite full|quick --run id.
/// </summary>
public static class Driver
{
    static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }, NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals,
        WriteIndented = true
    };

    public static async Task<int> RunAsync(string[] args)
    {
        int nodes = 1, seconds = 20;
        string control = "http://elb-{0}.elb:8080", output = "results.json", suite = "full";
        var run = DateTime.UtcNow.ToString("MMddHHmmss", CultureInfo.InvariantCulture);
        string? only = null;
        for (var i = 0; i + 1 < args.Length; i += 2)
        {
            var value = args[i + 1];
            switch (args[i])
            {
                case "--nodes": nodes = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "--seconds": seconds = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "--control": control = value; break;
                case "--out": output = value; break;
                case "--suite": suite = value; break;
                case "--run": run = value; break;
                case "--only": only = value; break;
                default: throw new ArgumentException("Unknown option " + args[i]);
            }
        }
        using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(5) };
        var urls = Enumerable.Range(0, nodes).Select(i => string.Format(control, i)).ToArray();
        var infos = new List<NodeInfo>();
        foreach (var url in urls) infos.Add((await http.GetFromJsonAsync<NodeInfo>(url + "/ctl/info", Json))!);
        var results = new List<object>();
        foreach (var spec in Suite(suite, nodes, seconds, run))
        {
            if (only != null && !spec.Name.Contains(only, StringComparison.Ordinal)) continue;
            Console.Error.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] {spec.Name}");
            try
            {
                var result = await RunScenarioAsync(http, urls, spec, infos);
                results.Add(result);
                Console.Error.WriteLine("  " + JsonSerializer.Serialize(result.Headline, Json).ReplaceLineEndings(" "));
            }
            catch (Exception error)
            {
                Console.Error.WriteLine("  FAILED " + error);
                results.Add(new { spec, failed = error.ToString() });
                foreach (var url in urls)
                    try { await http.PostAsync(url + "/ctl/cleanup", null); } catch { }
            }
            await File.WriteAllTextAsync(output, JsonSerializer.Serialize(new { nodes, infos, results }, Json));
        }
        return 0;
    }

    static IEnumerable<ScenarioSpec> Suite(string suite, int nodes, int seconds, string run)
    {
        ScenarioSpec Spec(BenchMode mode, LoadKind load, double rate, int concurrency, int payload)
        {
            var name = (mode == BenchMode.EventLog ? "el" : "es") + "-n" + nodes + "-" +
                       load switch { LoadKind.Open => "rate" + rate, LoadKind.Closed => "c" + concurrency,
                           _ => "mix" + rate + "c" + concurrency } + "-b" + payload;
            return new(name, mode, load, nodes, rate, concurrency, seconds, payload, ("r" + run + "-" + name).ToLowerInvariant());
        }
        // "linger" keeps the loads below saturation, where a commit window can change how records share writes.
        var unsaturated = suite is "linger" or "adaptive";
        double[] rates = suite switch { "quick" => [1000], _ when unsaturated => [100, 1000, 5000], _ => [100, 1000, 5000, 10000] };
        int[] concurrencies = suite switch { "quick" => [64], _ when unsaturated => [1, 16, 128], _ => [1, 16, 128, 1024] };
        foreach (var rate in rates) yield return Spec(BenchMode.EventLog, LoadKind.Open, rate, 0, 1024);
        foreach (var concurrency in concurrencies) yield return Spec(BenchMode.EventLog, LoadKind.Closed, 0, concurrency, 1024);
        foreach (var rate in rates) yield return Spec(BenchMode.EventSourcing, LoadKind.Open, rate, 0, 256);
        foreach (var concurrency in concurrencies) yield return Spec(BenchMode.EventSourcing, LoadKind.Closed, 0, concurrency, 256);
        if (suite != "adaptive") yield break;
        // Independent traffic and publish-and-await loops at once: a commit window helps one and costs the other.
        yield return Spec(BenchMode.EventLog, LoadKind.Mixed, 1000, 4, 1024);
        yield return Spec(BenchMode.EventSourcing, LoadKind.Mixed, 1000, 4, 256);
    }

    sealed record ScenarioResult(object Headline, object Detail);

    static async Task<ScenarioResult> RunScenarioAsync(HttpClient http, string[] urls, ScenarioSpec spec, List<NodeInfo> infos)
    {
        // The first prepared node publishes first and so owns the topic; the others then resolve it.
        await PostAsync(http, urls[0] + "/ctl/prepare", spec);
        await Task.WhenAll(urls.Skip(1).Select(url => PostAsync(http, url + "/ctl/prepare", spec)));
        var before = await Task.WhenAll(urls.Select(url => ClockOffsetAsync(http, url)));
        var t0 = BenchClock.NowUs + 1_500_000;
        await Task.WhenAll(urls.Select((url, i) => PostAsync(http, url + "/ctl/start", new StartRequest(t0 + before[i].Offset))));
        await Task.Delay(TimeSpan.FromSeconds(spec.Seconds));
        NodeStatus[] statuses;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(90);
        while (true)
        {
            statuses = await Task.WhenAll(urls.Select(async url =>
                (await http.GetFromJsonAsync<NodeStatus>(url + "/ctl/status", Json))!));
            var published = statuses.Sum(s => s.Published);
            var done = statuses.All(s => s.LoadDone && s.Pending == 0) && statuses.All(s => s.Delivered >= published);
            if (done || DateTime.UtcNow > deadline || statuses.Any(s => s.Fault != null)) break;
            await Task.Delay(250);
        }
        var after = await Task.WhenAll(urls.Select(url => ClockOffsetAsync(http, url)));
        var reports = await Task.WhenAll(urls.Select(url => http.GetByteArrayAsync(url + "/ctl/report")));
        var storage = await Task.WhenAll(urls.Select(async url => (await http.GetFromJsonAsync<JsonElement>(url + "/ctl/storage", Json))));
        await Task.WhenAll(urls.Select(url => http.PostAsync(url + "/ctl/cleanup", null)));
        var data = reports.Select(Parse).OrderBy(r => r.Index).ToArray();
        var clock = new
        {
            minRttUs = before.Select(c => c.Rtt).ToArray(),
            driftUs = before.Zip(after, (b, a) => a.Offset - b.Offset).ToArray()
        };
        return Aggregate(spec, data, statuses, clock, infos, storage);
    }

    static async Task PostAsync<T>(HttpClient http, string url, T body)
    {
        using var response = await http.PostAsJsonAsync(url, body, Json);
        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException($"{url}: {(int)response.StatusCode} {await response.Content.ReadAsStringAsync()}");
    }

    static async Task<(long Offset, long Rtt)> ClockOffsetAsync(HttpClient http, string url)
    {
        var best = (Offset: 0L, Rtt: long.MaxValue);
        for (var i = 0; i < 40; i++)
        {
            var sent = BenchClock.NowUs;
            var remote = long.Parse(await http.GetStringAsync(url + "/ctl/clock"), CultureInfo.InvariantCulture);
            var received = BenchClock.NowUs;
            if (received - sent < best.Rtt) best = (remote - (sent + received) / 2, received - sent);
        }
        return best;
    }

    sealed record NodeData(int Index, float[] Scheduled, float[] Receipt, float[][] Delivered);

    static NodeData Parse(byte[] bytes)
    {
        using var reader = new BinaryReader(new MemoryStream(bytes));
        var index = reader.ReadInt32();
        var scheduled = SeqTimes.Read(reader);
        var receipt = SeqTimes.Read(reader);
        var delivered = new float[reader.ReadInt32()][];
        for (var i = 0; i < delivered.Length; i++) delivered[i] = SeqTimes.Read(reader);
        return new(index, scheduled, receipt, delivered);
    }

    static float At(float[] values, int index) => index < values.Length ? values[index] : float.NaN;

    static ScenarioResult Aggregate(ScenarioSpec spec, NodeData[] data, NodeStatus[] statuses, object clock,
        List<NodeInfo> infos, JsonElement[] storage)
    {
        var owner = Array.FindIndex(statuses, s => s.IsOwner);
        // Latency covers publications scheduled after the first second (connection and JIT warmup).
        const float warmupMs = 1000;
        var windowStart = warmupMs;
        var windowEnd = spec.Seconds * 1000f - 1000;
        var receipt = new List<double>();
        var receiptOwner = new List<double>();
        var receiptOther = new List<double>();
        var receiptOpen = new List<double>();
        var receiptClosed = new List<double>();
        var self = new List<double>();
        var all = new List<double>();
        var perNode = data.Select(_ => new List<double>()).ToArray();
        var perNodeDelivered = new long[data.Length];
        long published = 0, missing = 0, inWindow = 0;
        var timeline = new SortedDictionary<int, (int Count, List<double> Latency)>();
        foreach (var publisher in data)
        {
            for (var seq = 0; seq < publisher.Scheduled.Length; seq++)
            {
                var scheduled = publisher.Scheduled[seq];
                if (float.IsNaN(scheduled)) continue;
                var receivedAt = At(publisher.Receipt, seq);
                if (float.IsNaN(receivedAt)) continue;
                published++;
                if (receivedAt >= windowStart && receivedAt < windowEnd) inWindow++;
                var second = (int)Math.Floor(receivedAt / 1000);
                if (!timeline.TryGetValue(second, out var slot)) timeline[second] = slot = (0, []);
                timeline[second] = (slot.Count + 1, slot.Latency);
                slot.Latency.Add(receivedAt - scheduled);
                if (scheduled < warmupMs) continue;
                receipt.Add(receivedAt - scheduled);
                (publisher.Index == owner ? receiptOwner : receiptOther).Add(receivedAt - scheduled);
                if (spec.Load == LoadKind.Mixed) (seq % 2 == 0 ? receiptOpen : receiptClosed).Add(receivedAt - scheduled);
                var last = double.MinValue;
                var complete = true;
                foreach (var consumer in data)
                {
                    var deliveredAt = At(consumer.Delivered[publisher.Index], seq);
                    if (float.IsNaN(deliveredAt)) { complete = false; missing++; continue; }
                    perNode[consumer.Index].Add(deliveredAt - scheduled);
                    if (consumer.Index == publisher.Index) self.Add(deliveredAt - scheduled);
                    last = Math.Max(last, deliveredAt);
                }
                if (complete) all.Add(last - scheduled);
            }
        }
        foreach (var list in new[] { receipt, receiptOwner, receiptOther, receiptOpen, receiptClosed, self, all }.Concat(perNode))
            list.Sort();
        foreach (var slot in timeline.Values) slot.Latency.Sort();
        foreach (var consumer in data)
            perNodeDelivered[consumer.Index] = consumer.Delivered.Sum(d => (long)d.Count(v => v >= windowStart && v < windowEnd));
        var windowSeconds = (windowEnd - windowStart) / 1000.0;
        var throughput = inWindow / windowSeconds;
        var headline = new
        {
            spec.Name, owner, published, throughput = Math.Round(throughput),
            errors = statuses.Sum(s => s.Errors), missing,
            receiptP50 = Q(receipt, .5), receiptP99 = Q(receipt, .99),
            allP50 = Q(all, .5), allP99 = Q(all, .99),
            openP50 = Q(receiptOpen, .5), closedP50 = Q(receiptClosed, .5),
            linger = owner >= 0 ? statuses[owner].Linger : null, ownerStorage = owner >= 0 ? storage[owner] : default, fault = statuses.Select(s => s.Fault).FirstOrDefault(f => f != null)
        };
        var detail = new
        {
            spec, owner, nodes = infos.Take(spec.Nodes), published, errors = statuses.Sum(s => s.Errors), missing,
            throughput, deliveredThroughput = perNodeDelivered.Select(d => d / windowSeconds).ToArray(),
            receipt = Summary(receipt), receiptOwner = Summary(receiptOwner), receiptOther = Summary(receiptOther),
            receiptOpen = Summary(receiptOpen), receiptClosed = Summary(receiptClosed),
            linger = owner >= 0 ? statuses[owner].Linger : null,
            selfDelivery = Summary(self), allDelivery = Summary(all), perNodeDelivery = perNode.Select(Summary).ToArray(),
            timeline = timeline.Select(t => new
            {
                second = t.Key, commits = t.Value.Count, p50 = Q(t.Value.Latency, .5), p99 = Q(t.Value.Latency, .99)
            }).ToArray(),
            storage, clock, faults = statuses.Select(s => s.Fault).ToArray()
        };
        return new(headline, detail);
    }

    static readonly double[] CdfPoints =
        [.01, .05, .1, .2, .3, .4, .5, .6, .7, .8, .85, .9, .925, .95, .97, .98, .99, .995, .999, .9995, .9999];

    static double? Q(List<double> values, double q)
    {
        if (values.Count == 0) return null;
        return Math.Round(values[Math.Clamp((int)Math.Ceiling(q * values.Count) - 1, 0, values.Count - 1)], 3);
    }

    static object Summary(List<double> values)
    {
        return new
        {
            count = values.Count, mean = values.Count == 0 ? (double?)null : Math.Round(values.Average(), 3),
            p50 = Q(values, .5), p90 = Q(values, .9), p99 = Q(values, .99), p999 = Q(values, .999),
            max = values.Count == 0 ? (double?)null : Math.Round(values[^1], 3),
            cdf = values.Count == 0 ? [] : CdfPoints.Select(q => new[] { q, Q(values, q) ?? 0 }).ToArray()
        };
    }
}
