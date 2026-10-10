using System.Runtime.CompilerServices;
using BTDB.Replication.EventLog;

namespace EventLogBench;

/// <summary>Splits event-sourcing latency into EventLog receipt, arrival of the record at this node's reader, start of
/// its consumption and batch visibility, all on this node's clock.</summary>
public sealed class TopicProbe(IEventTopic inner) : IEventTopic
{
    readonly SeqTimes _arrival = new();
    readonly SeqTimes _consumed = new();
    readonly SeqTimes _visible = new();
    readonly List<double> _receipts = [];
    readonly long _originUs = BenchClock.NowUs;

    public string Name => inner.Name;

    float Now => (float)((BenchClock.NowUs - _originUs) / 1000.0);

    public async ValueTask<ulong> PublishAsync(ReadOnlyMemory<byte> record, CancellationToken cancellation = default)
    {
        var start = BenchClock.NowUs;
        var offset = await inner.PublishAsync(record, cancellation);
        var ms = (BenchClock.NowUs - start) / 1000.0;
        lock (_receipts) _receipts.Add(ms);
        return offset;
    }

    public ValueTask<EventLogBounds> GetBoundsAsync(CancellationToken cancellation = default) =>
        inner.GetBoundsAsync(cancellation);

    public async IAsyncEnumerable<EventLogRecord> ReadAsync(ulong start, ulong? end = null,
        [EnumeratorCancellation] CancellationToken cancellation = default)
    {
        await foreach (var record in inner.ReadAsync(start, end, cancellation))
        {
            _arrival.Set((int)record.Offset, Now);
            yield return record;
        }
    }

    public void Consumed(ulong eventId) => _consumed.Set((int)(eventId - 1), Now);

    public void Visible(ulong eventId) => _visible.Set((int)(eventId - 1), Now);

    public object Summary()
    {
        var arrival = Read(_arrival);
        var consumed = Read(_consumed);
        var visible = Read(_visible);
        var queue = new List<double>();
        var batch = new List<double>();
        for (var i = 0; i < Math.Min(arrival.Length, Math.Min(consumed.Length, visible.Length)); i++)
        {
            if (float.IsNaN(arrival[i]) || float.IsNaN(consumed[i]) || float.IsNaN(visible[i])) continue;
            queue.Add(consumed[i] - arrival[i]);
            batch.Add(visible[i] - consumed[i]);
        }
        List<double> receipts;
        lock (_receipts) receipts = [.. _receipts];
        return new { eventLogReceipt = Stats(receipts), arrivalToConsume = Stats(queue), consumeToVisible = Stats(batch) };
    }

    static float[] Read(SeqTimes times)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, true)) times.WriteTo(writer);
        stream.Position = 0;
        return SeqTimes.Read(new BinaryReader(stream));
    }

    static object Stats(List<double> values)
    {
        values.Sort();
        double? Q(double q) => values.Count == 0 ? null
            : Math.Round(values[Math.Clamp((int)Math.Ceiling(q * values.Count) - 1, 0, values.Count - 1)], 3);
        return new { count = values.Count, p50 = Q(.5), p90 = Q(.9), p99 = Q(.99) };
    }
}
