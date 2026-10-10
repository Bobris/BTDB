using System.Diagnostics;
using BTDB.ODBLayer;
using BTDB.Replication.EventSourcing;

namespace EventLogBench;

public enum BenchMode { EventLog, EventSourcing }

public enum LoadKind { Open, Closed, Mixed }

/// <summary>One measured run. Open load spreads <see cref="TotalRate"/> evenly over the nodes; closed load runs
/// <see cref="Concurrency"/> publish-await loops on every node; mixed load runs both at once, open publications with
/// even and closed ones with odd sequence numbers.</summary>
public sealed record ScenarioSpec(string Name, BenchMode Mode, LoadKind Load, int Nodes, double TotalRate,
    int Concurrency, int Seconds, int PayloadBytes, string Topic);

public sealed record StartRequest(long T0LocalUs);

/// <param name="Linger">The owner's commit window sampled every 50 ms during the load: p10, p50 and p90 in ms.</param>
public sealed record NodeStatus(int Index, bool LoadDone, long Published, long Errors, long Pending, long Delivered,
    bool IsOwner, string? Fault, double[]? Linger = null);

public sealed record NodeInfo(int Index, string Endpoint, string NodeName, int ProcessorCount);

/// <summary>Monotonic microsecond clock anchored to the wall clock once at startup. Cross-node comparisons use the
/// offsets the driver estimates against its own instance of this clock.</summary>
public static class BenchClock
{
    static readonly long AnchorUs = DateTime.UtcNow.Ticks / 10;
    static readonly long AnchorTimestamp = Stopwatch.GetTimestamp();
    static readonly double UsPerTick = 1_000_000.0 / Stopwatch.Frequency;

    public static long NowUs => AnchorUs + (long)((Stopwatch.GetTimestamp() - AnchorTimestamp) * UsPerTick);
}

/// <summary>Float milliseconds indexed by a publisher sequence number; grows in chunks, safe for concurrent writers
/// of distinct indexes. Missing values are NaN.</summary>
public sealed class SeqTimes
{
    const int ChunkBits = 16;
    const int ChunkSize = 1 << ChunkBits;
    readonly float[]?[] _chunks = new float[]?[4096];
    int _length;

    public int Length => Volatile.Read(ref _length);

    public void Set(int index, float value)
    {
        var chunkIndex = index >> ChunkBits;
        var chunk = Volatile.Read(ref _chunks[chunkIndex]);
        if (chunk == null)
        {
            var fresh = new float[ChunkSize];
            Array.Fill(fresh, float.NaN);
            chunk = Interlocked.CompareExchange(ref _chunks[chunkIndex], fresh, null) ?? fresh;
        }
        chunk[index & (ChunkSize - 1)] = value;
        var length = Volatile.Read(ref _length);
        while (index + 1 > length)
        {
            var seen = Interlocked.CompareExchange(ref _length, index + 1, length);
            if (seen == length) break;
            length = seen;
        }
    }

    public void WriteTo(BinaryWriter writer)
    {
        var length = Length;
        writer.Write(length);
        for (var i = 0; i < length; i++)
        {
            var chunk = _chunks[i >> ChunkBits];
            writer.Write(chunk == null ? float.NaN : chunk[i & (ChunkSize - 1)]);
        }
    }

    public static float[] Read(BinaryReader reader)
    {
        var result = new float[reader.ReadInt32()];
        for (var i = 0; i < result.Length; i++) result[i] = reader.ReadSingle();
        return result;
    }
}

public class BenchEvent : EventWithResult<BenchResult>
{
    public int Publisher { get; set; }
    public int Seq { get; set; }
    public byte[]? Payload { get; set; }
}

public class BenchResult
{
    public ulong EventId { get; set; }
}

public class BenchRow
{
    [PrimaryKey] public ulong Id { get; set; }
    public int Publisher { get; set; }
    public int Seq { get; set; }
    public int Size { get; set; }
}

public interface IBenchRows : IRelation<BenchRow>;

/// <summary>Consumes on every node: one relation upsert per event, and records batch visibility after commit.</summary>
public sealed class BenchHandler : IEventHandler<BenchEvent, BenchResult>
{
    public Action<int, int>? Visible;
    public TopicProbe? Probe;

    public BenchResult? Validate(BenchEvent ev) => null;

    public BenchResult Consume(BenchEvent ev)
    {
        Ambient.Relation<IBenchRows>().Upsert(new()
            { Id = ev.EventId, Publisher = ev.Publisher, Seq = ev.Seq, Size = ev.Payload?.Length ?? 0 });
        if (ev.Seq >= 0 && !Ambient.IsReplay && Visible is { } visible)
        {
            var publisher = ev.Publisher;
            var seq = ev.Seq;
            var eventId = ev.EventId;
            var probe = Probe;
            probe?.Consumed(eventId);
            Ambient.RegisterPostCommit(() =>
            {
                probe?.Visible(eventId);
                visible(publisher, seq);
            });
        }
        return new() { EventId = ev.EventId };
    }
}
