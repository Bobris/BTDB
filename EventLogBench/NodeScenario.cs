using System.Buffers.Binary;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.Replication.EventLog;
using BTDB.Replication.EventSourcing;

namespace EventLogBench;

/// <summary>
/// One scenario on one node. Every node publishes its share of the load and consumes the whole topic. Times are
/// milliseconds since the driver's common start instant, converted to this node's clock by the driver's offset, so the
/// driver can join publications and deliveries of all nodes.
/// </summary>
public sealed class NodeScenario : IAsyncDisposable
{
    readonly ScenarioSpec _spec;
    readonly int _self;
    readonly EventLogService _log;
    readonly string _dataDirectory;
    readonly CancellationTokenSource _stop = new();
    readonly SeqTimes _scheduled = new();
    readonly SeqTimes _receipt = new();
    readonly SeqTimes[] _delivered;
    long _t0LocalUs;
    long _published, _errors, _pending, _deliveredCount;
    int _nextSeq;
    volatile string? _fault;
    Task _consumer = Task.CompletedTask;
    Task _load = Task.CompletedTask;
    IEventTopic? _topic;
    BTreeKeyValueDB? _keyValues;
    ObjectDB? _objects;
    EventPublisher? _publisher;
    TopicProbe? _probe;

    public object? Breakdown => _probe?.Summary();

    public NodeScenario(ScenarioSpec spec, int self, EventLogService log, string dataRoot)
    {
        _spec = spec;
        _self = self;
        _log = log;
        _dataDirectory = Path.Combine(dataRoot, spec.Topic);
        _delivered = new SeqTimes[spec.Nodes];
        for (var i = 0; i < _delivered.Length; i++) _delivered[i] = new();
        _t0LocalUs = BenchClock.NowUs;
    }

    public ScenarioSpec Spec => _spec;

    float NowMs => (float)((BenchClock.NowUs - Volatile.Read(ref _t0LocalUs)) / 1000.0);

    string DataTopic => _spec.Mode == BenchMode.EventLog ? _spec.Topic : _spec.Topic + "-d";

    /// <summary>Starts consumption and publishes one warmup record, which also makes the first prepared node the
    /// topic owner and resolves the owner on the others.</summary>
    public async Task PrepareAsync(CancellationToken cancellation)
    {
        if (_spec.Mode == BenchMode.EventLog)
        {
            _topic = _log.GetTopic(_spec.Topic);
            await _topic.PublishAsync(Payload(-1, new byte[Math.Max(8, _spec.PayloadBytes)]), cancellation);
            _consumer = Task.Run(ConsumeAsync);
            return;
        }
        Directory.CreateDirectory(_dataDirectory);
        _keyValues = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = new OnDiskFileCollection(_dataDirectory), RequireExplicitTransactions = true,
            Compression = new NoCompressionStrategy()
        });
        _objects = new ObjectDB();
        _objects.Open(_keyValues, false, new DBOptions().WithoutAutoRegistrationOfRelations());
        await _objects.InitializeRelations([typeof(IBenchRows)]);
        _probe = new TopicProbe(_log.GetTopic(_spec.Topic + "-d"));
        var handler = new BenchHandler { Visible = (p, s) => Delivered(p, s), Probe = _probe };
        _publisher = new EventPublisher(_objects, _probe, _log.GetTopic(_spec.Topic + "-m"),
            new EventDispatcher().Register<BenchEvent, BenchResult>(handler));
        await _publisher.StartAsync(cancellation);
        _consumer = _publisher.Completion;
        await _publisher.PublishAndWaitAsync(new BenchEvent { Publisher = _self, Seq = -1, Payload = new byte[_spec.PayloadBytes] },
            cancellation);
    }

    public void Start(StartRequest request)
    {
        Volatile.Write(ref _t0LocalUs, request.T0LocalUs);
        _load = _spec.Load switch
        {
            LoadKind.Open => Task.Run(() => OpenLoadAsync(false)),
            LoadKind.Closed => Task.Run(() => ClosedLoadAsync(false)),
            _ => Task.WhenAll(Task.Run(() => OpenLoadAsync(true)), Task.Run(() => ClosedLoadAsync(true)))
        };
        _ = SampleLingerAsync();
    }

    public NodeStatus Status => new(_self, _load.IsCompleted, Interlocked.Read(ref _published), Interlocked.Read(ref _errors),
        Interlocked.Read(ref _pending), Interlocked.Read(ref _deliveredCount), _log.IsOwner(DataTopic),
        _fault ?? (_consumer.IsFaulted ? _consumer.Exception!.GetBaseException().ToString() : null), LingerSummary());

    readonly List<double> _linger = [];

    async Task SampleLingerAsync()
    {
        while (!_load.IsCompleted && !_stop.IsCancellationRequested)
        {
            if (_log.CommitLinger(DataTopic) is { } linger)
                lock (_linger) _linger.Add(linger.TotalMilliseconds);
            await Task.Delay(50);
        }
    }

    double[]? LingerSummary()
    {
        lock (_linger)
        {
            if (_linger.Count == 0) return null;
            var sorted = _linger.Order().ToArray();
            return [sorted[sorted.Length / 10], sorted[sorted.Length / 2], sorted[sorted.Length * 9 / 10]];
        }
    }

    void Delivered(int publisher, int seq)
    {
        if ((uint)publisher >= (uint)_delivered.Length) return;
        _delivered[publisher].Set(seq, NowMs);
        Interlocked.Increment(ref _deliveredCount);
    }

    ReadOnlyMemory<byte> Payload(int seq, byte[] buffer)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buffer, _self);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(4), seq);
        return buffer;
    }

    async Task ConsumeAsync()
    {
        try
        {
            await foreach (var record in _topic!.ReadAsync(0, null, _stop.Token))
            {
                var span = record.Payload.Span;
                var seq = BinaryPrimitives.ReadInt32LittleEndian(span[4..]);
                if (seq >= 0) Delivered(BinaryPrimitives.ReadInt32LittleEndian(span), seq);
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        catch (Exception error) { _fault = "consumer: " + error; }
    }

    // Publishing one record: the scheduled time is when the call was due, so queueing delay is part of every latency.
    Task PublishOneAsync(int seq, float scheduledMs, byte[] buffer)
    {
        _scheduled.Set(seq, scheduledMs);
        Interlocked.Increment(ref _pending);
        Task task;
        try
        {
            if (_spec.Mode == BenchMode.EventLog)
                task = _topic!.PublishAsync(Payload(seq, buffer), _stop.Token).AsTask();
            else
                task = _publisher!.PublishAndWaitAsync(new BenchEvent
                    { Publisher = _self, Seq = seq, Payload = new byte[_spec.PayloadBytes] }, _stop.Token);
        }
        catch (Exception error)
        {
            task = Task.FromException(error);
        }
        return task.ContinueWith(t =>
        {
            if (t.IsCompletedSuccessfully)
            {
                _receipt.Set(seq, NowMs);
                Interlocked.Increment(ref _published);
            }
            else
            {
                Interlocked.Increment(ref _errors);
                if (t.Exception is { } e && _fault == null && !_stop.IsCancellationRequested)
                    _fault = "publish: " + e.GetBaseException().Message;
            }
            Interlocked.Decrement(ref _pending);
        }, TaskScheduler.Default);
    }

    async Task OpenLoadAsync(bool mixed)
    {
        try
        {
            var interval = 1000.0 / _spec.TotalRate;
            var endMs = _spec.Seconds * 1000.0;
            var buffer = new byte[Math.Max(8, _spec.PayloadBytes)];
            var tasks = new List<Task>();
            for (var k = 0; ; k++)
            {
                var dueMs = (k * _spec.Nodes + _self) * interval;
                if (dueMs >= endMs) break;
                while (true)
                {
                    var wait = dueMs - NowMs;
                    if (wait <= 0) break;
                    if (wait > 2) await Task.Delay(1);
                    else Thread.SpinWait(30);
                }
                tasks.Add(PublishOneAsync(mixed ? 2 * k : k, (float)dueMs, buffer));
            }
            if (!mixed) _nextSeq = tasks.Count;
            await Task.WhenAll(tasks);
        }
        catch (Exception error) { _fault ??= "load: " + error; }
    }

    async Task ClosedLoadAsync(bool mixed)
    {
        try
        {
            while (NowMs < 0) await Task.Delay(1);
            var endMs = _spec.Seconds * 1000.0;
            await Task.WhenAll(Enumerable.Range(0, _spec.Concurrency).Select(_ => Task.Run(async () =>
            {
                var buffer = new byte[Math.Max(8, _spec.PayloadBytes)];
                while (NowMs < endMs && !_stop.IsCancellationRequested)
                {
                    var seq = Interlocked.Increment(ref _nextSeq) - 1;
                    await PublishOneAsync(mixed ? 2 * seq + 1 : seq, NowMs, buffer);
                }
            })));
        }
        catch (Exception error) { _fault ??= "load: " + error; }
    }

    public byte[] Report()
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream))
        {
            writer.Write(_self);
            _scheduled.WriteTo(writer);
            _receipt.WriteTo(writer);
            writer.Write(_delivered.Length);
            foreach (var delivered in _delivered) delivered.WriteTo(writer);
        }
        return stream.ToArray();
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        try { await _load; } catch { }
        if (_publisher != null)
            try { await _publisher.DisposeAsync(); } catch { }
        try { await _consumer; } catch { }
        _objects?.Dispose();
        _keyValues?.Dispose();
        try { if (Directory.Exists(_dataDirectory)) Directory.Delete(_dataDirectory, true); } catch { }
    }
}
