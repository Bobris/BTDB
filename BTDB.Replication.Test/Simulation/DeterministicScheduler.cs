using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace BTDB.Replication.Test.Simulation;

internal sealed class SimulationFailureException(string message, Exception inner) : Exception(message, inner);

internal sealed class DeterministicScheduler(ulong seed, Action<string>? failureSink = null)
{
    sealed class Work(long id, long due, Scope scope, Action callback, string description, Action<string> record) : IDisposable
    {
        public readonly long Id = id;
        public readonly long Due = due;
        public readonly Scope Scope = scope;
        public readonly Action Callback = callback;
        public readonly string Description = description;
        public bool Cancelled;
        public void Dispose()
        {
            if (Cancelled) return;
            Cancelled = true;
            record($"cancel {Id}");
        }
    }

    internal sealed class Scope(DeterministicScheduler owner, string name) : IReplicationScheduler, IDisposable
    {
        public string Name { get; } = name;
        bool _paused;
        public bool Paused
        {
            get => _paused;
            set
            {
                _paused = value;
                owner.Record($"pause {Name}={value}");
            }
        }
        public bool Stopped { get; private set; }
        public TimeSpan Elapsed => owner.Elapsed;

        public IDisposable Schedule(TimeSpan delay, Action callback, string description)
        {
            ObjectDisposedException.ThrowIf(Stopped, this);
            ArgumentOutOfRangeException.ThrowIfNegative(delay.Ticks);
            ArgumentNullException.ThrowIfNull(callback);
            var work = new Work(++owner._nextId, checked(owner.Elapsed.Ticks + delay.Ticks), this, callback, description,
                owner.Record);
            owner._work.Add(work);
            owner.Record($"queue {work.Id} due={work.Due} scope={Name} {description}");
            return work;
        }

        public void Dispose()
        {
            Stopped = true;
            owner.Record($"stop {Name}");
        }
    }

    readonly List<Work> _work = [];
    readonly HashSet<string> _scopes = [];
    readonly List<string> _trace = [$"seed={seed}"];
    long _nextId;
    bool _pumping;
    public TimeSpan Elapsed { get; private set; }
    public Action? CheckInvariants { get; set; }
    public string Trace => string.Join('\n', _trace);

    public Scope CreateScope(string name)
    {
        if (!_scopes.Add(name)) throw new ArgumentException("Scope names must be unique.", nameof(name));
        return new Scope(this, name);
    }

    public void Record(string message) => _trace.Add($"{Elapsed.Ticks}: {message}");

    Work? Next()
    {
        _work.RemoveAll(w => w.Cancelled || w.Scope.Stopped);
        return _work.Where(w => !w.Scope.Paused).MinBy(w => (w.Due, w.Id));
    }

    public bool RunNext() => Pump(null, 1, false) != 0;

    // "Idle" means no runnable callback, not completion of paused nodes or undelivered operations.
    public void RunUntilIdle(int maximumSteps = 10000) => Pump(null, maximumSteps, true);

    public void AdvanceBy(TimeSpan duration, int maximumSteps = 10000)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(duration.Ticks);
        Pump(checked(Elapsed.Ticks + duration.Ticks), maximumSteps, true);
    }

    int Pump(long? until, int maximumSteps, bool failOnLimit)
    {
        if (_pumping) throw new InvalidOperationException("Nested scheduler pumping is not deterministic.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumSteps);
        _pumping = true;
        try
        {
            var steps = 0;
            while (Next() is { } next && (!until.HasValue || next.Due <= until.Value))
            {
                if (steps == maximumSteps)
                {
                    if (failOnLimit) throw new InvalidOperationException("Simulation step budget exhausted.");
                    break;
                }
                _work.Remove(next);
                Elapsed = TimeSpan.FromTicks(Math.Max(Elapsed.Ticks, next.Due));
                Record($"run {next.Id} scope={next.Scope.Name} {next.Description}");
                next.Callback();
                CheckInvariants?.Invoke();
                steps++;
            }
            if (until.HasValue)
            {
                Elapsed = TimeSpan.FromTicks(until.Value);
                Record("advance");
                CheckInvariants?.Invoke();
            }
            return steps;
        }
        catch (Exception exception)
        {
            Record($"failure {exception.GetType().Name}: {exception.Message}");
            var trace = Trace;
            try
            {
                (failureSink ?? PersistFailure)(trace);
            }
            catch (Exception sinkError)
            {
                trace += $"\nFailure artifact could not be written: {sinkError.Message}";
            }
            throw new SimulationFailureException(trace, exception);
        }
        finally
        {
            _pumping = false;
        }
    }

    static void PersistFailure(string trace)
    {
        var directory = Path.Combine(AppContext.BaseDirectory, "simulation-failures");
        Directory.CreateDirectory(directory);
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(trace)));
        File.WriteAllText(Path.Combine(directory, $"{hash}.trace.txt"), trace);
    }
}
