using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace BTDB.Replication;

/// <summary>
/// Production <see cref="IReplicationScheduler"/>: elapsed time from a clock that is never slewed by time
/// synchronization and keeps advancing while the process is stopped, plus timer callbacks serialized on the thread
/// pool. Lease authority relies on <see cref="Elapsed"/> alone; timers only decide when work runs.
/// <list type="bullet">
/// <item>Linux: CLOCK_MONOTONIC_RAW, the unadjusted hardware counter (TSC or the hypervisor's reference counter). NTP
/// daemons slew CLOCK_MONOTONIC and CLOCK_BOOTTIME, chrony by up to 8.3 % by default, which would break any
/// practical drift bound. It keeps counting while the process or the VM is paused, but not during system suspend,
/// which servers and cloud VMs do not use; do not run a replication node on a host that suspends.</item>
/// <item>macOS: CLOCK_MONOTONIC_RAW, which is also unadjusted and continues while the system sleeps.</item>
/// <item>Windows: <see cref="Environment.TickCount64"/> (interrupt time, including sleep and hibernation, not adjusted
/// by time synchronization) with 10–16 ms resolution; configure a safety margin well above it.</item>
/// </list>
/// Configure the lease drift bound for the hardware counter against the service clock (1000 ppm is conservative for
/// invariant TSC and hypervisor counters) and a safety margin of at least 250 ms; see ReplicationHosting.md.
/// </summary>
public sealed class SystemReplicationScheduler : IReplicationScheduler
{
    readonly long _start = Now();
    readonly object _lock = new();
    readonly object _callbacks = new();

    public TimeSpan Elapsed => TimeSpan.FromTicks(Now() - _start);

    public IDisposable Schedule(TimeSpan delay, Action callback, string description)
    {
        ArgumentNullException.ThrowIfNull(callback);
        ArgumentOutOfRangeException.ThrowIfNegative(delay.Ticks);
        lock (_lock) return new Work(this, delay, callback);
    }

    const int ClockMonotonicRaw = 4; // Same value on Linux and macOS.

    [StructLayout(LayoutKind.Sequential)]
    struct Timespec
    {
        public long Seconds;
        public long Nanoseconds;
    }

    [DllImport("libc", EntryPoint = "clock_gettime", SetLastError = true)]
    static extern int ClockGetTime(int clock, out Timespec time);

    /// <summary>Ticks (100 ns) of the platform clock described on the class; only differences are meaningful.</summary>
    internal static long Now()
    {
        if (OperatingSystem.IsWindows()) return Environment.TickCount64 * TimeSpan.TicksPerMillisecond;
        if (ClockGetTime(ClockMonotonicRaw, out var time) != 0)
            throw new InvalidOperationException($"clock_gettime(CLOCK_MONOTONIC_RAW) failed with {Marshal.GetLastPInvokeError()}.");
        return time.Seconds * TimeSpan.TicksPerSecond + time.Nanoseconds / 100;
    }

    sealed class Work : IDisposable
    {
        readonly SystemReplicationScheduler _owner;
        readonly Action _callback;
        readonly Timer _timer;
        bool _closed;

        public Work(SystemReplicationScheduler owner, TimeSpan delay, Action callback)
        {
            _owner = owner;
            _callback = callback;
            // Created stopped and then started, so the callback can never observe an unassigned timer.
            _timer = new(_ => Run(), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            _timer.Change(delay, Timeout.InfiniteTimeSpan);
        }

        void Run()
        {
            // One callback at a time; disposal before the callback starts prevents it, disposal during it does not wait.
            lock (_owner._callbacks)
            {
                lock (_owner._lock)
                {
                    if (_closed) return;
                    _closed = true;
                    _timer.Dispose();
                }
                _callback();
            }
        }

        public void Dispose()
        {
            lock (_owner._lock)
            {
                _closed = true;
                _timer.Dispose();
            }
        }
    }
}
