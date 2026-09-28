using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using BTDB.Replication;

namespace DBBenchmark.Replication;

/// Compares the Linux clocks a replication node could use for lease deadlines against CLOCK_MONOTONIC_RAW, the
/// SystemReplicationScheduler clock: rates in ppm over the run and the largest step between samples.
static class ClockMeasurement
{
    [StructLayout(LayoutKind.Sequential)]
    struct Timespec
    {
        public long Seconds;
        public long Nanoseconds;
    }

    [DllImport("libc", EntryPoint = "clock_gettime")]
    static extern int ClockGetTime(int clock, out Timespec time);

    static readonly (string Name, int Id)[] Clocks =
        [("CLOCK_REALTIME", 0), ("CLOCK_MONOTONIC", 1), ("CLOCK_BOOTTIME", 7)];

    static long Read(int id) => ClockGetTime(id, out var t) == 0
        ? t.Seconds * 1_000_000_000 + t.Nanoseconds : throw new InvalidOperationException("clock_gettime failed");

    // One sample of a clock and CLOCK_MONOTONIC_RAW (ticks) at the same instant: a read bracketed by raw reads at
    // most 1 µs apart, so a preemption between them cannot look like a rate change.
    static (long Clock, long Raw) Sample(int id)
    {
        while (true)
        {
            var before = SystemReplicationScheduler.Now();
            var clock = Read(id);
            var after = SystemReplicationScheduler.Now();
            if (after - before <= 10) return (clock, (before + after) / 2);
        }
    }

    public static async Task RunAsync(int seconds)
    {
        if (!OperatingSystem.IsLinux())
        {
            Console.WriteLine("clock: Linux only");
            return;
        }
        var start = Array.ConvertAll(Clocks, c => Sample(c.Id));
        var previous = ((long Clock, long Raw)[])start.Clone();
        var maxStep = new double[Clocks.Length];
        for (var i = 1; i <= Math.Max(1, seconds); i++)
        {
            await Task.Delay(1000);
            for (var c = 0; c < Clocks.Length; c++)
            {
                var now = Sample(Clocks[c].Id);
                maxStep[c] = Math.Max(maxStep[c], Math.Abs(Ppm(previous[c], now)));
                previous[c] = now;
            }
            if (i % 60 != 0 && i != seconds) continue;
            Console.Write($"after {i,5} s:");
            for (var c = 0; c < Clocks.Length; c++)
                Console.Write($"  {Clocks[c].Name} {Ppm(start[c], previous[c]):+0.00;-0.00} ppm (max 1 s {maxStep[c]:0.0})");
            Console.WriteLine();
        }
    }

    // How much faster the clock advanced than CLOCK_MONOTONIC_RAW between two samples, in ppm.
    static double Ppm((long Clock, long Raw) from, (long Clock, long Raw) to)
    {
        var raw = (to.Raw - from.Raw) * 100.0;
        return ((to.Clock - from.Clock) - raw) / raw * 1e6;
    }
}
