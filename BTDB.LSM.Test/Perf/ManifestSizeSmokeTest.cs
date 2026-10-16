using System.Diagnostics;
using System.Linq;
using BTDB.LSM.Engine;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Perf;

/// Size and codec cost of a full manifest by table count, the evidence for keeping full checkpoints instead of
/// incremental edits (Measurements.md, section 6); prints numbers and asserts nothing about speed.
public class ManifestSizeSmokeTest(ITestOutputHelper output)
{
    [Theory]
    [InlineData(100)]
    [InlineData(1_000)]
    [InlineData(10_000)]
    [InlineData(100_000)]
    public void FullManifestCost(int tableCount)
    {
        // 64 MiB tables, one value file per ten tables, file numbers as a long-running writer allocates them.
        var manifest = new LsmManifest
        {
            Sequence = 123_456, NextFileNumber = (ulong)tableCount * 3, TransactionNumber = 9_876_543,
            CommitUlong = 1_234_567, Ulongs = [1, 2, 3], LevelCount = 7, WriterEpoch = 12, FlushedGeneration = 4_000,
            WalStartChunk = 400_000,
            Tables = Enumerable.Range(0, tableCount)
                .Select(i => (i < 8 ? 0 : 1 + i % 6, (ulong)(i * 3 + 1000), 64L * 1024 * 1024 - i)).ToArray(),
            ValueFiles = Enumerable.Range(0, tableCount / 10)
                .Select(i => ((ulong)(i * 3 + 2), 64L * 1024 * 1024 - i)).ToArray()
        };
        var stopwatch = Stopwatch.StartNew();
        byte[] encoded = [];
        for (var i = 0; i < 20; i++) encoded = manifest.Encode();
        var encode = stopwatch.Elapsed.TotalMilliseconds / 20;
        stopwatch.Restart();
        for (var i = 0; i < 20; i++) LsmManifest.Decode(encoded, "manifest");
        var decode = stopwatch.Elapsed.TotalMilliseconds / 20;
        output.WriteLine($"{tableCount} tables ({tableCount * 64L / 1024} GiB): {encoded.Length} bytes, " +
                         $"{(double)encoded.Length / tableCount:F1} bytes per table, encode {encode:F2} ms, " +
                         $"decode {decode:F2} ms");
    }
}
