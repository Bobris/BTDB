```

BenchmarkDotNet v0.14.0, macOS 26.6.2 (25G83) [Darwin 25.6.0]
Apple M2 Max, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.401
  [Host]   : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD
  ShortRun : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD

Job=ShortRun  IterationCount=3  LaunchCount=1
WarmupCount=3

```
| Method            | Length | MixedUnicode | Mean         | Error        | StdDev     | Ratio | RatioSD | Gen0   | Allocated | Alloc Ratio |
|------------------ |------- |------------- |-------------:|-------------:|-----------:|------:|--------:|-------:|----------:|------------:|
| **MaterializeString** | **24**     | **False**        |     **24.62 ns** |     **1.371 ns** |   **0.075 ns** |  **1.00** |    **0.00** | **0.0086** |      **72 B** |        **1.00** |
| Fused             | 24     | False        |     10.85 ns |     3.605 ns |   0.198 ns |  0.44 |    0.01 |      - |         - |        0.00 |
|                   |        |              |              |              |            |       |         |        |           |             |
| **MaterializeString** | **24**     | **True**         |    **198.21 ns** |   **201.697 ns** |  **11.056 ns** |  **1.00** |    **0.07** | **0.0086** |      **72 B** |        **1.00** |
| Fused             | 24     | True         |    157.32 ns |    23.857 ns |   1.308 ns |  0.80 |    0.04 |      - |         - |        0.00 |
|                   |        |              |              |              |            |       |         |        |           |             |
| **MaterializeString** | **8192**   | **False**        |  **1,519.13 ns** |   **277.146 ns** |  **15.191 ns** |  **1.00** |    **0.01** | **1.9569** |   **16408 B** |        **1.00** |
| Fused             | 8192   | False        |  1,154.54 ns |   116.186 ns |   6.369 ns |  0.76 |    0.01 |      - |         - |        0.00 |
|                   |        |              |              |              |            |       |         |        |           |             |
| **MaterializeString** | **8192**   | **True**         | **64,180.26 ns** | **7,838.201 ns** | **429.638 ns** |  **1.00** |    **0.01** | **1.9531** |   **16408 B** |        **1.00** |
| Fused             | 8192   | True         | 51,775.08 ns | 8,074.690 ns | 442.601 ns |  0.81 |    0.01 |      - |         - |        0.00 |
