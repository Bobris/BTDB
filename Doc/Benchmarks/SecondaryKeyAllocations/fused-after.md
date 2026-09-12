```

BenchmarkDotNet v0.14.0, macOS 26.6.2 (25G83) [Darwin 25.6.0]
Apple M2 Max, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.401
  [Host]   : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD
  ShortRun : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD

Job=ShortRun  InvocationCount=16  IterationCount=10
LaunchCount=1  UnrollFactor=1  WarmupCount=3

```
| Method      | RowCount | CustomFieldLength | Mean         | Error      | StdDev     | Gen0      | Allocated  |
|------------ |--------- |------------------ |-------------:|-----------:|-----------:|----------:|-----------:|
| **RemoveBatch** | **100**      | **24**                |     **61.25 μs** |   **4.155 μs** |   **2.472 μs** |         **-** |     **6.4 KB** |
| UpdateBatch | 100      | 24                |    156.98 μs |  11.957 μs |   7.909 μs |         - |   13.99 KB |
| **RemoveBatch** | **100**      | **8192**              |    **453.34 μs** |  **34.060 μs** |  **17.814 μs** |         **-** |  **841.89 KB** |
| UpdateBatch | 100      | 8192              |  1,762.47 μs |  21.729 μs |  14.372 μs |   62.5000 | 8859.28 KB |
| **RemoveBatch** | **1000**     | **24**                |    **715.92 μs** |  **17.820 μs** |  **11.787 μs** |         **-** |   **95.81 KB** |
| UpdateBatch | 1000     | 24                |  1,821.34 μs |  23.098 μs |  15.278 μs |         - |  199.35 KB |
| **RemoveBatch** | **1000**     | **8192**              |  **7,777.51 μs** |  **80.477 μs** |  **47.891 μs** |         **-** | **8413.84 KB** |
| UpdateBatch | 1000     | 8192              | 20,094.50 μs | 336.705 μs | 200.368 μs | 1250.0000 |   90745 KB |
