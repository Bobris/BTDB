```

BenchmarkDotNet v0.14.0, macOS 26.6.2 (25G83) [Darwin 25.6.0]
Apple M2 Max, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.401
  [Host]   : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD
  ShortRun : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD

Job=ShortRun  InvocationCount=16  IterationCount=10
LaunchCount=1  UnrollFactor=1  WarmupCount=3

```
| Method      | RowCount | CustomFieldLength | Mean         | Error      | StdDev     | Gen0      | Allocated    |
|------------ |--------- |------------------ |-------------:|-----------:|-----------:|----------:|-------------:|
| **RemoveBatch** | **100**      | **24**                |     **68.35 μs** |   **4.191 μs** |   **2.494 μs** |         **-** |     **28.28 KB** |
| UpdateBatch | 100      | 24                |    158.26 μs |  12.846 μs |   8.497 μs |         - |     35.87 KB |
| **RemoveBatch** | **100**      | **8192**              |    **863.73 μs** |  **61.273 μs** |  **40.528 μs** |  **187.5000** |   **4842.04 KB** |
| UpdateBatch | 100      | 8192              |  1,847.56 μs |  87.096 μs |  57.608 μs |  250.0000 |  10476.46 KB |
| **RemoveBatch** | **1000**     | **24**                |    **832.49 μs** |  **20.664 μs** |  **12.297 μs** |         **-** |    **314.57 KB** |
| UpdateBatch | 1000     | 24                |  1,919.42 μs |  67.625 μs |  44.730 μs |         - |     418.1 KB |
| **RemoveBatch** | **1000**     | **8192**              | **12,832.61 μs** | **284.152 μs** | **187.949 μs** | **2000.0000** |  **48631.95 KB** |
| UpdateBatch | 1000     | 8192              | 26,008.46 μs | 610.163 μs | 403.585 μs | 3187.5000 | 106916.88 KB |
