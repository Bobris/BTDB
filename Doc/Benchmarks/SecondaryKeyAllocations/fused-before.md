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
| **RemoveBatch** | **100**      | **24**                |     **65.03 μs** |   **5.183 μs** |   **3.428 μs** |         **-** |     **13.43 KB** |
| UpdateBatch | 100      | 24                |    157.71 μs |  13.889 μs |   9.187 μs |         - |     21.02 KB |
| **RemoveBatch** | **100**      | **8192**              |    **511.77 μs** |  **31.314 μs** |  **20.712 μs** |  **187.5000** |   **2444.23 KB** |
| UpdateBatch | 100      | 8192              |  1,875.89 μs |  71.766 μs |  42.707 μs |  250.0000 |  10461.62 KB |
| **RemoveBatch** | **1000**     | **24**                |    **756.11 μs** |  **20.296 μs** |  **13.424 μs** |         **-** |    **166.13 KB** |
| UpdateBatch | 1000     | 24                |  1,818.85 μs |  87.541 μs |  57.903 μs |         - |    269.67 KB |
| **RemoveBatch** | **1000**     | **8192**              |  **8,575.01 μs** | **124.435 μs** |  **74.049 μs** | **2000.0000** |  **24437.27 KB** |
| UpdateBatch | 1000     | 8192              | 22,419.26 μs | 392.502 μs | 259.616 μs | 3187.5000 | 106768.42 KB |
