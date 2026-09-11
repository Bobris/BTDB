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
| **RemoveBatch** | **100**      | **24**                |     **71.22 μs** |   **6.198 μs** |   **4.100 μs** |         **-** |     **28.28 KB** |
| UpdateBatch | 100      | 24                |    163.83 μs |   6.043 μs |   3.596 μs |         - |     35.87 KB |
| **RemoveBatch** | **100**      | **8192**              |    **788.65 μs** |  **48.161 μs** |  **28.660 μs** |  **187.5000** |   **4842.04 KB** |
| UpdateBatch | 100      | 8192              |  1,810.01 μs |  40.105 μs |  20.976 μs |  250.0000 |  10476.46 KB |
| **RemoveBatch** | **1000**     | **24**                |    **814.50 μs** |  **28.668 μs** |  **18.962 μs** |         **-** |    **314.57 KB** |
| UpdateBatch | 1000     | 24                |  1,942.04 μs |  55.285 μs |  36.568 μs |         - |     418.1 KB |
| **RemoveBatch** | **1000**     | **8192**              | **12,613.16 μs** | **350.966 μs** | **208.854 μs** | **2000.0000** |  **48631.95 KB** |
| UpdateBatch | 1000     | 8192              | 25,490.95 μs | 319.946 μs | 190.395 μs | 3187.5000 | 106916.88 KB |
