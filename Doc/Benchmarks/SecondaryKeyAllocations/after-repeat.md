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
| **RemoveBatch** | **100**      | **24**                |     **62.79 μs** |   **5.338 μs** |   **3.531 μs** |         **-** |     **13.43 KB** |
| UpdateBatch | 100      | 24                |    154.20 μs |  12.778 μs |   8.452 μs |         - |     21.02 KB |
| **RemoveBatch** | **100**      | **8192**              |    **784.79 μs** |  **62.258 μs** |  **41.180 μs** |  **187.5000** |   **4827.19 KB** |
| UpdateBatch | 100      | 8192              |  1,791.75 μs |  76.095 μs |  50.332 μs |  250.0000 |  10461.62 KB |
| **RemoveBatch** | **1000**     | **24**                |    **726.91 μs** |  **14.588 μs** |   **9.649 μs** |         **-** |    **166.13 KB** |
| UpdateBatch | 1000     | 24                |  1,800.38 μs |  47.298 μs |  31.285 μs |         - |    269.67 KB |
| **RemoveBatch** | **1000**     | **8192**              | **10,926.72 μs** | **371.130 μs** | **245.480 μs** | **2000.0000** |  **48483.51 KB** |
| UpdateBatch | 1000     | 8192              | 21,957.37 μs | 562.966 μs | 372.367 μs | 3187.5000 | 106768.44 KB |
