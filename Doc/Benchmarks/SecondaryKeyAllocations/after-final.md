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
| **RemoveBatch** | **100**      | **24**                |     **62.32 μs** |   **4.789 μs** |   **3.168 μs** |         **-** |     **13.43 KB** |
| UpdateBatch | 100      | 24                |    151.02 μs |   9.485 μs |   6.274 μs |         - |     21.02 KB |
| **RemoveBatch** | **100**      | **8192**              |    **815.45 μs** |  **32.848 μs** |  **19.547 μs** |  **187.5000** |   **4827.19 KB** |
| UpdateBatch | 100      | 8192              |  1,828.31 μs |  86.944 μs |  57.508 μs |  250.0000 |  10461.62 KB |
| **RemoveBatch** | **1000**     | **24**                |    **721.82 μs** |  **27.000 μs** |  **17.859 μs** |         **-** |    **166.13 KB** |
| UpdateBatch | 1000     | 24                |  1,802.41 μs |  57.624 μs |  38.114 μs |         - |    269.67 KB |
| **RemoveBatch** | **1000**     | **8192**              | **10,816.57 μs** | **261.716 μs** | **173.109 μs** | **2000.0000** |  **48483.51 KB** |
| UpdateBatch | 1000     | 8192              | 22,290.08 μs | 424.710 μs | 280.919 μs | 3187.5000 | 106768.44 KB |
