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
| **RemoveBatch** | **100**      | **24**                |     **58.27 μs** |   **5.162 μs** |   **3.414 μs** |         **-** |     **13.43 KB** |
| UpdateBatch | 100      | 24                |    145.97 μs |   5.702 μs |   3.393 μs |         - |     21.02 KB |
| **RemoveBatch** | **100**      | **8192**              |    **723.16 μs** |  **29.938 μs** |  **19.802 μs** |  **187.5000** |   **4827.19 KB** |
| UpdateBatch | 100      | 8192              |  1,747.31 μs |  53.525 μs |  31.852 μs |  250.0000 |  10461.62 KB |
| **RemoveBatch** | **1000**     | **24**                |    **701.86 μs** |  **17.456 μs** |  **11.546 μs** |         **-** |    **166.13 KB** |
| UpdateBatch | 1000     | 24                |  1,753.59 μs |  23.379 μs |  15.464 μs |         - |    269.67 KB |
| **RemoveBatch** | **1000**     | **8192**              | **10,213.49 μs** | **311.666 μs** | **206.148 μs** | **2000.0000** |  **48483.51 KB** |
| UpdateBatch | 1000     | 8192              | 21,108.25 μs | 642.889 μs | 382.573 μs | 3187.5000 | 106768.44 KB |
