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
| **RemoveBatch** | **100**      | **24**                |     **57.27 μs** |   **1.791 μs** |   **1.066 μs** |         **-** |     **13.43 KB** |
| UpdateBatch | 100      | 24                |    144.03 μs |   4.480 μs |   2.963 μs |         - |     21.02 KB |
| **RemoveBatch** | **100**      | **8192**              |    **487.13 μs** |  **16.780 μs** |  **11.099 μs** |  **187.5000** |   **2444.23 KB** |
| UpdateBatch | 100      | 8192              |  1,631.80 μs |  30.360 μs |  18.067 μs |  250.0000 |  10461.62 KB |
| **RemoveBatch** | **1000**     | **24**                |    **678.46 μs** |   **7.906 μs** |   **4.705 μs** |         **-** |    **166.13 KB** |
| UpdateBatch | 1000     | 24                |  1,698.49 μs |  25.080 μs |  16.589 μs |         - |    269.67 KB |
| **RemoveBatch** | **1000**     | **8192**              |  **8,884.91 μs** | **254.244 μs** | **168.167 μs** | **2000.0000** |  **24437.27 KB** |
| UpdateBatch | 1000     | 8192              | 21,134.28 μs | 832.547 μs | 550.678 μs | 3187.5000 | 106768.44 KB |
