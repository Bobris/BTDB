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
| **RemoveBatch** | **100**      | **24**                |     **59.47 μs** |   **2.438 μs** |   **1.613 μs** |         **-** |     **6.4 KB** |
| UpdateBatch | 100      | 24                |    141.41 μs |   5.755 μs |   3.425 μs |         - |   13.99 KB |
| **RemoveBatch** | **100**      | **8192**              |    **439.43 μs** |  **40.795 μs** |  **24.276 μs** |         **-** |   **841.9 KB** |
| UpdateBatch | 100      | 8192              |  1,667.41 μs |  83.092 μs |  54.961 μs |   62.5000 | 8859.29 KB |
| **RemoveBatch** | **1000**     | **24**                |    **681.01 μs** |  **20.933 μs** |  **13.846 μs** |         **-** |   **95.82 KB** |
| UpdateBatch | 1000     | 24                |  1,713.67 μs |  30.228 μs |  19.994 μs |         - |  199.35 KB |
| **RemoveBatch** | **1000**     | **8192**              |  **7,545.35 μs** | **183.430 μs** | **121.327 μs** |         **-** | **8413.84 KB** |
| UpdateBatch | 1000     | 8192              | 20,527.72 μs | 483.845 μs | 320.034 μs | 1250.0000 |   90745 KB |
