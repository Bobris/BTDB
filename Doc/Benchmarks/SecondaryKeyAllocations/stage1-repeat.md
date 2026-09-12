```

BenchmarkDotNet v0.14.0, macOS 26.6.2 (25G83) [Darwin 25.6.0]
Apple M2 Max, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.401
  [Host]   : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD
  ShortRun : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD

Job=ShortRun  InvocationCount=16  IterationCount=10
LaunchCount=1  UnrollFactor=1  WarmupCount=3

```
| Method      | RowCount | CustomFieldLength | Mean         | Error      | StdDev     | Gen0      | Allocated   |
|------------ |--------- |------------------ |-------------:|-----------:|-----------:|----------:|------------:|
| **RemoveBatch** | **100**      | **24**                |     **62.32 μs** |   **3.189 μs** |   **1.898 μs** |         **-** |    **13.43 KB** |
| **RemoveBatch** | **100**      | **8192**              |    **811.35 μs** |  **20.574 μs** |  **13.609 μs** |  **187.5000** |  **4827.19 KB** |
| **RemoveBatch** | **1000**     | **24**                |    **729.27 μs** |  **23.467 μs** |  **15.522 μs** |         **-** |   **166.13 KB** |
| **RemoveBatch** | **1000**     | **8192**              | **10,896.27 μs** | **188.103 μs** | **124.418 μs** | **2000.0000** | **48483.51 KB** |
