```

BenchmarkDotNet v0.14.0, macOS 26.6.2 (25G83) [Darwin 25.6.0]
Apple M2 Max, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.401
  [Host]   : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD
  ShortRun : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD

Job=ShortRun  InvocationCount=16  IterationCount=10  
LaunchCount=1  UnrollFactor=1  WarmupCount=3  

```
| Method      | RowCount | CustomFieldLength | Mean        | Error      | StdDev    | Gen0      | Allocated   |
|------------ |--------- |------------------ |------------:|-----------:|----------:|----------:|------------:|
| **RemoveBatch** | **100**      | **24**                |    **62.73 μs** |   **3.941 μs** |  **2.607 μs** |         **-** |    **13.43 KB** |
| **RemoveBatch** | **100**      | **8192**              |   **506.94 μs** |  **30.670 μs** | **20.286 μs** |  **187.5000** |  **2444.23 KB** |
| **RemoveBatch** | **1000**     | **24**                |   **701.59 μs** |  **20.635 μs** | **13.649 μs** |         **-** |   **166.13 KB** |
| **RemoveBatch** | **1000**     | **8192**              | **8,341.45 μs** | **160.861 μs** | **95.726 μs** | **2000.0000** | **24437.27 KB** |
