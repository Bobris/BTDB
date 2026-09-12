```

BenchmarkDotNet v0.14.0, macOS 26.6.2 (25G83) [Darwin 25.6.0]
Apple M2 Max, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.401
  [Host]   : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD
  ShortRun : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD

Job=ShortRun  InvocationCount=16  IterationCount=10
LaunchCount=1  UnrollFactor=1  WarmupCount=3

```
| Method      | WideValues | Mean     | Error     | StdDev    | Allocated |
|------------ |----------- |---------:|----------:|----------:|----------:|
| **RemoveBatch** | **False**      | **1.601 ms** | **0.0209 ms** | **0.0138 ms** |  **71.85 KB** |
| **RemoveBatch** | **True**       | **1.226 ms** | **0.0128 ms** | **0.0085 ms** |  **87.87 KB** |
