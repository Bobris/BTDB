```

BenchmarkDotNet v0.14.0, macOS 26.3.1 (a) (25D771280a) [Darwin 25.3.0]
Apple M2 Max, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.201
  [Host]   : .NET 10.0.5 (10.0.526.15411), Arm64 RyuJIT AdvSIMD
  ShortRun : .NET 10.0.5 (10.0.526.15411), Arm64 RyuJIT AdvSIMD

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
| Method                       | Mean     | Error     | StdDev   | Ratio | RatioSD | Gen0   | Allocated | Alloc Ratio |
|----------------------------- |---------:|----------:|---------:|------:|--------:|-------:|----------:|------------:|
| BtdbRegistration_BtdbResolve | 54.44 ns | 23.223 ns | 1.273 ns |  1.00 |    0.03 | 0.0488 |     408 B |        1.00 |
| BtdbRegistration_MsDiResolve | 19.98 ns |  5.342 ns | 0.293 ns |  0.37 |    0.01 | 0.0029 |      24 B |        0.06 |
