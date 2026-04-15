```

BenchmarkDotNet v0.14.0, macOS 26.3.1 (a) (25D771280a) [Darwin 25.3.0]
Apple M2 Max, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.201
  [Host]   : .NET 10.0.5 (10.0.526.15411), Arm64 RyuJIT AdvSIMD
  ShortRun : .NET 10.0.5 (10.0.526.15411), Arm64 RyuJIT AdvSIMD

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
| Method                                            | Mean      | Error      | StdDev    | Ratio | RatioSD | Gen0   | Allocated | Alloc Ratio |
|-------------------------------------------------- |----------:|-----------:|----------:|------:|--------:|-------:|----------:|------------:|
| BtdbRegistration_BtdbResolve                      |  7.173 ns |  0.7296 ns | 0.0400 ns |  1.00 |    0.01 | 0.0029 |      24 B |        1.00 |
| BtdbRegistration_MsDiResolve                      | 10.955 ns |  0.8425 ns | 0.0462 ns |  1.53 |    0.01 | 0.0029 |      24 B |        1.00 |
| MsDiRegistration_BtdbResolve                      | 12.132 ns |  1.4194 ns | 0.0778 ns |  1.69 |    0.01 | 0.0029 |      24 B |        1.00 |
| BtdbRegistration_BtdbFuncResolve                  |  3.090 ns |  0.8248 ns | 0.0452 ns |  0.43 |    0.01 | 0.0029 |      24 B |        1.00 |
| MsDiRegistration_BtdbFuncResolve                  |  9.673 ns | 19.2921 ns | 1.0575 ns |  1.35 |    0.13 | 0.0029 |      24 B |        1.00 |
| BtdbRegistration_BtdbZeroArgFuncResolve           |  3.072 ns |  1.6203 ns | 0.0888 ns |  0.43 |    0.01 | 0.0029 |      24 B |        1.00 |
| MsDiRegistration_BtdbZeroArgFuncResolve           |  8.949 ns |  0.2045 ns | 0.0112 ns |  1.25 |    0.01 | 0.0029 |      24 B |        1.00 |
| MsDiRegistration_MsDiResolve                      |  7.129 ns |  0.0357 ns | 0.0020 ns |  0.99 |    0.00 | 0.0029 |      24 B |        1.00 |
| Scoped_BtdbRegistration_BtdbResolve               |  9.408 ns |  0.2610 ns | 0.0143 ns |  1.31 |    0.01 |      - |         - |        0.00 |
| Scoped_BtdbRegistration_MsDiResolve               | 15.880 ns |  1.2623 ns | 0.0692 ns |  2.21 |    0.01 |      - |         - |        0.00 |
| Scoped_MsDiRegistration_BtdbResolve               | 20.224 ns |  1.9207 ns | 0.1053 ns |  2.82 |    0.02 |      - |         - |        0.00 |
| Scoped_MsDiRegistration_MsDiResolve               | 15.970 ns |  0.5730 ns | 0.0314 ns |  2.23 |    0.01 |      - |         - |        0.00 |
| Scoped_BtdbRegistration_BtdbFuncResolve           |  5.674 ns |  2.0601 ns | 0.1129 ns |  0.79 |    0.01 |      - |         - |        0.00 |
| Scoped_MsDiRegistration_BtdbFuncResolve           | 17.382 ns |  0.9044 ns | 0.0496 ns |  2.42 |    0.01 |      - |         - |        0.00 |
| Scoped_BtdbRegistration_BtdbZeroArgFuncResolve    |  5.492 ns |  0.4185 ns | 0.0229 ns |  0.77 |    0.00 |      - |         - |        0.00 |
| Scoped_MsDiRegistration_BtdbZeroArgFuncResolve    | 17.477 ns |  3.5859 ns | 0.1966 ns |  2.44 |    0.03 |      - |         - |        0.00 |
| Singleton_BtdbRegistration_BtdbResolve            |  6.565 ns |  0.5439 ns | 0.0298 ns |  0.92 |    0.01 |      - |         - |        0.00 |
| Singleton_BtdbRegistration_MsDiResolve            |  4.032 ns |  0.2095 ns | 0.0115 ns |  0.56 |    0.00 |      - |         - |        0.00 |
| Singleton_MsDiRegistration_BtdbResolve            |  9.045 ns |  1.0420 ns | 0.0571 ns |  1.26 |    0.01 |      - |         - |        0.00 |
| Singleton_MsDiRegistration_MsDiResolve            |  3.979 ns |  1.3836 ns | 0.0758 ns |  0.55 |    0.01 |      - |         - |        0.00 |
| Singleton_BtdbRegistration_BtdbFuncResolve        |  2.252 ns |  0.8705 ns | 0.0477 ns |  0.31 |    0.01 |      - |         - |        0.00 |
| Singleton_MsDiRegistration_BtdbFuncResolve        |  5.889 ns |  0.2838 ns | 0.0156 ns |  0.82 |    0.00 |      - |         - |        0.00 |
| Singleton_BtdbRegistration_BtdbZeroArgFuncResolve |  2.192 ns |  1.0535 ns | 0.0577 ns |  0.31 |    0.01 |      - |         - |        0.00 |
| Singleton_MsDiRegistration_BtdbZeroArgFuncResolve |  5.871 ns |  0.8109 ns | 0.0444 ns |  0.82 |    0.01 |      - |         - |        0.00 |
