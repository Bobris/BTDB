```

BenchmarkDotNet v0.14.0, macOS 26.3.1 (a) (25D771280a) [Darwin 25.3.0]
Apple M2 Max, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.201
  [Host]   : .NET 10.0.5 (10.0.526.15411), Arm64 RyuJIT AdvSIMD
  ShortRun : .NET 10.0.5 (10.0.526.15411), Arm64 RyuJIT AdvSIMD

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
| Method                                            | Mean      | Error     | StdDev    | Ratio | Gen0   | Allocated | Alloc Ratio |
|-------------------------------------------------- |----------:|----------:|----------:|------:|-------:|----------:|------------:|
| BtdbRegistration_BtdbResolve                      | 53.460 ns | 1.7604 ns | 0.0965 ns |  1.00 | 0.0488 |     408 B |        1.00 |
| BtdbRegistration_MsDiResolve                      | 20.206 ns | 0.5377 ns | 0.0295 ns |  0.38 | 0.0029 |      24 B |        0.06 |
| MsDiRegistration_BtdbResolve                      | 39.227 ns | 5.4377 ns | 0.2981 ns |  0.73 | 0.0373 |     312 B |        0.76 |
| BtdbRegistration_BtdbFuncResolve                  |  3.116 ns | 0.0556 ns | 0.0030 ns |  0.06 | 0.0029 |      24 B |        0.06 |
| MsDiRegistration_BtdbFuncResolve                  |  8.862 ns | 1.1928 ns | 0.0654 ns |  0.17 | 0.0029 |      24 B |        0.06 |
| BtdbRegistration_BtdbZeroArgFuncResolve           |  3.094 ns | 0.3868 ns | 0.0212 ns |  0.06 | 0.0029 |      24 B |        0.06 |
| MsDiRegistration_BtdbZeroArgFuncResolve           |  8.914 ns | 0.8852 ns | 0.0485 ns |  0.17 | 0.0029 |      24 B |        0.06 |
| MsDiRegistration_MsDiResolve                      |  7.090 ns | 0.2860 ns | 0.0157 ns |  0.13 | 0.0029 |      24 B |        0.06 |
| Scoped_BtdbRegistration_BtdbResolve               | 60.362 ns | 1.9474 ns | 0.1067 ns |  1.13 | 0.0497 |     416 B |        1.02 |
| Scoped_BtdbRegistration_MsDiResolve               | 15.779 ns | 0.3638 ns | 0.0199 ns |  0.30 |      - |         - |        0.00 |
| Scoped_MsDiRegistration_BtdbResolve               | 48.872 ns | 3.8335 ns | 0.2101 ns |  0.91 | 0.0344 |     288 B |        0.71 |
| Scoped_MsDiRegistration_MsDiResolve               | 15.618 ns | 0.4516 ns | 0.0248 ns |  0.29 |      - |         - |        0.00 |
| Scoped_BtdbRegistration_BtdbFuncResolve           |  5.612 ns | 1.7786 ns | 0.0975 ns |  0.10 |      - |         - |        0.00 |
| Scoped_MsDiRegistration_BtdbFuncResolve           | 17.335 ns | 2.1598 ns | 0.1184 ns |  0.32 |      - |         - |        0.00 |
| Scoped_BtdbRegistration_BtdbZeroArgFuncResolve    |  5.564 ns | 0.0669 ns | 0.0037 ns |  0.10 |      - |         - |        0.00 |
| Scoped_MsDiRegistration_BtdbZeroArgFuncResolve    | 17.231 ns | 0.4999 ns | 0.0274 ns |  0.32 |      - |         - |        0.00 |
| Singleton_BtdbRegistration_BtdbResolve            | 64.614 ns | 2.0250 ns | 0.1110 ns |  1.21 | 0.0592 |     496 B |        1.22 |
| Singleton_BtdbRegistration_MsDiResolve            |  4.036 ns | 0.2309 ns | 0.0127 ns |  0.08 |      - |         - |        0.00 |
| Singleton_MsDiRegistration_BtdbResolve            | 36.450 ns | 3.2215 ns | 0.1766 ns |  0.68 | 0.0344 |     288 B |        0.71 |
| Singleton_MsDiRegistration_MsDiResolve            |  3.956 ns | 0.0691 ns | 0.0038 ns |  0.07 |      - |         - |        0.00 |
| Singleton_BtdbRegistration_BtdbFuncResolve        |  2.920 ns | 1.8620 ns | 0.1021 ns |  0.05 |      - |         - |        0.00 |
| Singleton_MsDiRegistration_BtdbFuncResolve        |  5.822 ns | 0.0631 ns | 0.0035 ns |  0.11 |      - |         - |        0.00 |
| Singleton_BtdbRegistration_BtdbZeroArgFuncResolve |  2.724 ns | 0.1621 ns | 0.0089 ns |  0.05 |      - |         - |        0.00 |
| Singleton_MsDiRegistration_BtdbZeroArgFuncResolve |  5.910 ns | 1.5577 ns | 0.0854 ns |  0.11 |      - |         - |        0.00 |
