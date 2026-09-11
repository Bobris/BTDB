```

BenchmarkDotNet v0.14.0, macOS 26.6.2 (25G83) [Darwin 25.6.0]
Apple M2 Max, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.401
  [Host]   : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD
  ShortRun : .NET 10.0.12 (10.0.1226.42308), Arm64 RyuJIT AdvSIMD

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
| Method       | Signed | WideValues | Mean     | Error     | StdDev    | Ratio | Allocated | Alloc Ratio |
|------------- |------- |----------- |---------:|----------:|----------:|------:|----------:|------------:|
| **DecodeEncode** | **False**  | **False**      | **7.470 ns** | **0.2206 ns** | **0.0121 ns** |  **1.00** |         **-** |          **NA** |
| Copy         | False  | False      | 2.247 ns | 0.1852 ns | 0.0101 ns |  0.30 |         - |          NA |
|              |        |            |          |           |           |       |           |             |
| **DecodeEncode** | **False**  | **True**       | **7.444 ns** | **0.5043 ns** | **0.0276 ns** |  **1.00** |         **-** |          **NA** |
| Copy         | False  | True       | 4.220 ns | 0.3186 ns | 0.0175 ns |  0.57 |         - |          NA |
|              |        |            |          |           |           |       |           |             |
| **DecodeEncode** | **True**   | **False**      | **8.179 ns** | **0.5298 ns** | **0.0290 ns** |  **1.00** |         **-** |          **NA** |
| Copy         | True   | False      | 2.804 ns | 0.2226 ns | 0.0122 ns |  0.34 |         - |          NA |
|              |        |            |          |           |           |       |           |             |
| **DecodeEncode** | **True**   | **True**       | **7.964 ns** | **0.1676 ns** | **0.0092 ns** |  **1.00** |         **-** |          **NA** |
| Copy         | True   | True       | 5.013 ns | 0.4302 ns | 0.0236 ns |  0.63 |         - |          NA |
