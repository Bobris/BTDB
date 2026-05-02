# Roaring Bitmaps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add low-level static 16-bit Roaring-inspired bitmap operations over `Span<byte>` and `ReadOnlySpan<byte>`.

**Architecture:** `BTDB.Buffer.RoaringBitmaps` exposes static methods only. Inputs are encoded by length: `0` empty, `8192` raw bitmap, even length sorted little-endian `ushort` offsets, odd length RLE pairs `(start, lengthMinusOne)` padded by one zero byte.

**Tech Stack:** C# `net10.0`, xUnit, BenchmarkDotNet in `SimpleTester`, `System.Numerics.BitOperations`, `System.Runtime.Intrinsics`.

---

### Task 1: Behavior Tests

**Files:**
- Create: `BTDBTest/RoaringBitmapsTest.cs`

- [ ] Write failing tests for bit access, compression to array/RLE/bitmap, boolean operations, negation, malformed input, and randomized equivalence against raw 8192-byte bitmaps.
- [ ] Run `dotnet test BTDBTest/BTDBTest.csproj --filter RoaringBitmapsTest` and confirm the tests fail because `RoaringBitmaps` does not exist.

### Task 2: Static API

**Files:**
- Create: `BTDB/Buffer/RoaringBitmaps.cs`

- [ ] Implement static constants and validation helpers.
- [ ] Implement raw bitmap `SetBit`, `ClearBit`, `GetBit`.
- [ ] Implement `Compress(ReadOnlySpan<byte> bitmap, Span<byte> output)` choosing empty, sorted-array, RLE, or raw bitmap by encoded size.
- [ ] Implement `And`, `Or`, `AndNot`, and `Not` with representation-specific paths and fallback temporary bitmap only where justified.
- [ ] Run `dotnet test BTDBTest/BTDBTest.csproj --filter RoaringBitmapsTest`.

### Task 3: Benchmarks and Changelog

**Files:**
- Create: `SimpleTester/RoaringBitmapsBenchmark.cs`
- Modify: `SimpleTester/Program.cs`
- Modify: `CHANGELOG.md`

- [ ] Add BenchmarkDotNet scenarios for sparse array, RLE, dense bitmap, and mixed operations.
- [ ] Add an unreleased changelog entry for the new low-level Roaring-inspired bitmap operations.
- [ ] Run `dotnet test BTDBTest/BTDBTest.csproj --filter RoaringBitmapsTest`.
- [ ] Run `dotnet build BTDB.sln`.
