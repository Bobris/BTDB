# Repository Guidelines

These guidelines summarize how to navigate, build, and contribute to this repository.

## Project Structure & Module Organization

- `BTDB/` is the core library (KeyValueDB, ObjectDB, IL helpers, etc.).
- `BTDB.SourceGenerator/` contains the incremental source generator for IOC container support.
- `BTDBTest/` and `BTDB.SourceGenerator.Test/` hold xUnit test suites.
- `Doc/` contains feature documentation and design notes (e.g., relations).
- `SimpleTester/`, `BTDB.SourceGenerator.Sample/`, `Sample3rdPartyLib/`, and `ODbDump/` are samples/tools.
- `DBBenchmark/` is for benchmarks; `Releaser/` is for release automation.
- `artifacts/` is the standard build output location.

## Build, Test, and Development Commands

- `dotnet build BTDB.sln` builds the full solution.
- `dotnet test BTDB.sln` runs all tests.
- `dotnet test BTDBTest/BTDBTest.csproj` runs the main test suite only.
- `dotnet run --project SimpleTester/SimpleTester.csproj` runs the local tester app.

## Environment Requirements

- Always run `dotnet` commands with network access enabled so package restore can complete.

## Coding Style & Naming Conventions

- Indentation: 4 spaces; line endings: LF; always trim trailing whitespace.
- C# style follows `.editorconfig`: prefer `var` when the type is apparent, and use file-scoped namespaces.
- Naming: PascalCase for public types/members and type parameters (e.g., `TItem`).
- Target framework is .NET 9 with C# 12 language features.

## Testing Guidelines

- Tests use xUnit; look for `*Test.cs`/`*Tests.cs` under `BTDBTest/` and `BTDB.SourceGenerator.Test/`.
- Prioritize SourceGenerator work: ensure `BTDB.SourceGenerator.Test/BTDB.SourceGenerator.Tests.csproj` passes before other suites.
- Add tests for behavior changes and bug fixes; favor focused `Fact` tests and parameterized `Theory` tests.
- Run `dotnet test BTDB.sln` before opening a PR that changes core behavior.

## Commit & Pull Request Guidelines

- Commit messages are concise, imperative, sentence case (e.g., "Enhance validation for ...").
- Keep commits scoped to one logical change when possible.
- PRs should include: a brief description, test commands run, and linked issues/notes on breaking changes.

## Documentation Updates

- Update relevant docs in `Doc/` when changing public behavior or APIs.
- Keep README changes high-level; place deep dives in `Doc/`.
