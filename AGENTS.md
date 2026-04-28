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
- `dotnet test BTDB.SourceGenerator.Test/BTDB.SourceGenerator.Tests.csproj` runs the SourceGenerator test suite only.
- `dotnet test BTDBTest/BTDBTest.csproj` runs the main test suite only.
- `dotnet run --project SimpleTester/SimpleTester.csproj` runs the local tester app.

## Environment Requirements

- Always run `dotnet` commands with network access enabled so package restore and analyzer feeds can complete; request sandbox escalation before invoking `dotnet build`/`dotnet test` if the default environment denies network access.
- `nuget.config` restores from NuGet plus the `terrafx` feed, so offline restore/build/test runs are incomplete.
- Example: `dotnet test BTDB.SourceGenerator.Test/BTDB.SourceGenerator.Tests.csproj` must be run with the `sandbox_permissions` flag set to `require_escalated` so NuGet restore can reach the feed.

## Coding Style & Naming Conventions

- Indentation: 4 spaces; line endings: LF; always trim trailing whitespace.
- C# style follows `.editorconfig`: prefer `var` when the type is apparent, and use file-scoped namespaces.
- Do not use `#region`/`#endregion` in new or edited code.
- When calling methods with `params ReadOnlySpan<string>`, pass string arguments directly instead of array/collection expressions.
- Suppress `CS8620` at file scope when using `params ReadOnlySpan<string>` with direct string arguments.
- Prefer `resultType is null` over `string.IsNullOrWhiteSpace(resultType)` when checking nullable type strings.
- Naming: PascalCase for public types/members and type parameters (e.g., `TItem`).
- Main library and test projects target .NET 10; `BTDB.SourceGenerator/` targets `netstandard2.0` as a Roslyn component.

## Testing Guidelines

- Tests use xUnit; look for `*Test.cs`/`*Tests.cs` under `BTDBTest/` and `BTDB.SourceGenerator.Test/`.
- `BTDB.SourceGenerator.Test/` uses Verify snapshot files (`*.verified.cs`, `*.verified.txt`); add or update the matching baselines when generated output or diagnostics intentionally change.
- New SourceGenerator tests should go through `GeneratorTestsBase.VerifySourceGenerator(...)`, which verifies compilation succeeds and that incremental outputs are cached on rerun.
- `BTDBTest/` uses Assent approval files (`*.approved.txt`) for text baselines.
- Prioritize SourceGenerator work: ensure `BTDB.SourceGenerator.Test/BTDB.SourceGenerator.Tests.csproj` passes before other suites.
- Add tests for behavior changes and bug fixes; favor focused `Fact` tests and parameterized `Theory` tests.
- Run `dotnet test BTDB.sln` before opening a PR that changes core behavior.
- Run relevant test suites automatically after changes; do not ask for confirmation before running them.

## Before Finishing a Code Change

- Check whether the change is a bug fix, new feature, behavior change, performance improvement, or public/internal API
  change. If yes, update `CHANGELOG.md` in the same turn before reporting the work complete.
- Mention the changelog update in the final response, or explicitly state why no changelog entry was needed.
- Run the relevant build/test command after the final file edits, including documentation and changelog edits.

## Commit & Pull Request Guidelines

- Commit messages are concise, imperative, sentence case (e.g., "Enhance validation for ...").
- Keep commits scoped to one logical change when possible.
- PRs should include: a brief description, test commands run, and linked issues/notes on breaking changes.

## Documentation Updates

- Update `CHANGELOG.md` for every bug fix or new feature.
- Update relevant docs in `Doc/` when changing public behavior or APIs, or when it otherwise makes sense.
- Keep README changes high-level; place deep dives in `Doc/`.
