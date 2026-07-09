using System;

namespace BTDB.SourceGenerator.Tests;

static class AgentTestEnvironment
{
    public static bool IsAgentRun =>
        IsEnabled("CODEX_CI") ||
        IsEnabled("CODEX_SHELL") ||
        IsEnabled("CI") ||
        IsEnabled("TF_BUILD") ||
        IsEnabled("GITHUB_ACTIONS");

    static bool IsEnabled(string variable)
    {
        var value = Environment.GetEnvironmentVariable(variable);
        return !string.IsNullOrWhiteSpace(value) &&
               !string.Equals(value, "0", StringComparison.OrdinalIgnoreCase) &&
               !string.Equals(value, "false", StringComparison.OrdinalIgnoreCase);
    }
}
