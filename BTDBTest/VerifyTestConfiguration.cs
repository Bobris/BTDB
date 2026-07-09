using System;
using System.Runtime.CompilerServices;
using DiffEngine;

namespace BTDBTest;

static class VerifyTestConfiguration
{
    [ModuleInitializer]
    internal static void Init()
    {
        if (IsAgentRun)
            DiffRunner.Disabled = true;
    }

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
