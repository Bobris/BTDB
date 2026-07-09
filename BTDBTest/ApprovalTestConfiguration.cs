using System;
using Assent;

namespace BTDBTest;

public static class ApprovalTestConfiguration
{
    public static bool IsAgentRun =>
        IsEnabled("CODEX_CI") ||
        IsEnabled("CODEX_SHELL") ||
        IsEnabled("CI") ||
        IsEnabled("TF_BUILD") ||
        IsEnabled("GITHUB_ACTIONS");

    public static Configuration ForAssent(Configuration? configuration = null)
    {
        configuration ??= new();
        if (!IsAgentRun)
            return configuration;

        return configuration
            .SetInteractive(false)
            .UsingReporter((received, approved) =>
            {
                Console.Error.WriteLine("Assent approval mismatch.");
                Console.Error.WriteLine("  Received: " + received);
                Console.Error.WriteLine("  Approved: " + approved);
            });
    }

    static bool IsEnabled(string variable)
    {
        var value = Environment.GetEnvironmentVariable(variable);
        return !string.IsNullOrWhiteSpace(value) &&
               !string.Equals(value, "0", StringComparison.OrdinalIgnoreCase) &&
               !string.Equals(value, "false", StringComparison.OrdinalIgnoreCase);
    }
}
