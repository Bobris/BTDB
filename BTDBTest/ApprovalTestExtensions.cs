using System.Runtime.CompilerServices;
using Assent;

namespace BTDBTest;

public static class ApprovalTestExtensions
{
    public static void Assent(this object testFixture, string text, Configuration? configuration = null,
        [CallerMemberName] string? testName = null, [CallerFilePath] string? filePath = null)
    {
        global::Assent.Extensions.Assent(testFixture, text, ApprovalTestConfiguration.ForAssent(configuration),
            testName, filePath);
    }
}
