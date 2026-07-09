using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using VerifyTests;
using VerifyXunit;

namespace BTDBTest;

public static class VerifyApprovalExtensions
{
    public static Task VerifyApproval(this object testFixture, string text,
        [CallerMemberName] string? testName = null, [CallerFilePath] string? filePath = null)
    {
        var settings = new VerifySettings();
        settings.UseTypeName(testFixture.GetType().Name);
        settings.UseMethodName(testName!);
        settings.UseDirectory(Path.GetDirectoryName(filePath)!);
        return Verifier.Verify(text, settings);
    }
}
