using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using VerifyTests;
using VerifyXunit;

namespace BTDBTest;

public static class VerifyApprovalExtensions
{
    /// typeName overrides the fixture's runtime type name, so subclasses running inherited tests share the snapshot.
    public static Task VerifyApproval(this object testFixture, string text,
        [CallerMemberName] string? testName = null, [CallerFilePath] string? filePath = null, string? typeName = null)
    {
        var settings = new VerifySettings();
        settings.UseTypeName(typeName ?? testFixture.GetType().Name);
        settings.UseMethodName(testName!);
        settings.UseDirectory(Path.GetDirectoryName(filePath)!);
        return Verifier.Verify(text, settings);
    }
}
