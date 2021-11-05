using System.IO;

namespace BTDB.IL;

public static class DynamicILDirectoryPath
{
    static DynamicILDirectoryPath()
    {
        DynamicIL = Path.Combine(Path.GetTempPath(), "dynamicIL");
    }

    public static string DynamicIL { get; set; }
}
