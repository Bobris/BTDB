using System;
using Microsoft.Win32.SafeHandles;

namespace BTDB.StreamLayer;

public interface IPlatformMethods
{
    /// returns original name of any existing file or directory (follows symlinks), if anything fails it returns null. On windows it also fixes casing
    string? RealPath(string path);
}
