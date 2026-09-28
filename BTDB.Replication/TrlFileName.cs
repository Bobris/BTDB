using System;
using System.Globalization;

namespace BTDB.Replication;

/// <summary>
/// A named canonical TRL identity, including a genesis or checkpoint recovery root. FileId must match
/// the key's {fileId}.trl filename; all terms share {fileId}.trl.
/// </summary>
public sealed record TrlSuccessor(string Key, uint FileId);

public static class TrlFileName
{
    public static string Key(uint fileId) => fileId != 0
        ? $"{fileId.ToString(CultureInfo.InvariantCulture)}.trl"
        : throw new ArgumentOutOfRangeException(nameof(fileId));

    internal static void Validate(TrlSuccessor next)
    {
        if (next.FileId != FileIdFromKey(next.Key)) throw new FormatException("TRL file ID does not match its key.");
    }

    /// <summary>TRL object names are an unpadded positive decimal native ID followed by .trl.
    /// Keys are relative to the database prefix, with no subdirectory.</summary>
    public static uint FileIdFromKey(string key)
    {
        ValidateKey(key);
        var name = key.AsSpan();
        if (!name.EndsWith(".trl", StringComparison.Ordinal) || name.Length <= 4 || name[0] == '0' ||
            !uint.TryParse(name[..^4], NumberStyles.None, CultureInfo.InvariantCulture, out var id))
            throw new FormatException("TRL keys must end in {fileId}.trl with an unpadded positive decimal ID.");
        return id;
    }

    internal static void ValidateKey(string key)
    {
        if (string.IsNullOrEmpty(key) || key.Length > 256 ||
            key.StartsWith('/') || key.Contains("..", StringComparison.Ordinal))
            throw new FormatException("Invalid object key.");
        foreach (var c in key)
            if (!(char.IsAsciiLetterOrDigit(c) || c is '/' or '-' or '_' or '.'))
                throw new FormatException("Object keys must use the restricted ASCII object namespace.");
    }
}
