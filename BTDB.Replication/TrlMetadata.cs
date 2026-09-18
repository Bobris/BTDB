using System;
using System.Collections.Generic;
using System.Globalization;

namespace BTDB.Replication;

/// <summary>
/// M1 candidate metadata, committed atomically with unchanged native TRL bytes. Database/stream identity is scoped
/// by the namespace. A successor key is never reused; its file ID is the native identity used when restoring it.
/// </summary>
internal sealed record TrlSuccessor(string Key, uint FileId);

internal sealed record TrlMetadata(ulong Term, TrlSuccessor? Next = null)
{
    public IReadOnlyDictionary<string, string> Encode()
    {
        if (Term == 0) throw new InvalidOperationException("Term zero is not selected authority.");
        var result = new Dictionary<string, string>
        {
            ["btdb_term"] = Term.ToString(CultureInfo.InvariantCulture)
        };
        if (Next is { } next)
        {
            Validate(next);
            result.Add("btdb_next", next.Key);
            result.Add("btdb_next_id", next.FileId.ToString(CultureInfo.InvariantCulture));
        }
        return result;
    }

    public static TrlMetadata Decode(IReadOnlyDictionary<string, string> metadata)
    {
        if (!metadata.TryGetValue("btdb_term", out var termText) ||
            !ulong.TryParse(termText, NumberStyles.None, CultureInfo.InvariantCulture, out var term) || term == 0)
            throw new FormatException("Missing or invalid TRL authority metadata.");
        var hasKey = metadata.TryGetValue("btdb_next", out var key);
        var hasId = metadata.TryGetValue("btdb_next_id", out var idText);
        if (hasKey != hasId) throw new FormatException("Incomplete successor metadata.");
        if (!hasKey) return new(term);
        if (!uint.TryParse(idText, NumberStyles.None, CultureInfo.InvariantCulture, out var id))
            throw new FormatException("Invalid successor file ID.");
        var next = new TrlSuccessor(key!, id);
        Validate(next);
        return new(term, next);
    }

    static void Validate(TrlSuccessor next)
    {
        if (next.FileId == 0 || string.IsNullOrEmpty(next.Key) || next.Key.Length > 256 ||
            next.Key.StartsWith('/') || next.Key.Contains("..", StringComparison.Ordinal))
            throw new FormatException("Invalid successor identity.");
        foreach (var c in next.Key)
            if (!(char.IsAsciiLetterOrDigit(c) || c is '/' or '-' or '_' or '.'))
                throw new FormatException("Successor key must use the restricted ASCII object namespace.");
    }
}
