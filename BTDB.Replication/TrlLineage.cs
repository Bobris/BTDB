using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>Native TRL continuation. Existing chains and the one-time legacy conversion may contain gaps,
/// so readers follow PreviousFileId headers back from a known end even though new replication successors use +2.</summary>
internal static class TrlLineage
{
    /// <summary>The ascending successors of current through end, found with one walk back over the headers.</summary>
    public static async ValueTask<uint[]> SuccessorsAsync(uint current, uint end, Func<uint, ValueTask<uint>> previousOf)
    {
        if (end <= current) throw new InvalidDataException("The advertised TRL cut does not follow the current file.");
        var successors = new List<uint> { end };
        var id = end;
        while (true)
        {
            var previous = await previousOf(id).ConfigureAwait(false);
            if (previous == current) break;
            if (previous <= current || previous >= id)
                throw new InvalidDataException("TRL lineage does not connect to the advertised cut.");
            successors.Add(previous);
            id = previous;
        }
        successors.Reverse();
        return successors.ToArray();
    }

    public static uint LocalPrevious(Func<uint, IFileCollectionFile?> getFile, uint id)
    {
        var file = getFile(id) ?? throw new FileNotFoundException("Missing retained local TRL.", id.ToString());
        return FileCollectionWithFileInfos.ReadFileInfo(file) is IFileTransactionLog log
            ? log.PreviousFileId
            : throw new InvalidDataException("Local TRL lineage references a non-TRL file.");
    }
}
