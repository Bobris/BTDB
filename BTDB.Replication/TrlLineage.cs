using System;
using System.IO;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>Native TRL continuation. Odd-ID allocation may skip IDs reserved by legacy or abandoned files,
/// so a successor is never inferred from numeric parity; follow PreviousFileId headers back from a known end.</summary>
internal static class TrlLineage
{
    public static async ValueTask<uint> NextAsync(uint current, uint end, Func<uint, ValueTask<uint>> previousOf)
    {
        if (end <= current) throw new InvalidDataException("The advertised TRL cut does not follow the current file.");
        var id = end;
        while (true)
        {
            var previous = await previousOf(id).ConfigureAwait(false);
            if (previous == current) return id;
            if (previous <= current || previous >= id)
                throw new InvalidDataException("TRL lineage does not connect to the advertised cut.");
            id = previous;
        }
    }

    public static uint LocalPrevious(Func<uint, IFileCollectionFile?> getFile, uint id)
    {
        var file = getFile(id) ?? throw new FileNotFoundException("Missing retained local TRL.", id.ToString());
        return FileCollectionWithFileInfos.ReadFileInfo(file) is IFileTransactionLog log
            ? log.PreviousFileId
            : throw new InvalidDataException("Local TRL lineage references a non-TRL file.");
    }
}
