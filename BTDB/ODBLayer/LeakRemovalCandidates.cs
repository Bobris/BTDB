using System;
using System.IO;
using System.Threading;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

/// <summary>Bounded exact-key candidates encoded by the existing detector (VUInt32 length followed by key bytes).
/// The application transports these bytes in its ordered event, then applies them inside the event transaction on
/// every replica. It owns ordering, event cursor and commit; repeated application is harmless and no scan is repeated.</summary>
public sealed record LeakRemovalCandidates(ReadOnlyMemory<byte> EncodedKeys, ulong KeyCount)
{
    public ulong ApplyTo(IObjectDBTransaction transaction, CancellationToken cancellation = default)
    {
        var reader = MemReader.CreateFromPinnedSpan(EncodedKeys.Span);
        using var cursor = transaction.KeyValueDBTransaction.CreateCursor();
        ulong removed = 0;
        for (ulong i = 0; i < KeyCount; i++)
        {
            cancellation.ThrowIfCancellationRequested();
            var key = reader.ReadBlockAsSpan(reader.ReadVUInt32());
            if (key.IsEmpty || (key[0] != ObjectDB.AllObjectsPrefixByte && key[0] != ObjectDB.AllDictionariesPrefixByte))
                throw new InvalidDataException("Leak events may contain only object or dictionary keys.");
            if (!cursor.FindExactKey(key)) continue;
            cursor.EraseCurrent();
            removed++;
        }
        if (!reader.Eof) throw new InvalidDataException("Trailing leak event bytes.");
        return removed;
    }
}
