using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.Replication;

/// <summary>Inspects native transaction terminators before comparison, including while local execution is behind.
/// Keeps only a complete scan position. Payloads are skipped in bounded buffers, never decoded or applied.</summary>
internal sealed class SchemaTrlScanner(TransactionLogPosition start, Guid? databaseIdentity)
{
    TransactionLogPosition _position = start;
    byte[]? _buffer;

    public async ValueTask<bool> ContainsSchemaAsync(ILeaderTrlReader leader, TransactionLogPosition end, CancellationToken cancellation)
    {
        if (Order(end) <= Order(_position)) return false;
        var fileId = _position.FileId;
        var offset = (ulong)_position.Offset;
        var buffer = _buffer ??= GC.AllocateUninitializedArray<byte>(256 * 1024, pinned: true);
        var transaction = false;
        uint previousId = 0;
        while (true)
        {
            var available = 0;
            var consumed = 0;
            var limit = fileId == end.FileId ? (ulong)end.Offset : uint.MaxValue;
            async ValueTask Fill(int maximumRead = 256 * 1024)
            {
                offset += (uint)consumed;
                available -= consumed;
                buffer.AsSpan(consumed, available).CopyTo(buffer);
                consumed = 0;
                while (available < 64 && offset + (uint)available < limit)
                {
                    var count = (int)Math.Min((ulong)Math.Min(buffer.Length - available, maximumRead), limit - offset - (uint)available);
                    var read = await leader.ReadAsync(fileId, offset + (uint)available, buffer.AsMemory(available, count), cancellation).ConfigureAwait(false);
                    cancellation.ThrowIfCancellationRequested();
                    if (read < 0 || read > count) throw new IOException("Invalid native range length.");
                    if (read == 0) break;
                    available += read;
                }
            }
            var resumeOffset = offset;
            offset = 0;
            await Fill(64).ConfigureAwait(false);
            var headerReader = MemReader.CreateFromPinnedArray(buffer, 0, available);
            if (FileCollectionWithFileInfos.ReadFileInfo(ref headerReader, true) is not IFileTransactionLog header ||
                header.Guid != databaseIdentity || (previousId != 0 && header.PreviousFileId != previousId))
                throw new InvalidDataException("Peer TRL lineage differs from the restored database.");
            if (resumeOffset == 0) consumed = (int)headerReader.GetCurrentPosition();
            else { offset = resumeOffset; available = 0; }
            var ended = false;
            var afterTemporaryEnd = false;
            while (!ended && offset + (uint)consumed < limit)
            {
                await Fill().ConfigureAwait(false);
                if (available == 0)
                {
                    if (fileId != end.FileId) break; // Native reopen can rotate at physical EOF.
                    throw new IOException("Truncated native transaction range.");
                }
                var reader = MemReader.CreateFromPinnedArray(buffer, 0, available);
                var command = (KVCommandType)reader.ReadUInt8() & KVCommandType.CommandMask;
                if (command == 0 && afterTemporaryEnd) { ended = true; break; }
                afterTemporaryEnd = false;
                ulong skip = 0;
                var schema = false;
                switch (command)
                {
                    case KVCommandType.TransactionStart:
                        if (transaction || reader.ReadUInt8() != (byte)'t' || reader.ReadUInt8() != (byte)'R')
                            throw new InvalidDataException("Invalid native transaction start.");
                        transaction = true;
                        break;
                    case KVCommandType.Commit:
                    case KVCommandType.CommitWithDeltaUlong:
                        if (!transaction) throw new InvalidDataException("Native commit without transaction.");
                        schema = command == KVCommandType.Commit || reader.ReadVUInt64() == 0;
                        transaction = false;
                        break;
                    case KVCommandType.Rollback:
                        if (!transaction) throw new InvalidDataException("Native rollback without transaction.");
                        transaction = false;
                        break;
                    case KVCommandType.EndOfFile:
                        ended = true;
                        break;
                    case KVCommandType.TemporaryEndOfFile:
                        afterTemporaryEnd = true;
                        break;
                    case KVCommandType.CreateOrUpdate:
                    case KVCommandType.CreateOrUpdateDeprecated:
                    case KVCommandType.EraseRange:
                        skip = checked((ulong)reader.ReadVInt32() + (ulong)reader.ReadVInt32());
                        break;
                    case KVCommandType.EraseOne:
                        skip = checked((ulong)reader.ReadVInt32());
                        break;
                    case KVCommandType.UpdateKeySuffix:
                        reader.ReadVUInt32();
                        skip = reader.ReadVUInt32();
                        break;
                    case KVCommandType.DeltaUlongs:
                        reader.ReadVUInt32();
                        reader.ReadVUInt64();
                        break;
                    default: throw new InvalidDataException("Unknown native transaction command.");
                }
                if (command is KVCommandType.CreateOrUpdate or KVCommandType.CreateOrUpdateDeprecated or
                    KVCommandType.EraseOne or KVCommandType.EraseRange or KVCommandType.UpdateKeySuffix or KVCommandType.DeltaUlongs)
                    if (!transaction) throw new InvalidDataException("Native mutation outside a transaction.");
                consumed = (int)reader.GetCurrentPosition();
                if (schema)
                {
                    cancellation.ThrowIfCancellationRequested();
                    return true;
                }
                while (skip != 0)
                {
                    var count = (int)Math.Min(skip, (ulong)(available - consumed));
                    consumed += count;
                    skip -= (uint)count;
                    if (skip == 0) break;
                    await Fill().ConfigureAwait(false);
                    if (available == 0) throw new IOException("Truncated native payload.");
                }
            }
            if (fileId == end.FileId)
            {
                if (transaction || ended || offset + (uint)consumed != limit)
                    throw new InvalidDataException("Advertised native cut is not a complete transaction boundary.");
                _position = end;
                return false;
            }
            previousId = fileId;
            fileId = checked(fileId + ((fileId & 1) == 0 ? 1u : 2u));
            if (fileId > end.FileId) throw new InvalidDataException("Invalid native TRL sequence.");
            offset = 0;
        }
    }

    static ulong Order(TransactionLogPosition position) => ((ulong)position.FileId << 32) | position.Offset;
}
