using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>An object from one remote database inventory. Version binds every read to the listed object version.
/// FileId and FileType come from the numeric filename and its extension, without reading the native header.
/// Higher IDs order TRLs and KVIs within their respective sequences. Replication does not use native generation.
/// Sha256, when present, is the whole-file hexadecimal checksum of a sealed object.</summary>
internal sealed record RemoteFile(uint FileId, KVFileType FileType, ulong Length, string Version,
    bool IsSealed, string? Sha256);

/// <summary>A remote inventory, distinct from the local cache even when numeric IDs coincide.
/// The adapter owns authority, conditional I/O and conditional creation and SHA-metadata reconciliation.</summary>
internal interface IRemoteFileCollection
{
    IAsyncEnumerable<RemoteFile> EnumerateAsync(CancellationToken cancellation);

    /// <summary>Read at most buffer.Length bytes from exactly file.Version. Fail if it changed or disappeared;
    /// never silently read a newer object. Return zero only at end of file.</summary>
    ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation);
}
