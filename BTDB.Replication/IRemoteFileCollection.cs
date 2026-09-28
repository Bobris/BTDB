using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>An object from one remote database inventory. Version binds every read to the listed object content.
/// FileId and FileType come from the numeric filename and its extension, without reading the native header.
/// Higher IDs order TRLs and KVIs within their respective sequences. Replication does not use native generation.
/// Sha256, when present, is the whole-file hexadecimal checksum of a sealed object.</summary>
public sealed record RemoteFile(uint FileId, KVFileType FileType, ulong Length, string Version,
    bool IsSealed, string? Sha256);

/// <summary>A remote inventory, distinct from the local cache even when numeric IDs coincide.
/// The adapter owns authority, conditional I/O and conditional creation and SHA-metadata reconciliation.</summary>
public interface IRemoteFileCollection
{
    IAsyncEnumerable<RemoteFile> EnumerateAsync(CancellationToken cancellation);

    /// <summary>Read at most buffer.Length bytes of the listed content. A newer version counts only when it provably
    /// keeps those bytes: a canonical TRL (append-only) at least as long, or an immutable PVL/KVI whose length and
    /// Sha256 match. Fail if the object disappeared or changed otherwise; never read beyond the listed length.
    /// Return zero only at end of file.</summary>
    ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation);
}
