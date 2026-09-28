using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

/// A sequential read of a remote file that runs while the file is prefetched.
public interface IStreamingFileRead : IDisposable
{
    /// Forward reader of the selected version's bytes; it blocks until a download has written the bytes it returns.
    IMemReader Reader { get; }

    ulong Length { get; }

    /// Finish the prefetch after reading. True when the bytes read are the verified selected version; false when a
    /// cached copy failed its whole-file checksum and was discarded, so the file must be read again after
    /// PrefetchAsync. A failed download throws.
    ValueTask<bool> CompleteAsync(CancellationToken cancellation);
}

/// Separate remote inventory and local cache/storage, with asynchronous metadata and prefetch.
/// Implementing this interface selects replication database semantics, including odd TRL and even non-TRL allocation.
/// The caller must await InitializeAsync before OpenAsync, remote inventory access, metadata access or prefetch.
/// Collections requiring discovery must throw InvalidOperationException on premature access, never appear empty.
public interface IFileReplicatedCollection : IFileCollection
{
    /// Read-only remote file handle, or null when absent. Lookup does not populate the local cache.
    IFileCollectionFile? GetRemoteFile(uint index);

    /// Remote inventory discovered by InitializeAsync. Enumeration does not download file bodies.
    IEnumerable<IFileCollectionFile> RemoteEnumerate();

    /// Translate a remote inventory ID to its session-local ID, including files not downloaded yet.
    /// The assignment remains stable for this session; implementations may use identity mappings.
    uint GetLocalFileId(uint remoteFileId);

    /// Allocate a fresh identity from an independent sequence for the requested parity, without creating intermediate files.
    /// Any allocates above both sequences.
    IFileCollectionFile AddFile(string humanHint, FileIdParity parity);

    /// Create a new TRL at the exact odd ID selected from its native predecessor by the database.
    /// A local or remote collision must fail without overwriting or selecting a different ID.
    IFileCollectionFile CreateTransactionLogFile(uint fileId);

    /// During startup, conditionally publish a newly created legacy-transition TRL containing only its native header.
    /// Compare an existing remote header; never overwrite it. A changed or extended remote history requires fresh restore.
    /// This bootstrap operation needs no leader authority and must finish before application writes can start.
    ValueTask PublishTransactionLogHeaderAsync(IFileCollectionFile file, CancellationToken cancellation) =>
        throw new System.NotSupportedException("This collection cannot publish a legacy transition header.");

    /// During startup only, discard a local TRL belonging exclusively to an unfinished transaction.
    /// Its remote bytes remain published and must be compared before later publication under the same ID.
    void DiscardUncommittedTransactionLog(uint fileId) => GetFile(fileId)?.Remove();

    /// Remove the local copy of a file the opened database does not use (for example a superseded KVI), including a
    /// cached copy that was never verified and is therefore not visible through GetFile.
    void DiscardLocalFile(uint fileId) => GetFile(fileId)?.Remove();

    /// Discover the inventory without downloading file bodies. Called and awaited by the owner before OpenAsync.
    /// After discovery, remove unmapped local files and cache candidates whose extension, length or remote metadata rule
    /// them out. Whole-file SHA validation may be deferred to the first prefetch; GetFile must not expose a candidate
    /// before it is verified.
    /// Repeated initialization must preserve files created in the initialized session.
    /// OpenAsync and PrefetchAsync never invoke initialization. Already-ready collections must implement initialization explicitly.
    ValueTask InitializeAsync(CancellationToken cancellation = default);

    /// Validate/download the selected remote file into local storage before GetFile is used.
    /// An unselected local file must never substitute for a missing remote file.
    ValueTask PrefetchAsync(uint fileId, CancellationToken cancellation = default);

    /// Start prefetching a remote file and read it sequentially meanwhile, so a KVI load overlaps its own download or
    /// checksum. Null when the file is already verified or being prefetched, or the collection cannot stream; use
    /// PrefetchAsync and GetFile then.
    IStreamingFileRead? StartStreamingRead(uint fileId, CancellationToken cancellation) => null;

    /// Selected remote type, or local type for a newly created file, without opening the file.
    /// Null allows legacy/custom collections to fall back to inspecting the native header.
    KVFileType? GetFileType(uint fileId);

    /// Read only the native header metadata, without fetching the whole remote file.
    ValueTask<IFileInfo> ReadFileInfoAsync(uint fileId, CancellationToken cancellation = default);
}
