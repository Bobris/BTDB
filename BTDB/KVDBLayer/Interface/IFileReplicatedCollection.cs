using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.KVDBLayer;

/// Separate remote inventory and local cache/storage, with asynchronous metadata and prefetch.
/// Implementing this interface selects replication database semantics, including odd TRL and even non-TRL allocation.
/// The caller must await InitializeAsync before OpenAsync, remote inventory access, metadata access or prefetch.
/// Collections requiring discovery must throw InvalidOperationException on premature access, never appear empty.
public interface IFileReplicatedCollection : IFileCollection
{
    /// Number of files in the remote inventory discovered by InitializeAsync, independent of local cache contents.
    uint GetRemoteCount();

    /// Read-only remote file handle, or null when absent. Lookup does not populate the local cache.
    IFileCollectionFile? GetRemoteFile(uint index);

    /// Remote inventory discovered by InitializeAsync. Enumeration does not download file bodies.
    IEnumerable<IFileCollectionFile> RemoteEnumerate();

    /// Translate a remote inventory ID to its session-local ID, including files not downloaded yet.
    /// The assignment remains stable for this session; implementations may use identity mappings.
    uint GetLocalFileId(uint remoteFileId);

    /// Leader-only publication of a complete sealed local PVL pinned by the caller's snapshot.
    /// Return its confirmed remote ID, reusing an existing verified publication or allocating/uploading a new one.
    /// Remember the session mapping only after upload confirmation; retry an uncertain upload with the same reserved ID.
    /// The owner supplies a fenced remote adapter and serializes publication calls. Local compaction itself never calls this.
    ValueTask<uint> PublishPureValuesAsync(KeyIndexFileSource source, CancellationToken cancellation = default);

    /// Allocate a fresh identity from an independent sequence for the requested parity, without creating intermediate files.
    /// Any allocates above both sequences.
    IFileCollectionFile AddFile(string humanHint, FileIdParity parity);

    /// Discover the inventory without downloading file bodies. Called and awaited by the owner before OpenAsync.
    /// After discovery, remove unmapped local files. For mapped cache candidates, compare extension first, then length, then SHA metadata.
    /// Remove invalid/unverifiable candidates so prefetch can download them; remember validated files for reuse.
    /// Repeated initialization must preserve files created in the initialized session.
    /// OpenAsync and PrefetchAsync never invoke initialization. Already-ready collections must implement initialization explicitly.
    ValueTask InitializeAsync(CancellationToken cancellation = default);

    /// Refresh remote membership and versions before admitting writes after a leader transition.
    /// Requires initialization. The owner must quiesce file operations and discard old remote handles first.
    /// Publish the complete listing atomically; failed/canceled discovery leaves the previous listing visible.
    /// Preserve session mappings and local files, including unpublished compaction output. No startup cleanup.
    /// This refresh does not recover database state or establish leadership; the owner must do both separately.
    ValueTask RefreshRemoteInventoryAsync(CancellationToken cancellation = default);

    /// Validate/download the selected remote file into local storage before GetFile is used.
    /// An unselected local file must never substitute for a missing remote file.
    ValueTask PrefetchAsync(uint fileId, CancellationToken cancellation = default);

    /// Selected remote type, or local type for a newly created file, without opening the file.
    /// Null allows legacy/custom collections to fall back to inspecting the native header.
    KVFileType? GetFileType(uint fileId);

    /// Read only the native header metadata, without fetching the whole remote file.
    ValueTask<IFileInfo> ReadFileInfoAsync(uint fileId, CancellationToken cancellation = default);
}
