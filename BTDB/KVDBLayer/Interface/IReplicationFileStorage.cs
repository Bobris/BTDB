namespace BTDB.KVDBLayer;

/// Node-local backing store of a replicated database. Files keep the identity chosen by replication: exact imports
/// of remote IDs and parity-constrained allocation (odd TRL, even non-TRL). IDs are never reused while files exist.
/// It stores bytes only; the replicated collection owns remote inventory, mappings and cache validation.
public interface IReplicationFileStorage : IFileCollection
{
    IFileCollectionFile AddFile(string humanHint, FileIdParity parity);

    /// Create an empty file with exactly this ID; throws when the ID already exists.
    IFileCollectionFile ImportFile(uint fileId, string humanHint);

    /// Type implied by the file's hint (extension), without reading its header; null when unknown.
    KVFileType? GetFileType(uint fileId);
}
