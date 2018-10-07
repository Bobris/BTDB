using System;
using BTDB.KVDBLayer;

namespace BTDB.ARTLib
{
    public interface IRootNode: IRootNodeInternal, IDisposable
    {
        void Reference();
        bool Dereference();
        bool ShouldBeDisposed { get; }

        ulong CommitUlong { get; set; }
        long TransactionId { get; set; }
        uint TrLogFileId { get; set; }
        uint TrLogOffset { get; set; }
        string DescriptionForLeaks { get; set; }

        IRootNode Snapshot();
        IRootNode CreateWritableTransaction();
        void Commit();
        void RevertTo(IRootNode snapshot);
        ICursor CreateCursor();
        long GetCount();
        ulong GetUlong(uint idx);
        void SetUlong(uint idx, ulong value);
        uint GetUlongCount();
        ulong[] UlongsArray { get; }
    }
}
