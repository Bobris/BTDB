using System;

namespace BTDB.ARTLib
{
    public interface IRootNode: IDisposable
    {
        ulong CommitUlong { get; set; }
        long TransactionId { get; set; }
        string DescriptionForLeaks { get; set; }

        IRootNode Snapshot();
        void RevertTo(IRootNode snapshot);
        ICursor CreateCursor();
        long GetCount();
        ulong GetUlong(uint idx);
        void SetUlong(uint idx, ulong value);
        uint GetUlongCount();
    }
}
