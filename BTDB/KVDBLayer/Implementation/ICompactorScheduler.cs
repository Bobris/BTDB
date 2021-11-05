using System;
using System.Threading;

namespace BTDB.KVDBLayer;

public interface ICompactorScheduler
{
    Func<CancellationToken, bool> AddCompactAction(Func<CancellationToken, bool> compactAction);
    void RemoveCompactAction(Func<CancellationToken, bool> compactAction);
    void AdviceRunning(bool openingDb);
}
