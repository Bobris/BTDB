using System;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.KVDBLayer;

public interface ICompactorScheduler
{
    Func<CancellationToken, ValueTask<bool>> AddCompactAction(Func<CancellationToken, ValueTask<bool>> compactAction);
    void RemoveCompactAction(Func<CancellationToken, ValueTask<bool>> compactAction);
    void AdviceRunning(bool openingDb);
}
