using System;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.KVDBLayer;

public interface ICompactorScheduler
{
    void AddCompactAction(IKeyValueDB keyValueDB);
    void RemoveCompactAction(IKeyValueDB keyValueDB);
    void AdviceRunning(IKeyValueDB keyValueDB, bool openingDb);
}
