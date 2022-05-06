using System;

namespace BTDB.IOC;

public interface IScanTrait
{
    void Where(Predicate<Type> filter);
}
