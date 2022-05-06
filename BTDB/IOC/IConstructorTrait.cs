using System;

namespace BTDB.IOC;

public interface IConstructorTrait
{
    void UsingConstructor(params Type[] parameterTypes);
}
