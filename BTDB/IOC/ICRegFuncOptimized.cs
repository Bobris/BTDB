using System;

namespace BTDB.IOC;

interface ICRegFuncOptimized
{
    object? BuildFuncOfT(ContainerImpl container, Type funcType);
}
