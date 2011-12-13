using System;

namespace BTDB.IOC
{
    internal interface ICRegFuncOptimized
    {
        object BuildFuncOfT(ContainerImpl container, Type funcType);
    }
}