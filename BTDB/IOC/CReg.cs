using System;
using BTDB.Collections;

namespace BTDB.IOC;

class CReg
{
    internal Func<IContainer, ICreateFactoryCtx, Func<IContainer, IResolvingCtx?, object>> Factory;
    internal Lifetime Lifetime;
    internal uint SingletonId;
    internal uint ScopedId;
    internal StructList<CReg> Multi;
    internal Func<IContainer, IResolvingCtx?, object>? SingletonFactoryCache;
    internal Func<IContainer, IResolvingCtx?, object>? ScopedFactoryCache;

    public void Add(CReg registration, bool preserveExistingDefaults)
    {
        if (!preserveExistingDefaults)
        {
            Factory = registration.Factory;
            Lifetime = registration.Lifetime;
            SingletonId = registration.SingletonId;
            ScopedId = registration.ScopedId;
        }

        Multi.Add(registration);
    }
}
