using System;
using BTDB.Collections;

namespace BTDB.IOC;

class CReg
{
    internal Func<IContainer, ICreateFactoryCtx, Func<IContainer, IResolvingCtx?, object>> Factory;
    internal Lifetime Lifetime;
    internal object? SingletonValue;
    internal uint ScopedId;
    internal StructList<CReg> Multi;
    internal CReg? DefaultRegistration;
    internal bool IsSingletonSafe;
    internal Func<IContainer, IResolvingCtx?, object>? LifetimeFactoryCache;

    public void Add(CReg registration, bool preserveExistingDefaults)
    {
        if (!preserveExistingDefaults)
        {
            Factory = registration.Factory;
            Lifetime = registration.Lifetime;
            SingletonValue = registration.SingletonValue;
            ScopedId = registration.ScopedId;
            DefaultRegistration = registration;
            IsSingletonSafe = registration.IsSingletonSafe;
        }

        Multi.Add(registration);
    }
}
