using System;

namespace BTDB.IOC;

public interface IAsTrait
{
    void As(Type type);
    void Keyed(object serviceKey, Type type);
    void AsSelf();
    void AsImplementedInterfaces();
    void SetPreserveExistingDefaults();
    bool UniqueRegistration { set; }
}
