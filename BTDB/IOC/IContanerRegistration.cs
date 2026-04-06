namespace BTDB.IOC;

interface IContanerRegistration
{
    void Register(ContainerRegistrationContext context);
    void RegisterForServiceCollection(ServiceCollectionRegistrationContext context);
}
