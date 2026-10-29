using BTDB.IOC;

namespace Sample3rdPartyLib;

/// <summary>
/// Simulates a base class from an external assembly whose [Dependency] members a generated derived class injects.
/// Its private members are not visible to the source generator compiling the derived class: the private setter is
/// found by reflection, the private field is not injected.
/// </summary>
public class ExternalDependenciesBase
{
    [Dependency] public I3rdPartyInterface Public { get; set; } = null!;
    [Dependency] public I3rdPartyInterface PrivateSetter { get; private set; } = null!;
    [Dependency] public I3rdPartyInterface InitOnly { get; init; } = null!;
    [Dependency] public I3rdPartyInterface ProtectedSetter { get; protected set; } = null!;
    [Dependency] protected I3rdPartyInterface ProtectedField = null!;
    [Dependency] I3rdPartyInterface _privateField = null!;

    public I3rdPartyInterface PrivateField => _privateField;
    public I3rdPartyInterface ProtectedFieldValue => ProtectedField;
}
