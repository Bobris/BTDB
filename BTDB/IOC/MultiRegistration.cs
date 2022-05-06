using System;
using System.Reflection;
using BTDB.IL;

namespace BTDB.IOC;

class MultiRegistration : RegistrationBaseImpl<IAsLiveScopeConstructorPropertiesScanTrait>, IContanerRegistration
{
    readonly AsTraitImpl _asTrait = new AsTraitImpl();
    readonly LiveScopeTraitImpl _liveScopeTrait = new LiveScopeTraitImpl();
    readonly ScanTraitImpl _scanTrait = new ScanTraitImpl();
    readonly ConstructorTraitImpl _constructorTrait = new ConstructorTraitImpl();
    readonly PropertiesTraitImpl _propertiesTrait = new PropertiesTraitImpl();
    readonly Assembly[] _froms;

    public MultiRegistration(Assembly[] froms)
    {
        _froms = froms;
    }

    public void Register(ContainerRegistrationContext context)
    {
        foreach (var assembly in _froms)
        {
            foreach (var type in assembly.GetTypes())
            {
                if (!type.IsClass) continue;
                if (type.IsAbstract) continue;
                if (type.IsGenericTypeDefinition) continue;
                if (type.IsDelegate()) continue;
                if (!_scanTrait.MatchFilter(type)) continue;
                if (_constructorTrait.ChooseConstructor(type, _constructorTrait.ReturnPossibleConstructors(type)) == null)
                    continue;
                ((IContanerRegistration)new SingleRegistration(type, _asTrait, _liveScopeTrait, _constructorTrait, _propertiesTrait)).Register(context);
            }
        }
    }

    public override object InternalTraits(Type trait)
    {
        if (trait == typeof(IAsTrait)) return _asTrait;
        if (trait == typeof(ILiveScopeTrait)) return _liveScopeTrait;
        if (trait == typeof(IConstructorTrait)) return _constructorTrait;
        if (trait == typeof(IPropertiesTrait)) return _propertiesTrait;
        if (trait == typeof(IScanTrait)) return _scanTrait;
        throw new ArgumentOutOfRangeException();
    }
}
