namespace BTDB.IOC;

class PropertiesTraitImpl : IPropertiesTrait, IPropertiesTraitImpl
{
    public void PropertiesAutowired()
    {
        ArePropertiesAutowired = true;
    }

    public bool ArePropertiesAutowired { get; private set; }
}
