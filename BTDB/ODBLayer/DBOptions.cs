namespace BTDB.ODBLayer
{
    public class DBOptions
    {
        public DBOptions()
        {
            AutoRegisterType = true;
        }

        public DBOptions WithoutAutoRegistration()
        {
            AutoRegisterType = false;
            return this;
        }

        public DBOptions WithCustomType2NameRegistry(IType2NameRegistry registry)
        {
            CustomType2NameRegistry = registry;
            return this;
        }

        public bool AutoRegisterType { get; private set; }
        public IType2NameRegistry CustomType2NameRegistry { get; private set; }
    }
}