using System;

namespace BTDB.ODBLayer
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true, Inherited = true)]
    public class SecondaryKeyAttribute : Attribute
    {
        public string Name { get; set; }
        public uint Order { get; set; }
        public uint IncludePrimaryKeyOrder { get; set; }

        public SecondaryKeyAttribute(string name)
        {
            Name = name;
        }
    }
}