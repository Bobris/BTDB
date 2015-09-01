using System;

namespace BTDB.ODBLayer
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class UniqueIndexAttribute : Attribute
    {
        public string Name { get; set; }
        public uint Order { get; set; }

        public UniqueIndexAttribute(string name, uint order = 0)
        {
            Name = name;
            Order = order;
        }
    }
}
