using System;

namespace BTDB.FieldHandler
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Property | AttributeTargets.Enum | AttributeTargets.Field, AllowMultiple = false, Inherited = false)]
    public class PersistedNameAttribute : Attribute
    {
        public PersistedNameAttribute(string name)
        {
            Name = name;
        }
        public string Name { get; private set; }
    }
}
