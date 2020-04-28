using System;

namespace BTDB.IOC
{
    [AttributeUsage(AttributeTargets.Property)]
    public class DependencyAttribute : Attribute
    {
        public readonly string? Name;

        public DependencyAttribute(string? name = null)
        {
            Name = name;
        }
    }
}
