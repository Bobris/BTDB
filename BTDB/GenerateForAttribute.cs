using System;

namespace BTDB;

[AttributeUsage(AttributeTargets.Assembly | AttributeTargets.Module | AttributeTargets.Class, AllowMultiple = true)]
public class GenerateForAttribute : Attribute
{
    public GenerateForAttribute(Type type)
    {
        Type = type;
    }

    public Type Type { get; }

    public Type[]? ConstructorParameters { get; set; }
}
