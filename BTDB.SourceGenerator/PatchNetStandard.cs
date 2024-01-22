// ReSharper disable once CheckNamespace

namespace System.Runtime.CompilerServices
{
    static class IsExternalInit
    {
    }

    [AttributeUsage(
        AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Interface,
        Inherited = false)]
    public sealed class CollectionBuilderAttribute(Type builderType, string methodName) : Attribute
    {
        public Type BuilderType { get; } = builderType;
        public string MethodName { get; } = methodName;
    }
}
