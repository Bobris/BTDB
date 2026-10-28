using System;
using System.Collections.Generic;
using System.Reflection;
using BTDB.FieldHandler;

namespace BTDB.EventStoreLayer;

// A simple type whose values can also be instances of other types, which events describe by their runtime type.
class SimpleTypeDescriptorWithNativeTypes : SimpleTypeDescriptor, ITypeDescriptorMultipleNativeTypes
{
    readonly Type[] _nativeTypes;

    public SimpleTypeDescriptorWithNativeTypes(string name, MethodInfo loader, MethodInfo skipper, MethodInfo saver,
        Layer2Loader load, Layer2Skipper skip, Layer2Saver save, Type[] nativeTypes) : base(name, loader, skipper,
        saver, load, skip, save)
    {
        _nativeTypes = nativeTypes;
    }

    public IEnumerable<Type> GetNativeTypes() => _nativeTypes;
}
