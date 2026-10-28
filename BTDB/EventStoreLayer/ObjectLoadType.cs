using System;

namespace BTDB.EventStoreLayer;

static class ObjectLoadType
{
    // A list or dictionary in an object member loads as its preferred type, whose collection metadata the generator
    // registers only for the types members are declared with. A nested one, like a List<List<int>> whose preferred
    // ICollection<ICollection<int>> no member declares, loads as the objects it holds instead.
    internal static Type For(ITypeDescriptor descriptor, Type loadAsType) => descriptor switch
    {
        ListTypeDescriptor when !loadAsType.IsArray && ListTypeDescriptor.FindMetadata(loadAsType) == null => typeof(object),
        DictionaryTypeDescriptor when DictionaryTypeDescriptor.FindMetadata(loadAsType) == null => typeof(object),
        _ => loadAsType
    };
}
