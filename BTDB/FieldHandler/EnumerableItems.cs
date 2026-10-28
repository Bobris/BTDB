using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BTDB.Serialization;

namespace BTDB.FieldHandler;

// Lists save the collections they know from their storage; any other IEnumerable<T>, like a ReadOnlyCollection<T> or
// an ImmutableList<T>, is enumerated, and each item is set into a one-item array whose element the item saver reads.
sealed class EnumerableItems
{
    readonly Type _itemType;
    readonly Type _enumerableType;
    readonly uint _slotOffset;

    public EnumerableItems(Type itemType)
    {
        _itemType = itemType;
        _enumerableType = typeof(IEnumerable<>).MakeGenericType(itemType);
        _slotOffset = RawData.MethodTableOf(itemType.MakeArrayType()).BaseSize - (uint)Unsafe.SizeOf<nint>();
    }

    public bool CanEnumerate(Type objType) => objType.IsAssignableTo(_enumerableType);

    public static List<object?> Collect(object obj)
    {
        var items = new List<object?>();
        foreach (var item in (IEnumerable)obj) items.Add(item);
        return items;
    }

    public Array CreateSlot() => Array.CreateInstance(_itemType, 1);

    public ref byte SetSlot(Array slot, object? item)
    {
        // SetValue unboxes into the element type, a Nullable<T> included.
        slot.SetValue(item, 0);
        return ref RawData.Ref(slot, _slotOffset);
    }
}
