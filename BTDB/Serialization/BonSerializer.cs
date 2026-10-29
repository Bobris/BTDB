using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using BTDB.Bon;
using BTDB.Collections;
using BTDB.IL;

namespace BTDB.Serialization;

public delegate void Serialize(ref SerializerCtx ctx, ref byte value);

// Consumes exactly one Bon item, also when it returns false
public delegate bool Deserialize(ref DeserializerCtx ctx, ref byte value);

public interface ISerializerFactory
{
    void SerializeObject(ref SerializerCtx ctx, object? value);
    Serialize CreateSerializerForType(Type type);
    object? DeserializeObject(ref DeserializerCtx ctx);
    Deserialize CreateDeserializerForType(Type type);
    void SerializeTypeType(ref SerializerCtx ctx, Type type);
    Type DeserializeTypeType(ref DeserializerCtx ctx);
}

public struct SerializerCtx
{
    public ISerializerFactory Factory;
}

public struct DeserializerCtx
{
    public ISerializerFactory Factory;
}

public ref struct BonSerializerCtx
{
    public ISerializerFactory Factory;
    public ref BonBuilder Builder;
}

public ref struct BonDeserializerCtx
{
    public ISerializerFactory Factory;
    public ref Bon.Bon Bon;
}

public class BonSerializerFactory : ISerializerFactory
{
    readonly ConcurrentDictionary<nint, Serialize> _cache = new();
    readonly ConcurrentDictionary<nint, Deserialize> _cache2 = new();

    public void SerializeObject(ref SerializerCtx ctx, object? obj)
    {
        if (obj == null)
        {
            AsCtx(ref ctx).Builder.WriteNull();
            return;
        }

        EnsureSerializationStack();
        var type = obj.GetType();
        // A plain object has no members; its serializer would be the one for object, which waits below for a real
        // serializer replacing it and so would wait forever.
        if (type == typeof(object))
            throw new NotSupportedException("BonSerialization of " + type.ToSimpleName() + " is not supported.");
        var typePtr = type.TypeHandle.Value;
        _cache.TryGetValue(typePtr, out var serializer);
        if (serializer == null)
        {
            serializer = CreateSerializerForType(type);
            _cache.TryAdd(typePtr, serializer);
            _cache.TryGetValue(typePtr, out serializer);
        }

        if (type.IsValueType)
        {
            serializer!(ref ctx, ref RawData.Ref(obj, (uint)Unsafe.SizeOf<nint>()));
        }
        else
        {
            _cache.TryGetValue(typeof(object).TypeHandle.Value, out var objSerializer);
            while (serializer == objSerializer)
            {
                Thread.Yield();
                _cache.TryGetValue(typePtr, out serializer);
            }

            serializer!(ref ctx, ref Unsafe.As<object, byte>(ref obj));
        }
    }

    // Bon stores values, not references, so a reference cycle nests without end: fail with an exception instead of
    // overflowing the stack, which ends the process.
    static void EnsureSerializationStack()
    {
        if (!RuntimeHelpers.TryEnsureSufficientExecutionStack())
            throw new InsufficientExecutionStackException(
                "Bon serialization nests too deep. Bon stores values, not references, so it cannot serialize a reference cycle.");
    }

    [DebuggerStepThrough]
    static unsafe ref BonSerializerCtx AsCtx(ref SerializerCtx ctx)
    {
#pragma warning disable CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        fixed (void* ptr = &ctx)
        {
            return ref *(BonSerializerCtx*)ptr;
        }
#pragma warning restore CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
    }

    [DebuggerStepThrough]
    static unsafe ref SerializerCtx AsCtx(ref BonSerializerCtx ctx)
    {
#pragma warning disable CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        fixed (void* ptr = &ctx)
        {
            return ref *(SerializerCtx*)ptr;
        }
#pragma warning restore CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
    }

    [DebuggerStepThrough]
    static unsafe ref BonDeserializerCtx AsCtx(ref DeserializerCtx ctx)
    {
#pragma warning disable CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        fixed (void* ptr = &ctx)
        {
            return ref *(BonDeserializerCtx*)ptr;
        }
#pragma warning restore CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
    }

    [DebuggerStepThrough]
    static unsafe ref DeserializerCtx AsCtx(ref BonDeserializerCtx ctx)
    {
#pragma warning disable CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        fixed (void* ptr = &ctx)
        {
            return ref *(DeserializerCtx*)ptr;
        }
#pragma warning restore CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
    }

    public Serialize CreateCachedSerializerForType(Type type)
    {
        var typePtr = type.TypeHandle.Value;
        _cache.TryGetValue(typePtr, out var serializer);
        if (serializer == null)
        {
            if (type == typeof(object) || type.IsInterface || type == typeof(string) || type == typeof(byte[]) ||
                type.IsValueType)
            {
                serializer = CreateSerializerForType(type);
                _cache.TryAdd(typePtr, serializer);
                _cache.TryGetValue(typePtr, out serializer);
            }
            else
            {
                var objSerializer = CreateCachedSerializerForType(typeof(object));
                _cache.TryAdd(typePtr, objSerializer);
                try
                {
                    serializer = CreateSerializerForType(type);
                }
                catch
                {
                    // A placeholder left behind would make every later serialization of the type wait for it forever.
                    _cache.TryRemove(new KeyValuePair<nint, Serialize>(typePtr, objSerializer));
                    throw;
                }

                _cache.TryUpdate(typePtr, serializer, objSerializer);
                _cache.TryGetValue(typePtr, out serializer);
            }
        }

        return serializer!;
    }

    public unsafe Serialize CreateSerializerForType(Type type)
    {
        if (type.IsInterface && ReflectionMetadata.FindCollectionByType(type) is { } collectionMetadata)
        {
            return CreateCollectionInterfaceSerializer(collectionMetadata);
        }

        if (type == typeof(object) || type.IsInterface)
        {
            return (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Factory.SerializeObject(ref ctx, Unsafe.As<byte, object>(ref value));
            };
        }

        if (type == typeof(string))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, string>(ref value));
            };
        }

        if (type == typeof(Type))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Factory.SerializeTypeType(ref ctx, Unsafe.As<byte, Type>(ref value));
            };
        }

        if (type == typeof(byte[]))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                // A null array would otherwise convert to an empty span and load as an empty array.
                if (Unsafe.As<byte, byte[]?>(ref value) is { } bytes) AsCtx(ref ctx).Builder.Write(bytes);
                else AsCtx(ref ctx).Builder.WriteNull();
            };
        }

        if (type == typeof(bool))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, bool>(ref value));
            };
        }

        if (type == typeof(DateTime))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, DateTime>(ref value));
            };
        }

        if (type == typeof(Guid))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, Guid>(ref value));
            };
        }

        if (type == typeof(sbyte))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, sbyte>(ref value));
            };
        }

        if (type == typeof(short))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, short>(ref value));
            };
        }

        if (type == typeof(int))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, int>(ref value));
            };
        }

        if (type == typeof(long))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, long>(ref value));
            };
        }

        if (type == typeof(byte))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, byte>(ref value));
            };
        }

        if (type == typeof(ushort) || type == typeof(char))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, ushort>(ref value));
            };
        }

        if (type == typeof(uint))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, uint>(ref value));
            };
        }

        if (type == typeof(ulong))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, ulong>(ref value));
            };
        }

        if (type == typeof(Half))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write((double)Unsafe.As<byte, Half>(ref value));
            };
        }

        if (type == typeof(float))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, float>(ref value));
            };
        }

        if (type == typeof(double))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, double>(ref value));
            };
        }

        if (type.IsEnum)
        {
            return CreateSerializerForType(Enum.GetUnderlyingType(type));
        }

        if (Nullable.GetUnderlyingType(type) is { } nullableType)
        {
            var offset = RawData.Align(1, RawData.GetSizeAndAlign(nullableType).Align);
            var serializer = CreateCachedSerializerForType(nullableType);
            return (ref SerializerCtx ctx, ref byte value) =>
            {
                if (value != 0)
                {
                    serializer(ref ctx, ref Unsafe.AddByteOffset(ref value, offset));
                }
                else
                {
                    AsCtx(ref ctx).Builder.WriteNull();
                }
            };
        }

        if (TryGetTupleTypeArguments(type, typeof(ValueTuple), out var valueTupleTypes))
        {
            var offsets = RawData.GetValueTupleOffsets(type, valueTupleTypes);
            var serializers = CreateSerializers(valueTupleTypes);
            return (ref SerializerCtx ctx, ref byte value) =>
            {
                ref var builder = ref AsCtx(ref ctx).Builder;
                builder.StartTuple();
                for (var i = 0; i < serializers.Length; i++)
                {
                    serializers[i](ref ctx, ref Unsafe.AddByteOffset(ref value, offsets[i]));
                }

                builder.FinishTuple();
            };
        }

        if (TryGetTupleTypeArguments(type, typeof(Tuple), out var tupleTypes))
        {
            var offsets = RawData.GetTupleOffsets(type, tupleTypes);
            var serializers = CreateSerializers(tupleTypes);
            return (ref SerializerCtx ctx, ref byte value) =>
            {
                if (Unsafe.As<byte, object>(ref value) == null)
                {
                    AsCtx(ref ctx).Builder.WriteNull();
                    return;
                }

                ref var builder = ref AsCtx(ref ctx).Builder;
                builder.StartTuple();
                var obj = Unsafe.As<byte, object>(ref value);
                for (var i = 0; i < serializers.Length; i++)
                {
                    serializers[i](ref ctx, ref RawData.Ref(obj, offsets[i]));
                }

                builder.FinishTuple();
            };
        }

        if (type.IsArray)
        {
            if (!type.IsSZArray) throw new InvalidOperationException("Only SZArray is supported");
            var elementType = type.GetElementType()!;
            var elementTypeSerializer = CreateCachedSerializerForType(elementType);
            return (ref SerializerCtx ctx, ref byte value) =>
            {
                var obj = Unsafe.As<byte, object>(ref value);
                if (obj == null)
                {
                    AsCtx(ref ctx).Builder.WriteNull();
                    return;
                }

                var count = Unsafe.As<byte, int>(ref RawData.Ref(obj, (uint)Unsafe.SizeOf<nint>()));
                AsCtx(ref ctx).Builder.StartArray();
                if (count != 0)
                {
                    ref readonly var mt = ref RawData.MethodTableRef(obj);
                    var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                    var offsetDelta = mt.ComponentSize;
                    for (var i = 0; i < count; i++, offset += offsetDelta)
                    {
                        elementTypeSerializer(ref ctx, ref RawData.Ref(obj, offset));
                    }
                }

                AsCtx(ref ctx).Builder.FinishArray();
            };
        }

        if (type.SpecializationOf(typeof(List<>)) is { } listType)
        {
            var elementType = listType.GetGenericArguments()[0];
            var elementTypeSerializer = CreateCachedSerializerForType(elementType);
            return (ref SerializerCtx ctx, ref byte value) =>
            {
                var obj = Unsafe.As<byte, object>(ref value);
                if (obj == null)
                {
                    AsCtx(ref ctx).Builder.WriteNull();
                    return;
                }

                var count = Unsafe.As<ICollection>(obj).Count;
                AsCtx(ref ctx).Builder.StartArray();
                if (count != 0)
                {
                    obj = RawData.ListItems(Unsafe.As<List<object>>(obj));
                    ref readonly var mt = ref RawData.MethodTableRef(obj);
                    var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                    var offsetDelta = mt.ComponentSize;
                    for (var i = 0; i < count; i++, offset += offsetDelta)
                    {
                        elementTypeSerializer(ref ctx, ref RawData.Ref(obj, offset));
                    }
                }

                AsCtx(ref ctx).Builder.FinishArray();
            };
        }

        if (type.SpecializationOf(typeof(HashSet<>)) is { } hashSetType)
        {
            var elementType = hashSetType.GetGenericArguments()[0];
            var elementTypeSerializer = CreateCachedSerializerForType(elementType);
            var layout = RawData.GetHashSetEntriesLayout(elementType);
            return (ref SerializerCtx ctx, ref byte value) =>
            {
                var obj = Unsafe.As<byte, object>(ref value);
                if (obj == null)
                {
                    AsCtx(ref ctx).Builder.WriteNull();
                    return;
                }

                var count = Unsafe.As<byte, int>(ref RawData.Ref(obj,
                    RawData.Align(8 + 4 * (uint)Unsafe.SizeOf<nint>(), 8)));
                AsCtx(ref ctx).Builder.StartArray();
                if (count != 0)
                {
                    obj = RawData.HashSetEntries(Unsafe.As<HashSet<object>>(obj));
                    ref readonly var mt = ref RawData.MethodTableRef(obj);
                    var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                    var offsetDelta = mt.ComponentSize;
                    Debug.Assert(offsetDelta == layout.Size);
                    for (var i = 0; i < count; i++, offset += offsetDelta)
                    {
                        if (Unsafe.As<byte, int>(ref RawData.Ref(obj, offset + layout.OffsetNext)) < -1)
                        {
                            continue;
                        }

                        elementTypeSerializer(ref ctx, ref RawData.Ref(obj, offset + layout.Offset));
                    }
                }

                AsCtx(ref ctx).Builder.FinishArray();
            };
        }

        if (type.SpecializationOf(typeof(Dictionary<,>)) is { } dictionaryType)
        {
            var keyType = dictionaryType.GetGenericArguments()[0];
            var valueType = dictionaryType.GetGenericArguments()[1];
            var keyTypeSerializer = CreateCachedSerializerForType(keyType);
            var valueTypeSerializer = CreateCachedSerializerForType(valueType);
            var layout = RawData.GetDictionaryEntriesLayout(keyType, valueType);
            return (ref SerializerCtx ctx, ref byte value) =>
            {
                var obj = Unsafe.As<byte, object>(ref value);
                if (obj == null)
                {
                    AsCtx(ref ctx).Builder.WriteNull();
                    return;
                }

                var count = Unsafe.As<byte, int>(ref RawData.Ref(obj,
                    RawData.Align(8 + 6 * (uint)Unsafe.SizeOf<nint>(), 8)));
                AsCtx(ref ctx).Builder.StartDictionary();
                if (count != 0)
                {
                    obj = RawData.HashSetEntries(Unsafe.As<HashSet<object>>(obj));
                    ref readonly var mt = ref RawData.MethodTableRef(obj);
                    var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                    var offsetDelta = mt.ComponentSize;
                    for (var i = 0; i < count; i++, offset += offsetDelta)
                    {
                        if (Unsafe.As<byte, int>(ref RawData.Ref(obj, offset + layout.OffsetNext)) < -1)
                        {
                            continue;
                        }

                        keyTypeSerializer(ref ctx, ref RawData.Ref(obj, offset + layout.OffsetKey));
                        valueTypeSerializer(ref ctx, ref RawData.Ref(obj, offset + layout.OffsetValue));
                    }
                }

                AsCtx(ref ctx).Builder.FinishDictionary();
            };
        }

        var classMetadata = ReflectionMetadata.FindByType(type);
        if (classMetadata != null)
        {
            var persistName = classMetadata.TruePersistedName;
            var persistNameUtf8 = Encoding.UTF8.GetBytes(persistName);
            var fieldSerializers = new Serialize[classMetadata.Fields.Length];
            for (var i = 0; i < classMetadata.Fields.Length; i++)
            {
                var field = classMetadata.Fields[i];
                var nameUtf8 = Encoding.UTF8.GetBytes(field.Name);
                var serializer = CreateCachedSerializerForType(field.Type);
                if (field.PropRefGetter != null)
                {
                    var getter = field.PropRefGetter;
                    var stackAllocator = ReflectionMetadata.FindStackAllocatorByType(field.Type);
                    fieldSerializers[i] = (ref SerializerCtx ctx, ref byte value) =>
                    {
                        AsCtx(ref ctx).Builder.WriteKey(nameUtf8);
                        FieldSerializerCtx context;
                        context.CtxPtr = (nint)Unsafe.AsPointer(ref ctx);
                        context.StoragePtr = 0;
                        context.Getter = getter;
                        context.Value = (nint)Unsafe.AsPointer(ref value);
                        context.Serializer = serializer;

                        stackAllocator(ref Unsafe.As<FieldSerializerCtx, byte>(ref context), ref context.StoragePtr,
                            &Nested);

                        static void Nested(ref byte value)
                        {
                            ref var context = ref Unsafe.As<byte, FieldSerializerCtx>(ref value);
                            context.Getter(Unsafe.AsRef<object>((void*)context.Value),
                                ref Unsafe.AsRef<byte>((void*)context.StoragePtr));
                            context.Serializer(ref Unsafe.AsRef<SerializerCtx>((void*)context.CtxPtr),
                                ref Unsafe.AsRef<byte>((void*)context.StoragePtr));
                        }
                    };
                }
                else
                {
                    var offset = field.ByteOffset!.Value;
                    fieldSerializers[i] = (ref SerializerCtx ctx, ref byte value) =>
                    {
                        AsCtx(ref ctx).Builder.WriteKey(nameUtf8);
                        serializer(ref ctx, ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset));
                    };
                }
            }

            return (ref SerializerCtx ctx, ref byte value) =>
            {
                if (Unsafe.As<byte, object>(ref value) == null)
                {
                    AsCtx(ref ctx).Builder.WriteNull();
                    return;
                }

                EnsureSerializationStack();
                AsCtx(ref ctx).Builder.StartClass(persistNameUtf8);
                for (var i = 0; i < fieldSerializers.Length; i++)
                {
                    fieldSerializers[i](ref ctx, ref value);
                }

                AsCtx(ref ctx).Builder.FinishClass();
            };
        }

        throw new NotSupportedException("BonSerialization of " + type.ToSimpleName() + " is not supported.");
    }

    delegate void SerializeBoxed(ref SerializerCtx ctx, object? item);

    // A collection interface member can hold any implementation, like the array a collection expression makes for
    // IReadOnlyList<T>. The implementations Bon serializes keep their output, others are enumerated.
    Serialize CreateCollectionInterfaceSerializer(CollectionMetadata metadata)
    {
        var keySerializer = CreateBoxedSerializer(metadata.ElementKeyType);
        var valueSerializer = metadata.ElementValueType is { } valueType ? CreateBoxedSerializer(valueType) : null;
        var serializedDirectly = new ConcurrentDictionary<nint, bool>();
        return (ref SerializerCtx ctx, ref byte value) =>
        {
            var obj = Unsafe.As<byte, object?>(ref value);
            if (obj == null)
            {
                AsCtx(ref ctx).Builder.WriteNull();
                return;
            }

            var objType = obj.GetType();
            if (serializedDirectly.GetOrAdd(objType.TypeHandle.Value, static (_, t) => t.IsArray ||
                    t.SpecializationOf(typeof(List<>)) != null || t.SpecializationOf(typeof(HashSet<>)) != null ||
                    t.SpecializationOf(typeof(Dictionary<,>)) != null || ReflectionMetadata.FindByType(t) != null,
                    objType))
            {
                AsCtx(ref ctx).Factory.SerializeObject(ref ctx, obj);
                return;
            }

            if (valueSerializer == null)
            {
                AsCtx(ref ctx).Builder.StartArray();
                foreach (var item in (IEnumerable)obj)
                {
                    keySerializer(ref ctx, item);
                }

                AsCtx(ref ctx).Builder.FinishArray();
                return;
            }

            if (obj is not IDictionary dictionary)
                throw new NotSupportedException("BonSerialization of " + objType.ToSimpleName() + " is not supported.");
            AsCtx(ref ctx).Builder.StartDictionary();
            foreach (DictionaryEntry entry in dictionary)
            {
                keySerializer(ref ctx, entry.Key);
                valueSerializer(ref ctx, entry.Value);
            }

            AsCtx(ref ctx).Builder.FinishDictionary();
        };
    }

    SerializeBoxed CreateBoxedSerializer(Type type)
    {
        if (!type.IsValueType)
        {
            var serializer = CreateCachedSerializerForType(type);
            return (ref SerializerCtx ctx, object? item) => serializer(ref ctx, ref Unsafe.As<object?, byte>(ref item));
        }

        // A box holds its value after the method table pointer, and a boxed Nullable is null or a box of its value.
        var valueSerializer = CreateCachedSerializerForType(Nullable.GetUnderlyingType(type) ?? type);
        return (ref SerializerCtx ctx, object? item) =>
        {
            if (item == null) AsCtx(ref ctx).Builder.WriteNull();
            else valueSerializer(ref ctx, ref RawData.Ref(item, (uint)Unsafe.SizeOf<nint>()));
        };
    }

    static bool TryGetTupleTypeArguments(Type type, Type tupleType, out Type[] typeArguments)
    {
        if (type.IsGenericType)
        {
            var genericTypeDefinition = type.GetGenericTypeDefinition();
            typeArguments = type.GetGenericArguments();
            if (typeArguments.Length is >= 1 and <= 7 &&
                genericTypeDefinition.Namespace == "System" &&
                genericTypeDefinition.Name == tupleType.Name + "`" + typeArguments.Length)
            {
                return true;
            }
        }

        typeArguments = [];
        return false;
    }

    Serialize[] CreateSerializers(Type[] types)
    {
        var serializers = new Serialize[types.Length];
        for (var i = 0; i < serializers.Length; i++)
        {
            serializers[i] = CreateCachedSerializerForType(types[i]);
        }

        return serializers;
    }

    Deserialize[] CreateDeserializers(Type[] types)
    {
        var deserializers = new Deserialize[types.Length];
        for (var i = 0; i < deserializers.Length; i++)
        {
            deserializers[i] = CreateCachedDeserializerForType(types[i]);
        }

        return deserializers;
    }

    static Type CreateObjectTupleType(uint count)
    {
        return count switch
        {
            1 => typeof(Tuple<object?>),
            2 => typeof(Tuple<object?, object?>),
            3 => typeof(Tuple<object?, object?, object?>),
            4 => typeof(Tuple<object?, object?, object?, object?>),
            5 => typeof(Tuple<object?, object?, object?, object?, object?>),
            6 => typeof(Tuple<object?, object?, object?, object?, object?, object?>),
            7 => typeof(Tuple<object?, object?, object?, object?, object?, object?, object?>),
            _ => throw new NotSupportedException("Tuple with " + count + " items is not supported.")
        };
    }

    public Deserialize CreateCachedDeserializerForType(Type type)
    {
        var typePtr = type.TypeHandle.Value;
        _cache2.TryGetValue(typePtr, out var deserializer);
        if (deserializer != null) return deserializer;
        if (type.IsValueType || type.IsInterface || ReflectionMetadata.FindByType(type) == null)
        {
            deserializer = CreateDeserializerForType(type);
            _cache2.TryAdd(typePtr, deserializer);
            _cache2.TryGetValue(typePtr, out deserializer);
            return deserializer!;
        }

        // A registered class can hold itself, also when first reached as a member type rather than by its name: while
        // its deserializer is created, a placeholder stands in for it. Members created meanwhile keep the placeholder,
        // and other threads can get it from the cache, so it waits for the deserializer and uses it.
        Deserialize objDeserializer = null!;
        objDeserializer = (ref DeserializerCtx ctx, ref byte value) =>
            WaitForCreatedDeserializer(type, typePtr, objDeserializer)(ref ctx, ref value);
        if (!_cache2.TryAdd(typePtr, objDeserializer))
        {
            _cache2.TryGetValue(typePtr, out deserializer);
            return deserializer!;
        }

        try
        {
            deserializer = CreateDeserializerForType(type);
        }
        catch
        {
            // The placeholder waits for a deserializer replacing it, so left behind it would wait forever.
            _cache2.TryRemove(new KeyValuePair<nint, Deserialize>(typePtr, objDeserializer));
            throw;
        }

        _cache2.TryUpdate(typePtr, deserializer, objDeserializer);
        _cache2.TryGetValue(typePtr, out deserializer);
        return deserializer!;
    }

    Deserialize WaitForCreatedDeserializer(Type type, nint typePtr, Deserialize placeholder)
    {
        var spin = new SpinWait();
        while (true)
        {
            // Creation failed and removed the placeholder: create it here, which reports the failure again.
            if (!_cache2.TryGetValue(typePtr, out var deserializer)) return CreateCachedDeserializerForType(type);
            if (deserializer != placeholder) return deserializer;
            spin.SpinOnce();
        }
    }

    public object? DeserializeObject(ref DeserializerCtx ctx)
    {
        // Damaged or hostile data can nest, or loop, deeper than the stack: fail with an exception instead of crashing.
        RuntimeHelpers.EnsureSufficientExecutionStack();
        switch (AsCtx(ref ctx).Bon.BonType)
        {
            case BonType.Null:
            case BonType.Undefined:
                AsCtx(ref ctx).Bon.Skip();
                return null;
            case BonType.Class:
            {
                AsCtx(ref ctx).Bon.PeekClass(out var name);
                var deserializer =
                    ((BonSerializerFactory)AsCtx(ref ctx).Factory).CreateCachedDeserializerForName(name);
                object? res = null;
                deserializer(ref ctx, ref Unsafe.As<object, byte>(ref res));
                return res;
            }
            case BonType.Array:
            {
                AsCtx(ref ctx).Bon.TryGetArray(out var arrayBon);
                var count = arrayBon.Items;
                var res = new object?[count];
                for (var idx = 0u; idx < count; idx++)
                {
                    arrayBon.TryGet(idx, out var itemBon);
                    BonDeserializerCtx subCtx = new() { Factory = AsCtx(ref ctx).Factory, Bon = ref itemBon };
                    res[idx] = AsCtx(ref ctx).Factory.DeserializeObject(ref AsCtx(ref subCtx));
                }

                return res;
            }
            case BonType.Tuple:
            {
                AsCtx(ref ctx).Bon.TryGetTuple(out var arrayBon);
                var count = arrayBon.Items;
                if (count is < 1 or > 7) throw new NotSupportedException("Tuple with " + count + " items is not supported.");
                var res = RuntimeHelpers.GetUninitializedObject(CreateObjectTupleType(count));
                for (var idx = 0u; idx < count; idx++)
                {
                    arrayBon.TryGet(idx, out var itemBon);
                    BonDeserializerCtx subCtx = new() { Factory = AsCtx(ref ctx).Factory, Bon = ref itemBon };
                    Unsafe.As<byte, object?>(ref RawData.Ref(res, (idx + 1) * (uint)Unsafe.SizeOf<nint>())) =
                        AsCtx(ref ctx).Factory.DeserializeObject(ref AsCtx(ref subCtx));
                }

                return res;
            }
            case BonType.Dictionary:
            {
                AsCtx(ref ctx).Bon.TryGetDictionary(out var dictBon);
                var res = new Dictionary<object?, object?>();
                BonDeserializerCtx subCtx = new() { Factory = AsCtx(ref ctx).Factory, Bon = ref dictBon };
                while (dictBon.Items > 0)
                {
                    var keyObj = AsCtx(ref ctx).Factory.DeserializeObject(ref AsCtx(ref subCtx));
                    var valueObj = AsCtx(ref ctx).Factory.DeserializeObject(ref AsCtx(ref subCtx));
                    if (keyObj != null)
                        res[keyObj] = valueObj;
                }

                return res;
            }
            case BonType.Bool:
            {
                AsCtx(ref ctx).Bon.TryGetBool(out var res);
                return res;
            }
            case BonType.String:
            {
                AsCtx(ref ctx).Bon.TryGetString(out var res);
                return res;
            }
            case BonType.Float:
            {
                AsCtx(ref ctx).Bon.TryGetDouble(out var res);
                return res;
            }
            case BonType.DateTime:
            {
                AsCtx(ref ctx).Bon.TryGetDateTime(out var res);
                return res;
            }
            case BonType.Guid:
            {
                AsCtx(ref ctx).Bon.TryGetGuid(out var res);
                return res;
            }
            case BonType.ByteArray:
            {
                AsCtx(ref ctx).Bon.TryGetByteArray(out var res);
                return res.ToArray();
            }
            case BonType.Integer:
            {
                if (AsCtx(ref ctx).Bon.TryGetULong(out var res2))
                    return res2;
                if (AsCtx(ref ctx).Bon.TryGetLong(out var res))
                    return res;
                // Only crafted data has a negative integer below long.MinValue.
                AsCtx(ref ctx).Bon.TryGetDouble(out var resDouble);
                return resDouble;
            }
            case BonType.Object:
            {
                AsCtx(ref ctx).Bon.TryGetObject(out var keyedBon);
                var res = new Dictionary<string, object?>((int)keyedBon.Items);
                var valuesBon = keyedBon.Values();
                BonDeserializerCtx subCtx = new() { Factory = AsCtx(ref ctx).Factory, Bon = ref valuesBon };
                while (keyedBon.NextKey() is { } key)
                {
                    res[key] = AsCtx(ref ctx).Factory.DeserializeObject(ref AsCtx(ref subCtx));
                }

                return res;
            }
            default:
                throw new InvalidDataException("Cannot deserialize BonType " + AsCtx(ref ctx).Bon.BonType);
        }
    }

    public Deserialize CreateCachedDeserializerForName(ReadOnlySpan<byte> name)
    {
        if (name.SequenceEqual("type"u8))
        {
            return CreateCachedDeserializerForType(typeof(Type));
        }

        if (ReflectionMetadata.FindByName(name) is { } classMetadata)
        {
            return CreateCachedDeserializerForType(classMetadata.Type);
        }

        return (ref DeserializerCtx ctx, ref byte _) =>
        {
            AsCtx(ref ctx).Bon.Skip();
            return false;
        };
    }

    // A collection interface member can also hold a registered class, which Bon stores by its name.
    static bool DeserializeNotCollection(ref DeserializerCtx ctx, ref byte value, Type type)
    {
        if (type.IsInterface)
        {
            var res = AsCtx(ref ctx).Factory.DeserializeObject(ref ctx);
            Unsafe.As<byte, object?>(ref value) = type.IsInstanceOfType(res) ? res : null;
            return true;
        }

        AsCtx(ref ctx).Bon.Skip();
        return false;
    }

    public unsafe Deserialize CreateDeserializerForType(Type type)
    {
        if (type == typeof(object))
        {
            return (ref DeserializerCtx ctx, ref byte value) =>
            {
                Unsafe.As<byte, object>(ref value) = AsCtx(ref ctx).Factory.DeserializeObject(ref ctx);
                return true;
            };
        }

        // A collection interface loads as the collection its metadata creates, as a List<T> for IList<T>.
        if (type.IsInterface && ReflectionMetadata.FindCollectionByType(type) == null)
        {
            return (ref DeserializerCtx ctx, ref byte value) =>
            {
                var res = AsCtx(ref ctx).Factory.DeserializeObject(ref ctx);
                if (res == null || type.IsInstanceOfType(res))
                {
                    Unsafe.As<byte, object>(ref value) = res;
                }
                else
                {
                    Unsafe.As<byte, object>(ref value) = null;
                }

                return true;
            };
        }

        if (type == typeof(Type))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                Unsafe.As<byte, Type>(ref value) = AsCtx(ref ctx).Factory.DeserializeTypeType(ref ctx);
                return Unsafe.As<byte, Type>(ref value) != null;
            };
        }

        if (type == typeof(string))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetNull())
                {
                    Unsafe.As<byte, string?>(ref value) = null;
                    return true;
                }

                if (AsCtx(ref ctx).Bon.TryGetString(out var v))
                {
                    Unsafe.As<byte, string>(ref value) = v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(byte[]))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetNull())
                {
                    Unsafe.As<byte, byte[]?>(ref value) = null;
                    return true;
                }

                if (AsCtx(ref ctx).Bon.TryGetByteArray(out var v))
                {
                    Unsafe.As<byte, byte[]>(ref value) = v.ToArray();
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(byte))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetULong(out var v))
                {
                    if (v > byte.MaxValue) return false;
                    value = (byte)v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(sbyte))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetLong(out var v))
                {
                    if (v is < sbyte.MinValue or > sbyte.MaxValue) return false;
                    Unsafe.As<byte, sbyte>(ref value) = (sbyte)v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        // A char is written as its UTF-16 code unit.
        if (type == typeof(ushort) || type == typeof(char))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetULong(out var v))
                {
                    if (v > ushort.MaxValue) return false;
                    Unsafe.As<byte, ushort>(ref value) = (ushort)v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(short))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetLong(out var v))
                {
                    if (v is < short.MinValue or > short.MaxValue) return false;
                    Unsafe.As<byte, short>(ref value) = (short)v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(uint))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetULong(out var v))
                {
                    if (v > uint.MaxValue) return false;
                    Unsafe.As<byte, uint>(ref value) = (uint)v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(int))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetLong(out var v))
                {
                    if (v is < int.MinValue or > int.MaxValue) return false;
                    Unsafe.As<byte, int>(ref value) = (int)v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(ulong))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetULong(out var v))
                {
                    Unsafe.As<byte, ulong>(ref value) = v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(long))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetLong(out var v))
                {
                    Unsafe.As<byte, long>(ref value) = v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(float))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetDouble(out var v))
                {
                    Unsafe.As<byte, float>(ref value) = (float)v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(double))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetDouble(out var v))
                {
                    Unsafe.As<byte, double>(ref value) = v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(bool))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetBool(out var v))
                {
                    Unsafe.As<byte, bool>(ref value) = v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(DateTime))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetDateTime(out var v))
                {
                    Unsafe.As<byte, DateTime>(ref value) = v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(Guid))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetGuid(out var v))
                {
                    Unsafe.As<byte, Guid>(ref value) = v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type == typeof(Half))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetDouble(out var v))
                {
                    Unsafe.As<byte, Half>(ref value) = (Half)v;
                    return true;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }
            };
        }

        if (type.IsEnum)
        {
            return CreateDeserializerForType(Enum.GetUnderlyingType(type));
        }

        if (Nullable.GetUnderlyingType(type) is { } nullableType)
        {
            var offset = RawData.Align(1, RawData.GetSizeAndAlign(nullableType).Align);
            var deserializer = CreateCachedDeserializerForType(nullableType);
            return (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetNull())
                {
                    value = 0;
                    return true;
                }

                if (deserializer(ref ctx, ref Unsafe.AddByteOffset(ref value, offset)))
                {
                    value = 1;
                    return true;
                }

                return false;
            };
        }

        if (TryGetTupleTypeArguments(type, typeof(ValueTuple), out var valueTupleTypes))
        {
            var offsets = RawData.GetValueTupleOffsets(type, valueTupleTypes);
            var deserializers = CreateDeserializers(valueTupleTypes);
            return (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (!AsCtx(ref ctx).Bon.TryGetTuple(out var tupleBon))
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }

                if (tupleBon.Items >= deserializers.Length)
                {
                    for (var i = 0; i < deserializers.Length; i++)
                    {
                        tupleBon.TryGet((uint)i, out var itemBon);
                        BonDeserializerCtx subCtx = new() { Factory = AsCtx(ref ctx).Factory, Bon = ref itemBon };
                        if (!deserializers[i](ref AsCtx(ref subCtx), ref Unsafe.AddByteOffset(ref value, offsets[i])))
                        {
                            return false;
                        }
                    }

                    return true;
                }

                return false;
            };
        }

        if (TryGetTupleTypeArguments(type, typeof(Tuple), out var tupleTypes))
        {
            var offsets = RawData.GetTupleOffsets(type, tupleTypes);
            var deserializers = CreateDeserializers(tupleTypes);
            return (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetNull())
                {
                    Unsafe.As<byte, object?>(ref value) = null;
                    return true;
                }

                if (!AsCtx(ref ctx).Bon.TryGetTuple(out var tupleBon))
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }

                if (tupleBon.Items >= deserializers.Length)
                {
                    var res = RuntimeHelpers.GetUninitializedObject(type);
                    for (var i = 0; i < deserializers.Length; i++)
                    {
                        tupleBon.TryGet((uint)i, out var itemBon);
                        BonDeserializerCtx subCtx = new() { Factory = AsCtx(ref ctx).Factory, Bon = ref itemBon };
                        if (!deserializers[i](ref AsCtx(ref subCtx), ref RawData.Ref(res, offsets[i])))
                        {
                            return false;
                        }
                    }

                    Unsafe.As<byte, object>(ref value) = res;
                    return true;
                }

                return false;
            };
        }

        if (type.IsArray)
        {
            if (!type.IsSZArray) throw new InvalidOperationException("Only SZArray is supported");
            var elementType = type.GetElementType()!;
            var elementTypeDeserializer = CreateCachedDeserializerForType(elementType);
            ref readonly var mt = ref RawData.MethodTableOf(type);
            var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
            var offsetDelta = mt.ComponentSize;
            return (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetNull())
                {
                    Unsafe.As<byte, object>(ref value) = null;
                    return true;
                }

                if (AsCtx(ref ctx).Bon.TryGetArray(out var arrayBon))
                {
                    var count = arrayBon.Items;
                    var res = Array.CreateInstance(elementType, count);
                    var o = offset;
                    for (var idx = 0u; idx < count; idx++)
                    {
                        arrayBon.TryGet(idx, out var itemBon);
                        BonDeserializerCtx subCtx = new() { Factory = AsCtx(ref ctx).Factory, Bon = ref itemBon };
                        elementTypeDeserializer(ref AsCtx(ref subCtx), ref RawData.Ref(res, o));
                        o += offsetDelta;
                    }

                    Unsafe.As<byte, object>(ref value) = res;
                    return true;
                }

                AsCtx(ref ctx).Bon.Skip();
                return false;
            };
        }

        var collectionMetadata = ReflectionMetadata.FindCollectionByType(type);
        delegate*<object> subclassCreator = null;
        foreach (var definition in (ReadOnlySpan<Type>)[typeof(List<>), typeof(HashSet<>), typeof(Dictionary<,>)])
        {
            if (collectionMetadata != null || type.SpecializationOf(definition) is not { } baseType) continue;
            collectionMetadata = ReflectionMetadata.FindCollectionByType(baseType);
            if (collectionMetadata == null || baseType == type) continue;
            // The base collection's metadata creates the base type, which a member of the subclass cannot hold: create
            // the subclass from its own metadata, and add to it as to the base, or do not support it.
            if (ReflectionMetadata.FindByType(type) is { } subclassMetadata) subclassCreator = subclassMetadata.Creator;
            else collectionMetadata = null;
        }

        if (collectionMetadata is { ElementValueType: null } listMetadata)
        {
            var elementType = listMetadata.ElementKeyType;
            var elementTypeDeserializer = CreateCachedDeserializerForType(elementType);
            var stackAllocator = ReflectionMetadata.FindStackAllocatorByType(elementType);
            var elementSize = RawData.GetSizeAndAlign(elementType).Size;
            return (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetNull())
                {
                    Unsafe.As<byte, object>(ref value) = null;
                    return true;
                }

                if (AsCtx(ref ctx).Bon.TryGetArray(out var arrayBon))
                {
                    ListDeserializerCtx context;
                    context.ArrayBonPtr = (nint)Unsafe.AsPointer(ref arrayBon);
                    context.StoragePtr = 0;
                    context.StorageSize = elementSize;
                    context.ElementTypeDeserializer = elementTypeDeserializer;
                    context.Metadata = listMetadata;
                    context.SubclassCreator = subclassCreator;
                    context.Result = null!;
                    context.Factory = ctx.Factory;

                    stackAllocator(ref Unsafe.As<ListDeserializerCtx, byte>(ref context), ref context.StoragePtr,
                        &Nested);

                    static void Nested(ref byte value)
                    {
                        ref var context = ref Unsafe.As<byte, ListDeserializerCtx>(ref value);
                        ref var arrayBon = ref Unsafe.AsRef<ArrayBon>((void*)context.ArrayBonPtr);
                        var count = arrayBon.Items;
                        var res = context.SubclassCreator != null
                            ? context.SubclassCreator()
                            : context.Metadata.Creator(count);
                        context.Result = res;
                        for (var i = 0u; i < count; i++)
                        {
                            arrayBon.TryGet(i, out var itemBon);
                            BonDeserializerCtx subCtx = new() { Factory = context.Factory, Bon = ref itemBon };
                            // The storage is reused for every item: an item that does not load, like a null string,
                            // must add the default instead of the item before it.
                            Unsafe.InitBlockUnaligned(ref Unsafe.AsRef<byte>((void*)context.StoragePtr), 0,
                                context.StorageSize);
                            context.ElementTypeDeserializer(ref AsCtx(ref subCtx),
                                ref Unsafe.AsRef<byte>((void*)context.StoragePtr));
                            context.Metadata.Adder(res, ref Unsafe.AsRef<byte>((void*)context.StoragePtr));
                        }
                    }

                    Unsafe.As<byte, object>(ref value) = context.Result;
                    return true;
                }

                return DeserializeNotCollection(ref ctx, ref value, type);
            };
        }

        if (collectionMetadata is { ElementValueType: not null } dictMetadata)
        {
            var keyType = dictMetadata.ElementKeyType;
            var valueType = dictMetadata.ElementValueType;
            var keyTypeDeserializer = CreateCachedDeserializerForType(keyType);
            var valueTypeDeserializer = CreateCachedDeserializerForType(valueType!);
            var keyStackAllocator = ReflectionMetadata.FindStackAllocatorByType(keyType);
            var valueStackAllocator = ReflectionMetadata.FindStackAllocatorByType(valueType);
            var keySize = RawData.GetSizeAndAlign(keyType).Size;
            var valueSize = RawData.GetSizeAndAlign(valueType).Size;
            return (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetNull())
                {
                    Unsafe.As<byte, object>(ref value) = null;
                    return true;
                }

                if (AsCtx(ref ctx).Bon.TryGetDictionary(out var dictBon))
                {
                    DictDeserializerCtx context;
                    context.DictBonPtr = (nint)Unsafe.AsPointer(ref dictBon);
                    context.KeyStoragePtr = 0;
                    context.KeyStorageSize = keySize;
                    context.ValueStorageSize = valueSize;
                    context.KeyTypeDeserializer = keyTypeDeserializer;
                    context.ValueStoragePtr = 0;
                    context.ValueTypeDeserializer = valueTypeDeserializer;
                    context.Metadata = dictMetadata;
                    context.SubclassCreator = subclassCreator;
                    context.Result = null!;
                    context.Factory = ctx.Factory;
                    context.ValueStackAllocator = valueStackAllocator;

                    keyStackAllocator(ref Unsafe.As<DictDeserializerCtx, byte>(ref context), ref context.KeyStoragePtr,
                        &NestedOuter);

                    static void NestedOuter(ref byte value)
                    {
                        ref var context = ref Unsafe.As<byte, DictDeserializerCtx>(ref value);
                        context.ValueStackAllocator(ref Unsafe.As<DictDeserializerCtx, byte>(ref context),
                            ref context.ValueStoragePtr, &Nested);
                    }

                    static void Nested(ref byte value)
                    {
                        ref var context = ref Unsafe.As<byte, DictDeserializerCtx>(ref value);

                        ref var dictBon = ref Unsafe.AsRef<Bon.Bon>((void*)context.DictBonPtr);
                        var res = context.SubclassCreator != null
                            ? context.SubclassCreator()
                            : context.Metadata.Creator(dictBon.Items / 2);
                        context.Result = res;
                        BonDeserializerCtx subCtx = new() { Factory = context.Factory, Bon = ref dictBon };
                        while (dictBon.Items > 0)
                        {
                            // The storage is reused for every pair, see the list above.
                            Unsafe.InitBlockUnaligned(ref Unsafe.AsRef<byte>((void*)context.KeyStoragePtr), 0,
                                context.KeyStorageSize);
                            Unsafe.InitBlockUnaligned(ref Unsafe.AsRef<byte>((void*)context.ValueStoragePtr), 0,
                                context.ValueStorageSize);
                            context.KeyTypeDeserializer(ref AsCtx(ref subCtx),
                                ref Unsafe.AsRef<byte>((void*)context.KeyStoragePtr));
                            context.ValueTypeDeserializer(ref AsCtx(ref subCtx),
                                ref Unsafe.AsRef<byte>((void*)context.ValueStoragePtr));
                            try
                            {
                                context.Metadata.AdderKeyValue(res, ref Unsafe.AsRef<byte>((void*)context.KeyStoragePtr),
                                    ref Unsafe.AsRef<byte>((void*)context.ValueStoragePtr));
                            }
                            // A serialized dictionary has distinct keys that are not null; damage can repeat one or
                            // turn one into null.
                            catch (ArgumentException error)
                            {
                                throw new InvalidDataException("Bon dictionary has a repeated or null key.", error);
                            }
                        }
                    }

                    Unsafe.As<byte, object>(ref value) = context.Result;
                    return true;
                }

                return DeserializeNotCollection(ref ctx, ref value, type);
            };
        }

        var classMetadata = ReflectionMetadata.FindByType(type);
        if (classMetadata != null)
        {
            var persistName = classMetadata.TruePersistedName;
            var persistNameUtf8 = Encoding.UTF8.GetBytes(persistName);
            var fieldDeserializers = new Deserialize[classMetadata.Fields.Length];
            var name2Idx = new SpanByteNoRemoveDictionary<uint>();
            for (var i = 0; i < classMetadata.Fields.Length; i++)
            {
                var field = classMetadata.Fields[i];
                var nameUtf8 = Encoding.UTF8.GetBytes(field.Name);
                name2Idx[nameUtf8] = (uint)i;
                var deserializer = CreateCachedDeserializerForType(field.Type);
                if (field.PropRefSetter != null)
                {
                    var setter = field.PropRefSetter;
                    var stackAllocator = ReflectionMetadata.FindStackAllocatorByType(field.Type);
                    fieldDeserializers[i] = (ref DeserializerCtx ctx, ref byte value) =>
                    {
                        FieldDeserializerCtx context;
                        context.Target = Unsafe.As<byte, object>(ref value);
                        context.CtxPtr = (nint)Unsafe.AsPointer(ref ctx);
                        context.StoragePtr = 0;
                        context.Deserializer = deserializer;
                        context.Result = false;
                        context.Setter = setter;
                        stackAllocator(ref Unsafe.As<FieldDeserializerCtx, byte>(ref context), ref context.StoragePtr,
                            &Nested);
                        return context.Result;

                        static void Nested(ref byte value)
                        {
                            ref var context = ref Unsafe.As<byte, FieldDeserializerCtx>(ref value);
                            if (context.Deserializer(ref Unsafe.AsRef<DeserializerCtx>((void*)context.CtxPtr),
                                    ref Unsafe.AsRef<byte>((void*)context.StoragePtr)))
                            {
                                context.Setter(context.Target,
                                    ref Unsafe.AsRef<byte>((void*)context.StoragePtr));
                                context.Result = true;
                            }
                        }
                    };
                }
                else
                {
                    var offset = field.ByteOffset!.Value;
                    fieldDeserializers[i] = (ref DeserializerCtx ctx, ref byte value) =>
                        deserializer(ref ctx, ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset));
                }
            }

            return (ref DeserializerCtx ctx, ref byte value) =>
            {
                RuntimeHelpers.EnsureSufficientExecutionStack();
                // A null loads as null: reported as not loaded, it left a member its initial value and failed a tuple.
                if (AsCtx(ref ctx).Bon.TryGetNull())
                {
                    Unsafe.As<byte, object?>(ref value) = null;
                    return true;
                }

                if (!AsCtx(ref ctx).Bon.PeekClass(out var peekedName))
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }

                if (!peekedName.SequenceEqual(persistNameUtf8))
                {
                    // A derived class, as a recursive member stores, whose serializer writes the runtime type.
                    if (ReflectionMetadata.FindByName(peekedName) is { } derived && derived.Type != type &&
                        type.IsAssignableFrom(derived.Type))
                        return ((BonSerializerFactory)AsCtx(ref ctx).Factory).CreateCachedDeserializerForType(derived.Type)(
                            ref ctx, ref value);
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }

                if (!AsCtx(ref ctx).Bon.TryGetClass(out var keyedBon, out _))
                {
                    AsCtx(ref ctx).Bon.Skip();
                    return false;
                }

                var res = classMetadata.Creator();
                var valuesBon = keyedBon.Values();
                BonDeserializerCtx subCtx = new() { Factory = AsCtx(ref ctx).Factory, Bon = ref valuesBon };
                while (!valuesBon.Eof)
                {
                    // Field deserializers consume their value even when they fail, so skip only unknown fields
                    if (name2Idx.TryGetValue(keyedBon.NextKeyUtf8(), out var idx))
                        fieldDeserializers[idx](ref AsCtx(ref subCtx), ref Unsafe.As<object, byte>(ref res));
                    else
                        valuesBon.Skip();
                }

                Unsafe.As<byte, object>(ref value) = res;
                return true;
            };
        }

        throw new NotSupportedException("BonDeserialization of " + type.ToSimpleName() + " is not supported.");
    }

    ReadOnlySpan<byte> SimpleType2String(Type type)
    {
        if (type == typeof(byte)) return "u8"u8;
        if (type == typeof(sbyte)) return "s8"u8;
        if (type == typeof(ushort)) return "u16"u8;
        if (type == typeof(short)) return "s16"u8;
        if (type == typeof(int)) return "s32"u8;
        if (type == typeof(uint)) return "u32"u8;
        if (type == typeof(long)) return "s64"u8;
        if (type == typeof(ulong)) return "u64"u8;
        if (type == typeof(Half)) return "f16"u8;
        if (type == typeof(float)) return "f32"u8;
        if (type == typeof(double)) return "f64"u8;
        if (type == typeof(string)) return "s"u8;
        if (type == typeof(char)) return "char"u8;
        if (type == typeof(bool)) return "b"u8;
        if (type == typeof(void)) return "void"u8;
        if (type == typeof(object)) return "object"u8;
        if (type == typeof(decimal)) return "decimal"u8;
        if (type == typeof(nint)) return "ni"u8;
        if (type == typeof(nuint)) return "nu"u8;
        throw new NotSupportedException("BonSerialization of Type " + type.ToSimpleName() + " is not supported.");
    }

    public void SerializeTypeType(ref SerializerCtx ctx, Type type)
    {
        ref var builder = ref AsCtx(ref ctx).Builder;
        builder.StartClass("type"u8);
        builder.WriteKey("Name"u8);
        if (ReflectionMetadata.FindByType(type) is { } metadata)
        {
            builder.Write(metadata.TruePersistedName);
        }
        else
        {
            builder.WriteUtf8(SimpleType2String(type));
        }

        AsCtx(ref ctx).Builder.FinishClass();
    }

    public Type? DeserializeTypeType(ref DeserializerCtx ctx)
    {
        if (!AsCtx(ref ctx).Bon.TryGetClass(out var keyedBon, out var name))
        {
            AsCtx(ref ctx).Bon.Skip();
            return null;
        }

        if (!name.SequenceEqual("type"u8))
        {
            return null;
        }

        if (keyedBon.NextKeyUtf8().SequenceEqual("Name"u8))
        {
            if (keyedBon.Values().TryGetStringBytes(out var typeName))
            {
                if (ReflectionMetadata.FindByName(typeName) is { } metadata)
                {
                    return metadata.Type;
                }

                if (typeName.SequenceEqual("u8"u8)) return typeof(byte);
                if (typeName.SequenceEqual("s8"u8)) return typeof(sbyte);
                if (typeName.SequenceEqual("u16"u8)) return typeof(ushort);
                if (typeName.SequenceEqual("s16"u8)) return typeof(short);
                if (typeName.SequenceEqual("s32"u8)) return typeof(int);
                if (typeName.SequenceEqual("u32"u8)) return typeof(uint);
                if (typeName.SequenceEqual("s64"u8)) return typeof(long);
                if (typeName.SequenceEqual("u64"u8)) return typeof(ulong);
                if (typeName.SequenceEqual("f16"u8)) return typeof(Half);
                if (typeName.SequenceEqual("f32"u8)) return typeof(float);
                if (typeName.SequenceEqual("f64"u8)) return typeof(double);
                if (typeName.SequenceEqual("s"u8)) return typeof(string);
                if (typeName.SequenceEqual("char"u8)) return typeof(char);
                if (typeName.SequenceEqual("b"u8)) return typeof(bool);
                if (typeName.SequenceEqual("void"u8)) return typeof(void);
                if (typeName.SequenceEqual("object"u8)) return typeof(object);
                if (typeName.SequenceEqual("decimal"u8)) return typeof(decimal);
                if (typeName.SequenceEqual("ni"u8)) return typeof(nint);
                if (typeName.SequenceEqual("nu"u8)) return typeof(nuint);
                throw new InvalidDataException("BonSerializer unknown type name: " + Encoding.UTF8.GetString(typeName));
            }
        }

        return null;
    }

    public static BonSerializerFactory Instance { get; } = new();

    public static void Serialize(ref BonBuilder builder, object? value)
    {
        var ctx = new BonSerializerCtx { Factory = Instance, Builder = ref builder };
        Instance.SerializeObject(ref AsCtx(ref ctx), value);
    }

    public static object? Deserialize(ref Bon.Bon bon)
    {
        var ctx = new BonDeserializerCtx { Factory = Instance, Bon = ref bon };
        return Instance.DeserializeObject(ref AsCtx(ref ctx));
    }

    struct FieldSerializerCtx
    {
        internal nint StoragePtr;
        internal nint CtxPtr;
        internal unsafe delegate*<object, ref byte, void> Getter;
        internal IntPtr Value;
        internal Serialize Serializer;
    }

    struct FieldDeserializerCtx
    {
        internal object Target;
        internal nint StoragePtr;
        internal nint CtxPtr;
        internal Deserialize Deserializer;
        internal bool Result;
        internal unsafe delegate*<object, ref byte, void> Setter;
    }

    struct ListDeserializerCtx
    {
        internal nint StoragePtr;
        internal uint StorageSize;
        internal Deserialize ElementTypeDeserializer;
        internal CollectionMetadata Metadata;
        internal unsafe delegate*<object> SubclassCreator;
        internal object Result;
        internal ISerializerFactory Factory;
        internal IntPtr ArrayBonPtr;
    }

    struct DictDeserializerCtx
    {
        internal nint KeyStoragePtr;
        internal uint KeyStorageSize;
        internal uint ValueStorageSize;
        internal Deserialize KeyTypeDeserializer;
        internal nint ValueStoragePtr;
        internal Deserialize ValueTypeDeserializer;
        internal CollectionMetadata Metadata;
        internal unsafe delegate*<object> SubclassCreator;
        internal object Result;
        internal ISerializerFactory Factory;
        internal IntPtr DictBonPtr;
        internal unsafe delegate*<ref byte, ref IntPtr, delegate*<ref byte, void>, void> ValueStackAllocator;
    }
}
