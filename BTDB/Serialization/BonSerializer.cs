using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using BTDB.Bon;
using BTDB.IL;

namespace BTDB.Serialization;

public delegate void Serialize(ref SerializerCtx ctx, ref byte value);

public interface ISerializerFactory
{
    void SerializeObject(ref SerializerCtx ctx, ref byte value);
    Serialize CreateSerializerForType(Type type);
}

public ref struct SerializerCtx
{
    public ISerializerFactory Factory;
}

public ref struct BonSerializerCtx
{
    public ISerializerFactory Factory;
    public ref BonBuilder Builder;
}

public class BonSerializerFactory : ISerializerFactory
{
    readonly ConcurrentDictionary<nint, Serialize> _cache = new();

    public void SerializeObject(ref SerializerCtx ctx, ref byte value)
    {
        var obj = Unsafe.As<byte, object>(ref value);
        if (obj == null)
        {
            AsCtx(ref ctx).Builder.WriteNull();
            return;
        }

        var typePtr = obj.GetType().TypeHandle.Value;
        _cache.TryGetValue(typePtr, out var serializer);
        if (serializer == null)
        {
            serializer = CreateSerializerForType(obj.GetType());
            _cache.TryAdd(typePtr, serializer);
            _cache.TryGetValue(typePtr, out serializer);
        }

        serializer!(ref ctx, ref value);
    }

    static unsafe ref BonSerializerCtx AsCtx(ref SerializerCtx ctx)
    {
#pragma warning disable CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        fixed (void* ptr = &ctx)
        {
            return ref *(BonSerializerCtx*)ptr;
        }
#pragma warning restore CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
    }

    static unsafe ref SerializerCtx AsCtx(ref BonSerializerCtx ctx)
    {
#pragma warning disable CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        fixed (void* ptr = &ctx)
        {
            return ref *(SerializerCtx*)ptr;
        }
#pragma warning restore CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
    }

    public Serialize CreateCachedSerializerForType(Type type)
    {
        if (!type.IsValueType)
        {
            return (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Factory.SerializeObject(ref ctx, ref value);
            };
        }

        var typePtr = type.TypeHandle.Value;
        _cache.TryGetValue(typePtr, out var serializer);
        if (serializer == null)
        {
            serializer = CreateSerializerForType(type);
            _cache.TryAdd(typePtr, serializer);
            _cache.TryGetValue(typePtr, out serializer);
        }

        return serializer!;
    }

    public unsafe Serialize CreateSerializerForType(Type type)
    {
        if (type == typeof(string))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, string>(ref value));
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

        if (type.SpecializationOf(typeof(ValueTuple<,>)) is { } valueTuple2)
        {
            var typeParams = valueTuple2.GetGenericArguments();
            var offsets = RawData.GetOffsets(typeParams[0], typeParams[1]);
            var serializer0 = CreateCachedSerializerForType(typeParams[0]);
            var serializer1 = CreateCachedSerializerForType(typeParams[1]);
            return (ref SerializerCtx ctx, ref byte value) =>
            {
                ref var builder = ref AsCtx(ref ctx).Builder;
                builder.StartClass("Tuple"u8);
                builder.WriteKey("Item1"u8);
                serializer0(ref ctx, ref Unsafe.AddByteOffset(ref value, offsets.Item1));
                builder.WriteKey("Item2"u8);
                serializer1(ref ctx, ref Unsafe.AddByteOffset(ref value, offsets.Item2));
                builder.FinishClass();
            };
        }

        if (type.SpecializationOf(typeof(Tuple<,>)) is { } tuple2)
        {
            var typeParams = tuple2.GetGenericArguments();
            var offsets = RawData.GetOffsets(typeParams[0], typeParams[1]);
            offsets = (offsets.Item1 + (uint)Unsafe.SizeOf<nint>(), offsets.Item2 + (uint)Unsafe.SizeOf<nint>());
            var serializer0 = CreateCachedSerializerForType(typeParams[0]);
            var serializer1 = CreateCachedSerializerForType(typeParams[1]);
            return (ref SerializerCtx ctx, ref byte value) =>
            {
                ref var builder = ref AsCtx(ref ctx).Builder;
                builder.StartClass("Tuple"u8);
                builder.WriteKey("Item1"u8);
                serializer0(ref ctx, ref RawData.Ref(Unsafe.As<byte, object>(ref value), offsets.Item1));
                builder.WriteKey("Item2"u8);
                serializer1(ref ctx, ref RawData.Ref(Unsafe.As<byte, object>(ref value), offsets.Item2));
                builder.FinishClass();
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
                var count = Unsafe.As<byte, int>(ref RawData.Ref(obj, (uint)Unsafe.SizeOf<nint>()));
                ref readonly var mt = ref RawData.MethodTableRef(obj);
                var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                var offsetDelta = mt.ComponentSize;
                AsCtx(ref ctx).Builder.StartArray();
                for (var i = 0; i < count; i++, offset += offsetDelta)
                {
                    elementTypeSerializer(ref ctx, ref RawData.Ref(obj, offset));
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
                var count = Unsafe.As<ICollection>(obj).Count;
                obj = RawData.ListItems(Unsafe.As<List<object>>(obj));
                ref readonly var mt = ref RawData.MethodTableRef(obj);
                var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                var offsetDelta = mt.ComponentSize;
                AsCtx(ref ctx).Builder.StartArray();
                for (var i = 0; i < count; i++, offset += offsetDelta)
                {
                    elementTypeSerializer(ref ctx, ref RawData.Ref(obj, offset));
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
                var count = Unsafe.As<byte, int>(ref RawData.Ref(obj,
                    RawData.Align(8 + 4 * (uint)Unsafe.SizeOf<nint>(), 8)));
                obj = RawData.HashSetEntries(Unsafe.As<HashSet<object>>(obj));
                ref readonly var mt = ref RawData.MethodTableRef(obj);
                var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                var offsetDelta = mt.ComponentSize;
                Debug.Assert(offsetDelta == layout.Size);
                AsCtx(ref ctx).Builder.StartArray();
                for (var i = 0; i < count; i++, offset += offsetDelta)
                {
                    if (Unsafe.As<byte, int>(ref RawData.Ref(obj, offset + 4)) < -1)
                    {
                        continue;
                    }

                    elementTypeSerializer(ref ctx, ref RawData.Ref(obj, offset + layout.Offset));
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
                var count = Unsafe.As<byte, int>(ref RawData.Ref(obj,
                    RawData.Align(8 + 6 * (uint)Unsafe.SizeOf<nint>(), 8)));
                obj = RawData.HashSetEntries(Unsafe.As<HashSet<object>>(obj));
                ref readonly var mt = ref RawData.MethodTableRef(obj);
                var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                var offsetDelta = mt.ComponentSize;
                AsCtx(ref ctx).Builder.StartDictionary();
                for (var i = 0; i < count; i++, offset += offsetDelta)
                {
                    if (Unsafe.As<byte, int>(ref RawData.Ref(obj, offset + 4)) < -1)
                    {
                        continue;
                    }

                    keyTypeSerializer(ref ctx, ref RawData.Ref(obj, offset + layout.OffsetKey));
                    valueTypeSerializer(ref ctx, ref RawData.Ref(obj, offset + layout.OffsetValue));
                }

                AsCtx(ref ctx).Builder.FinishDictionary();
            };
        }

        var classMetadata = ReflectionMetadata.FindByType(type);
        if (classMetadata != null)
        {
            var persistName = classMetadata.PersistedName ?? (string.IsNullOrEmpty(classMetadata.Namespace)
                ? classMetadata.Name
                : classMetadata.Namespace + "." + classMetadata.Name);
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
                    if (field.Type.IsValueType)
                    {
                        if ((*(RawData.MethodTable*)field.Type.TypeHandle.Value).ContainsGCPointers)
                            throw new InvalidOperationException("Value type with GC pointers is not supported.");
                        fieldSerializers[i] = (ref SerializerCtx ctx, ref byte value) =>
                        {
                            AsCtx(ref ctx).Builder.WriteKey(nameUtf8);
                            UInt128 temp = default;
                            getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref temp));
                            serializer(ref ctx, ref Unsafe.As<UInt128, byte>(ref temp));
                        };
                    }
                    else
                    {
                        fieldSerializers[i] = (ref SerializerCtx ctx, ref byte value) =>
                        {
                            AsCtx(ref ctx).Builder.WriteKey(nameUtf8);
                            object? tempObject = null;
                            getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<object, byte>(ref tempObject));
                            serializer(ref ctx, ref Unsafe.As<object, byte>(ref tempObject));
                        };
                    }
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

    public static BonSerializerFactory Instance { get; } = new();

    public static void Serialize(ref BonBuilder builder, object? value)
    {
        var ctx = new BonSerializerCtx { Factory = Instance, Builder = ref builder };
        Instance.SerializeObject(ref AsCtx(ref ctx), ref Unsafe.As<object, byte>(ref value));
    }
}
