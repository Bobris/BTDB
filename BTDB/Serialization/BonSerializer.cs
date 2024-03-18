using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using BTDB.Bon;
using BTDB.IL;

namespace BTDB.Serialization;

public delegate void Serialize(ref SerializerCtx ctx, ref byte value);

public delegate void Deserialize(ref DeserializerCtx ctx, ref byte value);

public interface ISerializerFactory
{
    void SerializeObject(ref SerializerCtx ctx, object? value);
    Serialize CreateSerializerForType(Type type);
    object? DeserializeObject(ref DeserializerCtx ctx);
    Deserialize CreateDeserializerForType(Type type);
}

public ref struct SerializerCtx
{
    public ISerializerFactory Factory;
}

public ref struct DeserializerCtx
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
    public ref KeyedBon KeyedBon;
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

        var type = obj.GetType();
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
            serializer!(ref ctx, ref Unsafe.As<object, byte>(ref obj));
        }
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

    static unsafe ref BonDeserializerCtx AsCtx(ref DeserializerCtx ctx)
    {
#pragma warning disable CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        fixed (void* ptr = &ctx)
        {
            return ref *(BonDeserializerCtx*)ptr;
        }
#pragma warning restore CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
    }

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
        if (!type.IsValueType && type != typeof(byte[]) && type != typeof(string))
        {
            return (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Factory.SerializeObject(ref ctx, Unsafe.As<byte, object>(ref value));
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

        if (type == typeof(byte[]))
        {
            return static (ref SerializerCtx ctx, ref byte value) =>
            {
                AsCtx(ref ctx).Builder.Write(Unsafe.As<byte, byte[]>(ref value));
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
                builder.StartTuple();
                serializer0(ref ctx, ref Unsafe.AddByteOffset(ref value, offsets.Item1));
                serializer1(ref ctx, ref Unsafe.AddByteOffset(ref value, offsets.Item2));
                builder.FinishTuple();
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
                builder.StartTuple();
                serializer0(ref ctx, ref RawData.Ref(Unsafe.As<byte, object>(ref value), offsets.Item1));
                serializer1(ref ctx, ref RawData.Ref(Unsafe.As<byte, object>(ref value), offsets.Item2));
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

    public Deserialize CreateCachedDeserializerForType(Type type)
    {
        if (!type.IsValueType && type != typeof(byte[]) && type != typeof(string))
        {
            return (ref DeserializerCtx ctx, ref byte value) =>
            {
                Unsafe.As<byte, object>(ref value) = AsCtx(ref ctx).Factory.DeserializeObject(ref ctx);
            };
        }

        var typePtr = type.TypeHandle.Value;
        _cache2.TryGetValue(typePtr, out var deserializer);
        if (deserializer == null)
        {
            deserializer = CreateDeserializerForType(type);
            _cache2.TryAdd(typePtr, deserializer);
            _cache2.TryGetValue(typePtr, out deserializer);
        }

        return deserializer!;
    }

    public object? DeserializeObject(ref DeserializerCtx ctx)
    {
        switch (AsCtx(ref ctx).Bon.BonType)
        {
            case BonType.Null:
            case BonType.Undefined:
                return null;
            case BonType.Class:
            {
                AsCtx(ref ctx).Bon.TryGetClass(out AsCtx(ref ctx).KeyedBon, out var name);
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
                AsCtx(ref ctx).Bon.TryGetArray(out var arrayBon);
                arrayBon.TryGet(0, out var itemBon);
                var count = arrayBon.Items;
                if (count == 2)
                {
                    BonDeserializerCtx subCtx = new() { Factory = AsCtx(ref ctx).Factory, Bon = ref itemBon };
                    var i0 = AsCtx(ref ctx).Factory.DeserializeObject(ref AsCtx(ref subCtx));
                    var i1 = AsCtx(ref ctx).Factory.DeserializeObject(ref AsCtx(ref subCtx));
                    var res = new Tuple<object?, object?>(i0, i1);
                    return res;
                }

                throw new NotSupportedException("Tuple with " + count + " items is not supported.");
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
                AsCtx(ref ctx).Bon.TryGetLong(out var res);
                return res;
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
        if (ReflectionMetadata.FindByName(name) is { } classMetadata)
        {
            var type = classMetadata.Type;
            var typePtr = type.TypeHandle.Value;
            _cache2.TryGetValue(typePtr, out var deserializer);
            if (deserializer == null)
            {
                deserializer = CreateDeserializerForType(type);
                _cache2.TryAdd(typePtr, deserializer);
                _cache2.TryGetValue(typePtr, out deserializer);
            }

            return deserializer!;
        }

        return (ref DeserializerCtx ctx, ref byte value) => { AsCtx(ref ctx).Bon.Skip(); };
    }

    public Deserialize CreateDeserializerForType(Type type)
    {
        if (type == typeof(string))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (!AsCtx(ref ctx).Bon.TryGetString(out Unsafe.As<byte, string>(ref value)))
                    AsCtx(ref ctx).Bon.Skip();
            };
        }

        if (type == typeof(byte))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetULong(out var v))
                {
                    value = (byte)v;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                }
            };
        }

        if (type == typeof(sbyte))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetLong(out var v))
                {
                    Unsafe.As<byte, sbyte>(ref value) = (sbyte)v;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                }
            };
        }

        if (type == typeof(ushort))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetULong(out var v))
                {
                    Unsafe.As<byte, ushort>(ref value) = (ushort)v;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                }
            };
        }

        if (type == typeof(short))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetLong(out var v))
                {
                    Unsafe.As<byte, short>(ref value) = (short)v;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                }
            };
        }

        if (type == typeof(uint))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetULong(out var v))
                {
                    Unsafe.As<byte, uint>(ref value) = (uint)v;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                }
            };
        }

        if (type == typeof(int))
        {
            return static (ref DeserializerCtx ctx, ref byte value) =>
            {
                if (AsCtx(ref ctx).Bon.TryGetLong(out var v))
                {
                    Unsafe.As<byte, int>(ref value) = (int)v;
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
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
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
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
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
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
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
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
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
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
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
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
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
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
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
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
                }
                else
                {
                    AsCtx(ref ctx).Bon.Skip();
                }
            };
        }

        if (type.IsEnum)
        {
            return CreateDeserializerForType(Enum.GetUnderlyingType(type));
        }

        throw new NotImplementedException();
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
}
