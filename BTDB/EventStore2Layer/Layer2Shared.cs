using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using BTDB.Encrypted;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.EventStore2Layer;

/// <summary>Parts shared by the metadata-log <see cref="EventSerializer"/>/<see cref="EventDeserializer"/> and the
/// catalog <see cref="CatalogEventSerializer"/>/<see cref="CatalogEventDeserializer"/>.</summary>
static class Layer2Shared
{
    internal enum TypeKind { Delegate, Dictionary, List, Indirect, Nullable, Tuple, Object, Enum, UnsupportedValueType }

    /// <summary>Which descriptor stores a CLR type. A dictionary, list, set, array or nullable is stored as its
    /// <paramref name="alternative"/> specialization, which shares the descriptor.</summary>
    internal static TypeKind KindOf(Type type, out Type? alternative)
    {
        alternative = null;
        if (type.IsSubclassOf(typeof(Delegate))) return TypeKind.Delegate;
        if (type.IsGenericType)
        {
            alternative = type.SpecializationOf(typeof(IDictionary<,>));
            if (alternative != null) return TypeKind.Dictionary;
            alternative = type.SpecializationOf(typeof(IList<>)) ?? type.SpecializationOf(typeof(ISet<>));
            if (alternative != null) return TypeKind.List;
            if (type.GetGenericTypeDefinition().InheritsOrImplements(typeof(IIndirect<>))) return TypeKind.Indirect;
            if (type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                alternative = type.SpecializationOf(typeof(Nullable<>));
                return alternative != null ? TypeKind.Nullable : TypeKind.Delegate;
            }
            return type.InheritsOrImplements(typeof(ITuple)) ? TypeKind.Tuple : TypeKind.Object;
        }
        if (type.IsArray)
        {
            alternative = type.SpecializationOf(typeof(IList<>));
            return TypeKind.List;
        }
        if (type.IsEnum) return TypeKind.Enum;
        return type.IsValueType ? TypeKind.UnsupportedValueType : TypeKind.Object;
    }

    /// <summary>A new descriptor of a type, not yet finished by <see cref="ITypeDescriptor.FinishBuildFromType"/>;
    /// null for <see cref="TypeKind.Indirect"/>.</summary>
    internal static ITypeDescriptor? NewDescriptor(ITypeDescriptorCallbacks callbacks, Type type, TypeKind kind,
        Type? alternative) => kind switch
    {
        TypeKind.Dictionary => new DictionaryTypeDescriptor(callbacks, alternative!),
        TypeKind.List => new ListTypeDescriptor(callbacks, alternative!),
        TypeKind.Indirect => null,
        TypeKind.Nullable => new NullableTypeDescriptor(callbacks, alternative!),
        TypeKind.Tuple => new TupleTypeDescriptor(callbacks, type),
        TypeKind.Object => new ObjectTypeDescriptor(callbacks, type, null),
        TypeKind.Enum => new EnumTypeDescriptor(callbacks, type),
        TypeKind.UnsupportedValueType => throw new BTDBException($"Unsupported value type {type.Name}."),
        _ => throw new BTDBException("Don't know how to serialize type " + type.ToSimpleName())
    };

    /// <summary>Built-in descriptors by the CLR types they store, as the metadata-log serializers register them.</summary>
    internal static readonly Dictionary<Type, ITypeDescriptor> BuiltInByType = CreateBuiltInByType();

    static Dictionary<Type, ITypeDescriptor> CreateBuiltInByType()
    {
        var result = new Dictionary<Type, ITypeDescriptor>(ReferenceEqualityComparer.Instance);
        foreach (var predefined in BasicSerializersFactory.TypeDescriptors)
        {
            result.TryAdd(predefined.GetPreferredType()!, predefined);
            if (predefined is ITypeDescriptorMultipleNativeTypes multiple)
                foreach (var type in multiple.GetNativeTypes())
                    result[type] = predefined;
        }
        return result;
    }

    internal static Layer1ComplexSaver BuildComplexSaver(ITypeDescriptor descriptor, Type type,
        ITypeConverterFactory converterFactory)
    {
        // A value that knows its descriptor, loaded without its type, has none of the members emitted code reads.
        if (IFieldHandler.UseNoEmitForDescriptors || typeof(IKnowDescriptor).IsAssignableFrom(type))
#pragma warning disable CS0162 // Unreachable code detected
        {
            var saver = typeof(object) == type
                ? descriptor.GenerateSave(type, converterFactory)
                : descriptor.BuildConvertingSaver(typeof(object), type, converterFactory);
            return (ref MemWriter writer, ITypeBinarySerializerContext ctx, object value) =>
            {
                saver(ref writer, ctx, ref Unsafe.As<object, byte>(ref value));
            };
        }
        else
        {
            var method =
                ILBuilder.Instance.NewMethod<Layer1ComplexSaver>(descriptor.Name + "Saver" + type.ToSimpleName());
            var il = method.Generator;
            descriptor.GenerateSave(il, ilgen => ilgen.Ldarg(0), ilgen => ilgen.Ldarg(1), ilgen =>
            {
                ilgen.Ldarg(2);
                if (type != typeof(object))
                {
                    ilgen.UnboxAny(type);
                }
            }, type);
            il.Ret();
            return method.Create();
        }
#pragma warning restore CS0162 // Unreachable code detected
    }

    /// <param name="loadAsType">The type to load as, already passed through <see cref="ObjectLoadType.For"/>.</param>
    internal static Layer2Loader BuildLoader(ITypeDescriptor descriptor, Type loadAsType,
        ITypeConverterFactory converterFactory)
    {
        if (IFieldHandler.UseNoEmitForDescriptors)
#pragma warning disable CS0162 // Unreachable code detected
        {
            var loader = loadAsType == typeof(object)
                ? descriptor.GenerateLoad(loadAsType, converterFactory)
                : descriptor
                    .BuildConvertingLoader(loadAsType, typeof(object), converterFactory);
            return (ref MemReader reader, ITypeBinaryDeserializerContext ctx, ITypeDescriptor typeDescriptor) =>
            {
                object res = null;
                loader(ref reader, ctx, ref Unsafe.As<object, byte>(ref res));
                return res;
            };
        }
        else
        {
            var methodBuilder = ILBuilder.Instance.NewMethod<Layer2Loader>("DeserializerFor" + descriptor.Name);
            var il = methodBuilder.Generator;
            try
            {
                descriptor.GenerateLoad(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldarg(1), ilGen => ilGen.Ldarg(2),
                    loadAsType);
            }
            catch (BTDBException ex)
            {
                throw new BTDBException("Deserialization of type " + loadAsType.FullName, ex);
            }

            if (loadAsType.IsValueType)
            {
                il.Box(loadAsType);
            }
            else if (loadAsType != typeof(object))
            {
                il.Castclass(typeof(object));
            }

            il.Ret();
            return methodBuilder.Create();
        }
#pragma warning restore CS0162 // Unreachable code detected
    }

    [SkipLocalsInit]
    internal static void StoreEncryptedString(ref MemWriter outerWriter, EncryptedString value,
        ISymmetricCipher symmetricCipher)
    {
        Span<byte> buf = stackalloc byte[2048];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        writer.WriteString(value);
        var plain = writer.GetSpan();
        var encSize = symmetricCipher.CalcEncryptedSizeFor(plain);
        outerWriter.WriteVUInt32((uint)encSize + 1);
        var enc = outerWriter.BlockWriteToSpan(encSize, out var needToBeWritten);
        symmetricCipher.Encrypt(plain, enc);
        if (needToBeWritten)
            outerWriter.WriteBlock(enc);
    }

    internal static unsafe EncryptedString LoadEncryptedString(ref MemReader reader, ISymmetricCipher symmetricCipher)
    {
        var enc = reader.ReadByteArrayAsSpan();
        var size = symmetricCipher.CalcPlainSizeFor(enc);
        var dec = size < 4096 ? stackalloc byte[size] : new byte[size];
        if (!symmetricCipher.Decrypt(enc, dec))
        {
            throw new CryptographicException();
        }

        fixed (void* _ = dec)
        {
            var r = MemReader.CreateFromPinnedSpan(dec);
            return r.ReadString();
        }
    }
}
