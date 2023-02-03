using System;
using System.Collections.Generic;
using System.Text;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace BTDB.EventStoreLayer;

public static class TypeDescriptorExtensions
{
    public static string Describe(this ITypeDescriptor descriptor)
    {
        var sb = new StringBuilder();
        descriptor.BuildHumanReadableFullName(sb,
            new HashSet<ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance), 0);
        return sb.ToString();
    }

    public static void GenerateSaveEx(this ITypeDescriptor descriptor, IILGen ilGenerator,
        Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushSubValue, Type subValueType)
    {
        if (descriptor.StoredInline)
        {
            descriptor.GenerateSave(ilGenerator, pushWriter, pushCtx, pushSubValue, subValueType);
        }
        else
        {
            ilGenerator
                .Do(pushCtx)
                .Do(pushWriter)
                .Do(pushSubValue)
                .Callvirt(typeof(ITypeBinarySerializerContext).GetMethod(
                    nameof(ITypeBinarySerializerContext.StoreObject))!);
        }
    }

    public static void GenerateSkipEx(this ITypeDescriptor descriptor, IILGen ilGenerator,
        Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        if (descriptor.StoredInline)
        {
            descriptor.GenerateSkip(ilGenerator, pushReader, pushCtx);
        }
        else
        {
            ilGenerator
                .Do(pushCtx)
                .Do(pushReader)
                .Callvirt(typeof(ITypeBinaryDeserializerContext).GetMethod(nameof(ITypeBinaryDeserializerContext
                    .SkipObject))!);
        }
    }

    public static void GenerateLoadEx(this ITypeDescriptor descriptor, IILGen ilGenerator,
        Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type asType,
        ITypeConvertorGenerator convertorGenerator)
    {
        if (descriptor.StoredInline)
        {
            if (descriptor.LoadNeedsHelpWithConversion && asType != typeof(object))
            {
                var origType = descriptor.GetPreferredType();
                descriptor.GenerateLoad(ilGenerator, pushReader, pushCtx, pushDescriptor, origType);
                if (origType != asType)
                {
                    var conv = convertorGenerator.GenerateConversion(origType, asType);
                    if (conv == null)
                        throw new BTDBException("Don't know how to convert " + descriptor.Name + " from " +
                                                origType.ToSimpleName() + " to " + asType.ToSimpleName());
                    conv(ilGenerator);
                }
            }
            else
            {
                descriptor.GenerateLoad(ilGenerator, pushReader, pushCtx, pushDescriptor, asType);
            }
        }
        else
        {
            ilGenerator
                .Do(pushCtx)
                .Do(pushReader)
                .Callvirt(typeof(ITypeBinaryDeserializerContext).GetMethod(nameof(ITypeBinaryDeserializerContext
                    .LoadObject))!);
            if (asType != typeof(object))
            {
                var origType = descriptor.GetPreferredType();
                var conv = convertorGenerator.GenerateConversion(origType, asType);
                if (conv != null)
                {
                    ilGenerator.Call(typeof(TypeDescriptorExtensions).GetMethod(nameof(IntelligentCast))!
                        .MakeGenericMethod(origType));
                    conv(ilGenerator);
                }
                else
                {
                    ilGenerator.Call(typeof(TypeDescriptorExtensions).GetMethod(nameof(IntelligentCast))!
                        .MakeGenericMethod(asType));
                }
            }
        }
    }

    public static T? IntelligentCast<T>(object? obj) where T : class
    {
        if (obj is T res) return res;
        if (obj == null) return null;
        if (obj is IIndirect indirect)
        {
            return indirect.ValueAsObject as T;
        }

        return (T)obj; // This will throw
    }

    public static StringBuilder AppendJsonLike(this StringBuilder sb, object? obj)
    {
        if (obj == null)
        {
            return sb.Append("null");
        }

        if (obj is string objString)
            return sb.Append('"').Append(objString.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\r", "\\r")
                .Replace("\n", "\\n")).Append('"');

        if (obj.GetType().IsEnum || obj is EnumTypeDescriptor.DynamicEnum || obj is bool ||
            obj is DateTime || obj is Guid)
            return sb.Append('"').Append(obj.ToString()).Append('"');

        return sb.Append(obj.ToString());
    }
}
