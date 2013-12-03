using System;
using System.Collections.Generic;
using System.Text;
using BTDB.IL;
using BTDB.ODBLayer;

namespace BTDB.EventStoreLayer
{
    public static class TypeDescriptorExtensions
    {
        public static string Describe(this ITypeDescriptor descriptor)
        {
            var sb = new StringBuilder();
            descriptor.BuildHumanReadableFullName(sb,new HashSet<ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance), 0);
            return sb.ToString();
        }

        public static void GenerateSave(this ITypeDescriptor descriptor, IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushSubValue, Type subValueType)
        {
            if (descriptor.StoredInline)
            {
                var generator = descriptor.BuildBinarySerializerGenerator();
                generator.GenerateSave(ilGenerator, pushWriter, pushCtx, pushSubValue, subValueType);
            }
            else
            {
                ilGenerator
                    .Do(pushCtx)
                    .Do(pushSubValue)
                    .Callvirt(() => default(ITypeBinarySerializerContext).StoreObject(null));
            }
        }

        public static void GenerateSkip(this ITypeDescriptor descriptor, IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
        {
            if (descriptor.StoredInline)
            {
                var skipper = descriptor.BuildBinarySkipperGenerator();
                skipper.GenerateSkip(ilGenerator, pushReader, pushCtx);
            }
            else
            {
                ilGenerator
                    .Do(pushCtx)
                    .Callvirt(() => default(ITypeBinaryDeserializerContext).SkipObject());
            }
        }

        public static void GenerateLoadEx(this ITypeDescriptor descriptor, IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type asType)
        {
            if (descriptor.StoredInline)
            {
                descriptor.GenerateLoad(ilGenerator, pushReader, pushCtx, pushDescriptor, asType);
            }
            else
            {
                ilGenerator
                    .Do(pushCtx)
                    .Callvirt(() => default(ITypeBinaryDeserializerContext).LoadObject());
                if (asType != typeof(object))
                    ilGenerator.Castclass(asType);
            }
        }

        public static StringBuilder AppendJsonLike(this StringBuilder sb, object obj)
        {
            if (obj==null)
            {
                return sb.Append("null");
            }
            var str = obj as string;
            if (str != null)
            {
                return sb.Append('"').Append(str).Append('"');
            }
// ReSharper disable RedundantToStringCall it is speed optimization
            return sb.Append(obj.ToString());
// ReSharper restore RedundantToStringCall
        }
    }
}