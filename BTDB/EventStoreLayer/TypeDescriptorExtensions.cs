using System;
using System.Collections.Generic;
using System.Text;
using BTDB.IL;
using BTDB.ODBLayer;

namespace BTDB.EventStoreLayer
{
    public static class TypeDescriptorExtensions
    {
        public static string Describe(this ITypeDescriptor typeDescriptor)
        {
            var sb = new StringBuilder();
            typeDescriptor.BuildHumanReadableFullName(sb,new HashSet<ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance), 0);
            return sb.ToString();
        }

        public static void GenerateSave(this ITypeDescriptor typeDescriptor, IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushSubValue)
        {
            if (typeDescriptor.StoredInline)
            {
                var generator = typeDescriptor.BuildBinarySerializerGenerator();
                generator.GenerateSave(ilGenerator, pushWriter, pushCtx, pushSubValue);
            }
            else
            {
                ilGenerator
                    .Do(pushCtx)
                    .Do(pushSubValue)
                    .Callvirt(() => default(ITypeBinarySerializerContext).StoreObject(null));
            }
        }

        public static void GenerateSkip(this ITypeDescriptor itemDescriptor, IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
        {
            if (itemDescriptor.StoredInline)
            {
                var skipper = itemDescriptor.BuildBinarySkipperGenerator();
                skipper.GenerateSkip(ilGenerator, pushReader, pushCtx);
            }
            else
            {
                ilGenerator
                    .Do(pushCtx)
                    .Callvirt(() => default(ITypeBinaryDeserializerContext).SkipObject());
            }
        }

        public static void GenerateLoad(this ITypeDescriptor dictionaryTypeDescriptor, IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type asType)
        {
            if (dictionaryTypeDescriptor.StoredInline)
            {
                var des = dictionaryTypeDescriptor.BuildBinaryDeserializerGenerator(asType);
                des.GenerateLoad(ilGenerator, pushReader, pushCtx, pushDescriptor);
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