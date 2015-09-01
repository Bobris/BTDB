using System;
using System.Collections.Generic;
using System.Text;
using BTDB.Buffer;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public class ByteArrayTypeDescriptor : ITypeDescriptorMultipleNativeTypes
    {
        public string Name
        {
            get { return "Byte[]"; }
        }

        public void FinishBuildFromType(ITypeDescriptorFactory factory)
        {
        }

        public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent)
        {
            text.Append(Name);
        }

        public bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack)
        {
            return ReferenceEquals(this, other);
        }

        public Type GetPreferedType()
        {
            return typeof(byte[]);
        }

        public ITypeNewDescriptorGenerator BuildNewDescriptorGenerator()
        {
            return null;
        }

        public ITypeDescriptor NestedType(int index)
        {
            return null;
        }

        public void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
        {
        }

        public bool Sealed { get { return true; } }

        public bool StoredInline { get { return true; } }

        public void ClearMappingToType()
        {
        }

        public bool ContainsField(string name)
        {
            return false;
        }

        public IEnumerable<Type> GetNativeTypes()
        {
            yield return typeof (ByteBuffer);
        }

        public bool AnyOpNeedsCtx()
        {
            return false;
        }

        public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type targetType)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(() => default(AbstractBufferedReader).ReadByteArray());
            if (targetType == typeof (ByteBuffer))
            {
                ilGenerator.Call(() => ByteBuffer.NewAsync(null));
                return;
            }
            if (targetType != typeof (object))
            {
                if (targetType!=typeof(byte[]))
                    throw new ArgumentOutOfRangeException(nameof(targetType));
                return;
            }
            ilGenerator.Castclass(typeof(object));
        }

        public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(() => default(AbstractBufferedReader).SkipByteArray());
        }

        public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue, Type valueType)
        {
            pushWriter(ilGenerator);
            pushValue(ilGenerator);
            if (valueType==typeof(byte[]))
                ilGenerator.Call(() => default(AbstractBufferedWriter).WriteByteArray(null));
            else if (valueType==typeof(ByteBuffer))
                ilGenerator.Call(() => default(AbstractBufferedWriter).WriteByteArray(ByteBuffer.NewEmpty()));
            else throw new ArgumentOutOfRangeException(nameof(valueType));
        }

        public bool Equals(ITypeDescriptor other)
        {
            return ReferenceEquals(this, other);
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }
    }
}