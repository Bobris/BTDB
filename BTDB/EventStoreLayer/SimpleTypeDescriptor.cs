using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using BTDB.IL;

namespace BTDB.EventStoreLayer
{
    public abstract class SimpleTypeDescriptor : ITypeDescriptor,
                                                 ITypeBinaryDeserializerGenerator, ITypeBinarySkipperGenerator, ITypeBinarySerializerGenerator
    {
        readonly MethodInfo _loader;
        readonly MethodInfo _skipper;
        readonly MethodInfo _saver;

        protected SimpleTypeDescriptor(MethodInfo loader, MethodInfo skipper, MethodInfo saver)
        {
            _loader = loader;
            _skipper = skipper;
            _saver = saver;
        }

        public abstract string Name { get; }

        public void FinishBuildFromType(ITypeDescriptorFactory factory)
        {
        }

        public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack)
        {
            text.Append(Name);
        }

        public bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack)
        {
            return ReferenceEquals(this, other);
        }

        public Type GetPreferedType()
        {
            return _loader.ReturnType;
        }

        public ITypeBinaryDeserializerGenerator BuildBinaryDeserializerGenerator(Type target)
        {
            var realType = _loader.ReturnType;
            if (realType == target) return this;
            if (target == typeof(object))
            {
                return new BoxingDeserializerGenerator(this, realType);
            }
            return null;
        }

        class BoxingDeserializerGenerator : ITypeBinaryDeserializerGenerator
        {
            readonly ITypeBinaryDeserializerGenerator _typeBinaryDeserializerGenerator;
            readonly Type _realType;

            public BoxingDeserializerGenerator(ITypeBinaryDeserializerGenerator typeBinaryDeserializerGenerator, Type realType)
            {
                _typeBinaryDeserializerGenerator = typeBinaryDeserializerGenerator;
                _realType = realType;
            }

            public bool LoadNeedsCtx()
            {
                return _typeBinaryDeserializerGenerator.LoadNeedsCtx();
            }

            public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
            {
                _typeBinaryDeserializerGenerator.GenerateLoad(ilGenerator, pushReader, pushCtx);
                if (_realType.IsValueType)
                {
                    ilGenerator.Box(_realType);
                }
                else
                {
                    ilGenerator.Castclass(typeof(object));
                }
            }
        }

        public ITypeBinarySkipperGenerator BuildBinarySkipperGenerator()
        {
            return this;
        }

        public ITypeBinarySerializerGenerator BuildBinarySerializerGenerator()
        {
            return this;
        }

        public ITypeNewDescriptorGenerator BuildNewDescriptorGenerator()
        {
            return null;
        }

        public IEnumerable<ITypeDescriptor> NestedTypes()
        {
            yield break;
        }

        public void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
        {
        }

        public bool Sealed { get { return true; } }

        public bool StoredInline { get { return true; } }

        public void ClearMappingToType()
        {
        }

        public bool LoadNeedsCtx()
        {
            return false;
        }

        public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(_loader);
        }

        public bool SkipNeedsCtx()
        {
            return false;
        }

        public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(_skipper);
        }

        public bool SaveNeedsCtx()
        {
            return false;
        }

        public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue)
        {
            pushWriter(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(_saver);
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