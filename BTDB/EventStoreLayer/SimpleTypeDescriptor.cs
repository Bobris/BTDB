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

        public SimpleTypeDescriptor(MethodInfo loader, MethodInfo skipper, MethodInfo saver)
        {
            _loader = loader;
            _skipper = skipper;
            _saver = saver;
        }

        public abstract string Name { get; }

        public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack)
        {
            text.Append(Name);
        }

        public ITypeBinaryDeserializerGenerator BuildBinaryDeserializerGenerator(Type target)
        {
            var realType = _loader.ReturnType;
            if (realType == target) return this;
            if (target == typeof(object))
            {
                return new BoxingDeserializerGenerator(this,realType);
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

            public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
            {
                _typeBinaryDeserializerGenerator.GenerateLoad(ilGenerator,pushReaderOrCtx);
                if (_realType.IsValueType)
                {
                    ilGenerator.Box(_realType);
                }
                else
                {
                    ilGenerator.Castclass(typeof (object));
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

        public ITypeDynamicTypeIterationGenerator BuildDynamicTypeIterationGenerator()
        {
            return null;
        }

        public IEnumerable<ITypeDescriptor> NestedTypes()
        {
            yield break;
        }

        public bool LoadNeedsCtx()
        {
            return false;
        }

        public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(_loader);
        }

        public bool SkipNeedsCtx()
        {
            return false;
        }

        public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(_skipper);
        }

        public bool SaveNeedsCtx()
        {
            return false;
        }

        public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
        {
            pushWriterOrCtx(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(_saver);
        }
    }
}