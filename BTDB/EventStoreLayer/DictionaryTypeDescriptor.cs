using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    internal class DictionaryTypeDescriptor : ITypeDescriptor, IPersistTypeDescriptor, ITypeBinarySerializerGenerator, ITypeBinarySkipperGenerator
    {
        readonly TypeSerializers _typeSerializers;
        Type _type;
        Type _keyType;
        Type _valueType;
        ITypeDescriptor _keyDescriptor;
        ITypeDescriptor _valueDescriptor;

        public DictionaryTypeDescriptor(TypeSerializers typeSerializers, Type type)
        {
            _typeSerializers = typeSerializers;
            _type = type;
            var genericArguments = type.GetGenericArguments();
            _keyType = genericArguments[0];
            _valueType = genericArguments[1];
        }

        public DictionaryTypeDescriptor(TypeSerializers typeSerializers, AbstractBufferedReader reader, Func<AbstractBufferedReader, ITypeDescriptor> nestedDescriptorReader)
        {
            _typeSerializers = typeSerializers;
            InitFromKeyValueDescriptors(nestedDescriptorReader(reader), nestedDescriptorReader(reader));
        }

        void InitFromKeyValueDescriptors(ITypeDescriptor keyDescriptor, ITypeDescriptor valueDescriptor)
        {
            if (_keyDescriptor == keyDescriptor && _valueDescriptor == valueDescriptor) return;
            _keyDescriptor = keyDescriptor;
            _valueDescriptor = valueDescriptor;
            _keyType = _keyDescriptor.GetPreferedType();
            _valueType = _valueDescriptor.GetPreferedType();
            Sealed = _keyDescriptor.Sealed && _valueDescriptor.Sealed;
            Name = string.Format("Dictionary<{0}, {1}>", _keyDescriptor.Name, _valueDescriptor.Name);
        }

        public bool Equals(ITypeDescriptor other)
        {
            return Equals(other, new HashSet<ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance));
        }

        public string Name { get; private set; }

        public void FinishBuildFromType(ITypeDescriptorFactory factory)
        {
            InitFromKeyValueDescriptors(factory.Create(_keyType), factory.Create(_valueType));
        }

        public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent)
        {
            text.Append("List<");
            _keyDescriptor.BuildHumanReadableFullName(text, stack, indent);
            text.Append(">");
        }

        public bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack)
        {
            var o = other as DictionaryTypeDescriptor;
            if (o == null) return false;
            return _keyDescriptor.Equals(o._keyDescriptor, stack) && _valueDescriptor.Equals(o._valueDescriptor, stack);
        }

        public Type GetPreferedType()
        {
            if (_type == null)
            {
                _keyType = _typeSerializers.LoadAsType(_keyDescriptor);
                _valueType = _typeSerializers.LoadAsType(_valueDescriptor);
                _type = typeof(IDictionary<,>).MakeGenericType(_keyType, _valueType);
            }
            return _type;
        }

        public bool LoadNeedsCtx()
        {
            return !_keyDescriptor.StoredInline || !_valueDescriptor.StoredInline
                || _keyDescriptor.LoadNeedsCtx()
                || _valueDescriptor.LoadNeedsCtx();
        }

        public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type targetType)
        {
            var localCount = ilGenerator.DeclareLocal(typeof(int));
            var keyType = _typeSerializers.LoadAsType(_keyDescriptor);
            var valueType = _typeSerializers.LoadAsType(_valueDescriptor);
            var dictionaryType = typeof(DictionaryWithDescriptor<,>).MakeGenericType(keyType, valueType);
            if (!targetType.IsAssignableFrom(dictionaryType)) throw new InvalidOperationException();
            var localDict = ilGenerator.DeclareLocal(dictionaryType);
            var loadFinished = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            ilGenerator
                .Do(pushReader)
                .Callvirt(() => default(AbstractBufferedReader).ReadVUInt32())
                .ConvI4()
                .Dup()
                .LdcI4(1)
                .Sub()
                .Stloc(localCount)
                .Brfalse(loadFinished)
                .Ldloc(localCount)
                .Do(pushDescriptor)
                .Newobj(dictionaryType.GetConstructor(new[] { typeof(int), typeof(ITypeDescriptor) }))
                .Stloc(localDict)
                .Mark(next)
                .Ldloc(localCount)
                .Brfalse(loadFinished)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .Stloc(localCount)
                .Ldloc(localDict);
            _keyDescriptor.GenerateLoadEx(ilGenerator, pushReader, pushCtx, il => il.Do(pushDescriptor).LdcI4(0).Callvirt(() => default(ITypeDescriptor).NestedType(0)), keyType);
            _valueDescriptor.GenerateLoadEx(ilGenerator, pushReader, pushCtx, il => il.Do(pushDescriptor).LdcI4(1).Callvirt(() => default(ITypeDescriptor).NestedType(0)), valueType);
            ilGenerator
                .Callvirt(dictionaryType.GetMethod("Add"))
                .Br(next)
                .Mark(loadFinished)
                .Ldloc(localDict)
                .Castclass(targetType);
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
            if (_keyDescriptor.Sealed && _valueDescriptor.Sealed) return null;
            return new TypeNewDescriptorGenerator(this);
        }

        class TypeNewDescriptorGenerator : ITypeNewDescriptorGenerator
        {
            readonly DictionaryTypeDescriptor _owner;

            public TypeNewDescriptorGenerator(DictionaryTypeDescriptor owner)
            {
                _owner = owner;
            }

            public void GenerateTypeIterator(IILGen ilGenerator, Action<IILGen> pushObj, Action<IILGen> pushCtx)
            {
                var finish = ilGenerator.DefineLabel();
                var next = ilGenerator.DefineLabel();
                var keyType = _owner._typeSerializers.LoadAsType(_owner._keyDescriptor);
                var valueType = _owner._typeSerializers.LoadAsType(_owner._valueDescriptor);
                var typeAsIDictionary = typeof(IDictionary<,>).MakeGenericType(keyType, valueType);
                var typeAsICollection = typeAsIDictionary.GetInterface("ICollection`1");
                var typeAsIEnumerable = typeAsIDictionary.GetInterface("IEnumerable`1");
                var getEnumeratorMethod = typeAsIEnumerable.GetMethod("GetEnumerator");
                var typeAsIEnumerator = getEnumeratorMethod.ReturnType;
                var typeKeyValuePair = typeAsICollection.GetGenericArguments()[0];
                var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
                var localPair = ilGenerator.DeclareLocal(typeKeyValuePair);
                ilGenerator
                    .Do(pushObj)
                    .Castclass(typeAsIDictionary)
                    .Callvirt(getEnumeratorMethod)
                    .Stloc(localEnumerator)
                    .Try()
                    .Mark(next)
                    .Ldloc(localEnumerator)
                    .Callvirt(() => default(IEnumerator).MoveNext())
                    .Brfalse(finish)
                    .Ldloc(localEnumerator)
                    .Callvirt(typeAsIEnumerator.GetProperty("Current").GetGetMethod())
                    .Stloc(localPair);
                if (!_owner._keyDescriptor.Sealed)
                {
                    ilGenerator
                        .Do(pushCtx)
                        .Ldloca(localPair)
                        .Call(typeKeyValuePair.GetProperty("Key").GetGetMethod())
                        .Callvirt(() => default(IDescriptorSerializerLiteContext).StoreNewDescriptors(null));
                }
                if (!_owner._valueDescriptor.Sealed)
                {
                    ilGenerator
                        .Do(pushCtx)
                        .Ldloca(localPair)
                        .Call(typeKeyValuePair.GetProperty("Value").GetGetMethod())
                        .Callvirt(() => default(IDescriptorSerializerLiteContext).StoreNewDescriptors(null));
                }
                ilGenerator
                    .Br(next)
                    .Mark(finish)
                    .Finally()
                    .Ldloc(localEnumerator)
                    .Callvirt(() => default(IDisposable).Dispose())
                    .EndTry();
            }
        }

        public ITypeDescriptor NestedType(int index)
        {
            if (index == 0) return _keyDescriptor;
            if (index == 1) return _valueDescriptor;
            return null;
        }

        public void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
        {
            InitFromKeyValueDescriptors(map(_keyDescriptor), map(_valueDescriptor));
        }

        public bool Sealed { get; private set; }
        public bool StoredInline { get { return true; } }
        public void ClearMappingToType()
        {
            _type = null;
            _keyType = null;
            _valueType = null;
        }

        public bool ContainsField(string name)
        {
            return false;
        }

        public void Persist(AbstractBufferedWriter writer, Action<AbstractBufferedWriter, ITypeDescriptor> nestedDescriptorPersistor)
        {
            nestedDescriptorPersistor(writer, _keyDescriptor);
            nestedDescriptorPersistor(writer, _valueDescriptor);
        }

        public bool SaveNeedsCtx()
        {
            return !_keyDescriptor.StoredInline || !_valueDescriptor.StoredInline
                || _keyDescriptor.BuildBinarySerializerGenerator().SaveNeedsCtx()
                || _valueDescriptor.BuildBinarySerializerGenerator().SaveNeedsCtx();
        }

        public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue, Type saveType)
        {
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            var notnull = ilGenerator.DefineLabel();
            var completeFinish = ilGenerator.DefineLabel();
            var keyType = saveType.GetGenericArguments()[0];
            var valueType = saveType.GetGenericArguments()[1];
            var typeAsIDictionary = typeof(IDictionary<,>).MakeGenericType(keyType, valueType);
            var typeAsICollection = typeAsIDictionary.GetInterface("ICollection`1");
            var typeAsIEnumerable = typeAsIDictionary.GetInterface("IEnumerable`1");
            var getEnumeratorMethod = typeAsIEnumerable.GetMethod("GetEnumerator");
            var typeAsIEnumerator = getEnumeratorMethod.ReturnType;
            var typeKeyValuePair = typeAsICollection.GetGenericArguments()[0];
            var localDict = ilGenerator.DeclareLocal(typeAsIDictionary);
            var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
            var localPair = ilGenerator.DeclareLocal(typeKeyValuePair);
            ilGenerator
                .Do(pushValue)
                .Castclass(typeAsIDictionary)
                .Dup()
                .Stloc(localDict)
                .Brtrue(notnull)
                .Do(pushWriter)
                .LdcI4(0)
                .Callvirt(() => default(AbstractBufferedWriter).WriteVUInt32(0))
                .Br(completeFinish)
                .Mark(notnull)
                .Do(pushWriter)
                .Ldloc(localDict)
                .Callvirt(typeAsICollection.GetProperty("Count").GetGetMethod())
                .LdcI4(1)
                .Add()
                .ConvU4()
                .Callvirt(() => default(AbstractBufferedWriter).WriteVUInt32(0))
                .Ldloc(localDict)
                .Callvirt(getEnumeratorMethod)
                .Stloc(localEnumerator)
                .Try()
                .Mark(next)
                .Ldloc(localEnumerator)
                .Callvirt(() => default(IEnumerator).MoveNext())
                .Brfalse(finish)
                .Ldloc(localEnumerator)
                .Callvirt(typeAsIEnumerator.GetProperty("Current").GetGetMethod())
                .Stloc(localPair);
            _keyDescriptor.GenerateSave(ilGenerator, pushWriter, pushCtx, il => il.Ldloca(localPair).Call(typeKeyValuePair.GetProperty("Key").GetGetMethod()), keyType);
            _valueDescriptor.GenerateSave(ilGenerator, pushWriter, pushCtx, il => il.Ldloca(localPair).Call(typeKeyValuePair.GetProperty("Value").GetGetMethod()), valueType);
            ilGenerator
                .Br(next)
                .Mark(finish)
                .Finally()
                .Ldloc(localEnumerator)
                .Callvirt(() => default(IDisposable).Dispose())
                .EndTry()
                .Mark(completeFinish);
        }

        public bool SkipNeedsCtx()
        {
            return !_keyDescriptor.StoredInline || !_valueDescriptor.StoredInline
                || _keyDescriptor.BuildBinarySkipperGenerator().SkipNeedsCtx()
                || _valueDescriptor.BuildBinarySkipperGenerator().SkipNeedsCtx();
        }

        public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
        {
            var localCount = ilGenerator.DeclareLocal(typeof(int));
            var skipFinished = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            ilGenerator
                .Do(pushReader)
                .Callvirt(() => default(AbstractBufferedReader).ReadVUInt32())
                .ConvI4()
                .Dup()
                .LdcI4(1)
                .Sub()
                .Stloc(localCount)
                .Brfalse(skipFinished)
                .Mark(next)
                .Ldloc(localCount)
                .Brfalse(skipFinished)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .Stloc(localCount);
            _keyDescriptor.GenerateSkip(ilGenerator, pushReader, pushCtx);
            _valueDescriptor.GenerateSkip(ilGenerator, pushReader, pushCtx);
            ilGenerator
                .Br(next)
                .Mark(skipFinished);
        }
    }
}