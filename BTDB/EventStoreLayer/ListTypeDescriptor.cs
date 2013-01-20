using System;
using System.Collections.Generic;
using System.Text;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    internal class ListTypeDescriptor : ITypeDescriptor, IPersistTypeDescriptor, ITypeBinarySerializerGenerator, ITypeBinarySkipperGenerator
    {
        readonly TypeSerializers _typeSerializers;
        Type _type;
        Type _itemType;
        ITypeDescriptor _itemDescriptor;

        public ListTypeDescriptor(TypeSerializers typeSerializers, Type type)
        {
            _typeSerializers = typeSerializers;
            _type = type;
            _itemType = type.GetGenericArguments()[0];
        }

        public ListTypeDescriptor(TypeSerializers typeSerializers, AbstractBufferedReader reader, Func<AbstractBufferedReader, ITypeDescriptor> nestedDescriptorReader)
        {
            _typeSerializers = typeSerializers;
            InitFromItemDescriptor(nestedDescriptorReader(reader));
        }

        void InitFromItemDescriptor(ITypeDescriptor descriptor)
        {
            if (descriptor == _itemDescriptor) return;
            _itemDescriptor = descriptor;
            _itemType = _itemDescriptor.GetPreferedType();
            Sealed = _itemDescriptor.Sealed;
            Name = "List<" + _itemDescriptor.Name + ">";
        }

        public bool Equals(ITypeDescriptor other)
        {
            return Equals(other, new HashSet<ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance));
        }

        public string Name { get; private set; }

        public void FinishBuildFromType(ITypeDescriptorFactory factory)
        {
            InitFromItemDescriptor(factory.Create(_itemType));
        }

        public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack)
        {
            text.Append("List<");
            _itemDescriptor.BuildHumanReadableFullName(text, stack);
            text.Append(">");
        }

        public bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack)
        {
            var o = other as ListTypeDescriptor;
            if (o == null) return false;
            return _itemDescriptor.Equals(o._itemDescriptor, stack);
        }

        public Type GetPreferedType()
        {
            if (_type == null)
            {
                _itemType = _typeSerializers.LoadAsType(_itemDescriptor);
                _type = typeof(IList<>).MakeGenericType(_itemType);
            }
            return _type;
        }

        public ITypeBinaryDeserializerGenerator BuildBinaryDeserializerGenerator(Type target)
        {
            return new Deserializer(this, target);
        }

        class Deserializer : ITypeBinaryDeserializerGenerator
        {
            readonly ListTypeDescriptor _owner;
            readonly Type _target;

            public Deserializer(ListTypeDescriptor owner, Type target)
            {
                _owner = owner;
                _target = target;
            }

            public bool LoadNeedsCtx()
            {
                return !_owner._itemDescriptor.StoredInline || _owner._itemDescriptor.BuildBinaryDeserializerGenerator(_owner._itemDescriptor.GetPreferedType()).LoadNeedsCtx();
            }

            public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
            {
                var localCount = ilGenerator.DeclareLocal(typeof(int));
                var itemType = _owner._typeSerializers.LoadAsType(_owner._itemDescriptor);
                var listType = typeof(List<>).MakeGenericType(itemType);
                if (!_target.IsAssignableFrom(listType)) throw new NotImplementedException();
                var localList = ilGenerator.DeclareLocal(listType);
                var loadFinished = ilGenerator.DefineLabel();
                var next = ilGenerator.DefineLabel();
                ilGenerator
                    .Do(pushReader)
                    .Callvirt(() => default(AbstractBufferedReader).ReadVUInt32())
                    .ConvI4()
                    .Dup()
                    .Stloc(localCount)
                    .Brfalse(loadFinished)
                    .Ldloc(localCount)
                    .LdcI4(1)
                    .Sub()
                    .Dup()
                    .Stloc(localCount)
                    .Newobj(listType.GetConstructor(new[] { typeof(int) }))
                    .Stloc(localList)
                    .Mark(next)
                    .Ldloc(localCount)
                    .Brfalse(loadFinished)
                    .Ldloc(localCount)
                    .LdcI4(1)
                    .Sub()
                    .Stloc(localCount)
                    .Ldloc(localList);
                _owner._itemDescriptor.GenerateLoad(ilGenerator, pushReader, pushCtx, itemType);
                ilGenerator
                    .Callvirt(listType.GetInterface("ICollection`1").GetMethod("Add"))
                    .Br(next)
                    .Mark(loadFinished)
                    .Ldloc(localList)
                    .Castclass(_target);
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
            if (_itemDescriptor.Sealed) return null;
            return new TypeNewDescriptorGenerator(this);
        }

        class TypeNewDescriptorGenerator : ITypeNewDescriptorGenerator
        {
            readonly ListTypeDescriptor _listTypeDescriptor;

            public TypeNewDescriptorGenerator(ListTypeDescriptor listTypeDescriptor)
            {
                _listTypeDescriptor = listTypeDescriptor;
            }

            public void GenerateTypeIterator(IILGen ilGenerator, Action<IILGen> pushObj, Action<IILGen> pushCtx)
            {
                var finish = ilGenerator.DefineLabel();
                var next = ilGenerator.DefineLabel();
                var itemType = _listTypeDescriptor._typeSerializers.LoadAsType(_listTypeDescriptor._itemDescriptor);
                var localList = ilGenerator.DeclareLocal(typeof(IList<>).MakeGenericType(itemType));
                var localIndex = ilGenerator.DeclareLocal(typeof(int));
                var localCount = ilGenerator.DeclareLocal(typeof(int));
                ilGenerator
                    .Do(pushObj)
                    .Castclass(localList.LocalType)
                    .Dup()
                    .Stloc(localList)
                    .Callvirt(localList.LocalType.GetInterface("ICollection`1").GetProperty("Count").GetGetMethod())
                    .Stloc(localCount)
                    .Mark(next)
                    .Ldloc(localIndex)
                    .Ldloc(localCount)
                    .BgeUn(finish)
                    .Do(pushCtx)
                    .Ldloc(localList)
                    .Ldloc(localIndex)
                    .Callvirt(localList.LocalType.GetMethod("get_Item"))
                    .Callvirt(() => default(IDescriptorSerializerLiteContext).StoreNewDescriptors(null))
                    .Ldloc(localIndex)
                    .LdcI4(1)
                    .Add()
                    .Stloc(localIndex)
                    .Br(next)
                    .Mark(finish);
            }
        }

        public IEnumerable<ITypeDescriptor> NestedTypes()
        {
            yield return _itemDescriptor;
        }

        public void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
        {
            InitFromItemDescriptor(map(_itemDescriptor));
        }

        public bool Sealed { get; private set; }
        public bool StoredInline { get { return true; } }
        public void ClearMappingToType()
        {
            _type = null;
            _itemType = null;
        }

        public void Persist(AbstractBufferedWriter writer, Action<AbstractBufferedWriter, ITypeDescriptor> nestedDescriptorPersistor)
        {
            nestedDescriptorPersistor(writer, _itemDescriptor);
        }

        public bool SaveNeedsCtx()
        {
            return !_itemDescriptor.StoredInline || _itemDescriptor.BuildBinarySerializerGenerator().SaveNeedsCtx();
        }

        public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue)
        {
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            var notnull = ilGenerator.DefineLabel();
            var localList = ilGenerator.DeclareLocal(
                typeof(IList<>).MakeGenericType(_itemDescriptor.GetPreferedType()));
            var localIndex = ilGenerator.DeclareLocal(typeof(int));
            var localCount = ilGenerator.DeclareLocal(typeof(int));
            ilGenerator
                .Do(pushValue)
                .Castclass(localList.LocalType)
                .Dup()
                .Stloc(localList)
                .BrtrueS(notnull)
                .Do(pushWriter)
                .LdcI4(0)
                .Callvirt(() => default(AbstractBufferedWriter).WriteVUInt32(0))
                .Br(finish)
                .Mark(notnull)
                .Ldloc(localList)
                .Callvirt(localList.LocalType.GetInterface("ICollection`1").GetProperty("Count").GetGetMethod())
                .Stloc(localCount)
                .Do(pushWriter)
                .Ldloc(localCount)
                .LdcI4(1)
                .Add()
                .ConvU4()
                .Callvirt(() => default(AbstractBufferedWriter).WriteVUInt32(0))
                .Mark(next)
                .Ldloc(localIndex)
                .Ldloc(localCount)
                .BgeUn(finish);
            _itemDescriptor.GenerateSave(ilGenerator, pushWriter, pushCtx,
                il => il.Ldloc(localList)
                        .Ldloc(localIndex)
                        .Callvirt(localList.LocalType.GetMethod("get_Item")));
            ilGenerator
                .Ldloc(localIndex)
                .LdcI4(1)
                .Add()
                .Stloc(localIndex)
                .Br(next)
                .Mark(finish);
        }

        public bool SkipNeedsCtx()
        {
            return !_itemDescriptor.StoredInline || _itemDescriptor.BuildBinarySkipperGenerator().SkipNeedsCtx();
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
                .Stloc(localCount)
                .Brfalse(skipFinished)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .Stloc(localCount)
                .Mark(next)
                .Ldloc(localCount)
                .Brfalse(skipFinished)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .Stloc(localCount);
            _itemDescriptor.GenerateSkip(ilGenerator, pushReader, pushCtx);
            ilGenerator
                .Br(next)
                .Mark(skipFinished);
        }
    }
}