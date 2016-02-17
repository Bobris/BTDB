using System;
using System.Collections.Generic;
using System.Text;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class ListTypeDescriptor : ITypeDescriptor, IPersistTypeDescriptor
    {
        readonly TypeSerializers _typeSerializers;
        Type _type;
        Type _itemType;
        ITypeDescriptor _itemDescriptor;
        string _name;
        readonly ITypeConvertorGenerator _convertorGenerator;

        public ListTypeDescriptor(TypeSerializers typeSerializers, Type type)
        {
            _convertorGenerator = typeSerializers.ConvertorGenerator;
            _typeSerializers = typeSerializers;
            _type = type;
            _itemType = GetItemType(type);
        }

        public ListTypeDescriptor(TypeSerializers typeSerializers, AbstractBufferedReader reader, Func<AbstractBufferedReader, ITypeDescriptor> nestedDescriptorReader)
        {
            _convertorGenerator = typeSerializers.ConvertorGenerator;
            _typeSerializers = typeSerializers;
            InitFromItemDescriptor(nestedDescriptorReader(reader));
        }

        void InitFromItemDescriptor(ITypeDescriptor descriptor)
        {
            if (descriptor == _itemDescriptor && _name != null) return;
            _itemDescriptor = descriptor;
            if ((descriptor.Name?.Length ?? 0) == 0) return;
            _itemType = _itemDescriptor.GetPreferedType();
            Sealed = _itemDescriptor.Sealed;
            Name = $"List<{_itemDescriptor.Name}>";
        }

        public bool Equals(ITypeDescriptor other)
        {
            return Equals(other, new HashSet<ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance));
        }

        public string Name
        {
            get
            {
                if (_name == null) InitFromItemDescriptor(_itemDescriptor);
                return _name;
            }
            private set { _name = value; }
        }

        public void FinishBuildFromType(ITypeDescriptorFactory factory)
        {
            InitFromItemDescriptor(factory.Create(_itemType));
        }

        public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent)
        {
            text.Append("List<");
            _itemDescriptor.BuildHumanReadableFullName(text, stack, indent);
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

        public bool AnyOpNeedsCtx()
        {
            return !_itemDescriptor.StoredInline || _itemDescriptor.AnyOpNeedsCtx();
        }

        public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type targetType)
        {
            var localCount = ilGenerator.DeclareLocal(typeof(int));
            var itemType = _typeSerializers.LoadAsType(_itemDescriptor);
            var listType = typeof(ListWithDescriptor<>).MakeGenericType(itemType);
            if (!targetType.IsAssignableFrom(listType)) throw new NotSupportedException();
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
                .Do(pushDescriptor)
                .Newobj(listType.GetConstructor(new[] { typeof(int), typeof(ITypeDescriptor) }))
                .Stloc(localList)
                .Mark(next)
                .Ldloc(localCount)
                .Brfalse(loadFinished)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .Stloc(localCount)
                .Ldloc(localList);
            _itemDescriptor.GenerateLoadEx(ilGenerator, pushReader, pushCtx, il => il.Do(pushDescriptor).LdcI4(0).Callvirt(() => default(ITypeDescriptor).NestedType(0)), itemType, _convertorGenerator);
            ilGenerator
                .Callvirt(listType.GetInterface("ICollection`1").GetMethod("Add"))
                .Br(next)
                .Mark(loadFinished)
                .Ldloc(localList)
                .Castclass(targetType);
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
                    .LdcI4(0)
                    .Stloc(localIndex)
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

        public ITypeDescriptor NestedType(int index)
        {
            if (index == 0) return _itemDescriptor;
            return null;
        }

        public void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
        {
            InitFromItemDescriptor(map(_itemDescriptor));
        }

        public bool Sealed { get; private set; }
        public bool StoredInline => true;
        public bool LoadNeedsHelpWithConversion => false;

        public void ClearMappingToType()
        {
            _type = null;
            _itemType = null;
        }

        public bool ContainsField(string name)
        {
            return false;
        }

        public void Persist(AbstractBufferedWriter writer, Action<AbstractBufferedWriter, ITypeDescriptor> nestedDescriptorPersistor)
        {
            nestedDescriptorPersistor(writer, _itemDescriptor);
        }

        public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue, Type valueType)
        {
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            var notnull = ilGenerator.DefineLabel();
            var itemType = GetItemType(valueType);
            var localList = ilGenerator.DeclareLocal(
                typeof(IList<>).MakeGenericType(itemType));
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
                .LdcI4(0)
                .Stloc(localIndex)
                .Mark(next)
                .Ldloc(localIndex)
                .Ldloc(localCount)
                .BgeUn(finish);
            _itemDescriptor.GenerateSaveEx(ilGenerator, pushWriter, pushCtx,
                il => il.Ldloc(localList)
                        .Ldloc(localIndex)
                        .Callvirt(localList.LocalType.GetMethod("get_Item")), itemType);
            ilGenerator
                .Ldloc(localIndex)
                .LdcI4(1)
                .Add()
                .Stloc(localIndex)
                .Br(next)
                .Mark(finish);
        }

        static Type GetItemType(Type valueType)
        {
            if (valueType.IsArray)
            {
                return valueType.GetElementType();
            }
            return valueType.GetGenericArguments()[0];
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
            _itemDescriptor.GenerateSkipEx(ilGenerator, pushReader, pushCtx);
            ilGenerator
                .Br(next)
                .Mark(skipFinished);
        }
    }
}