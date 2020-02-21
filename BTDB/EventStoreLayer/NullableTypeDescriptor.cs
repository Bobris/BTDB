using System;
using System.Collections.Generic;
using System.Text;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class NullableTypeDescriptor : ITypeDescriptor, IPersistTypeDescriptor
    {
        readonly ITypeDescriptorCallbacks _typeSerializers;
        Type _type;
        Type _itemType;
        ITypeDescriptor _itemDescriptor;
        string _name;
        readonly ITypeConvertorGenerator _convertorGenerator;

        public NullableTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, Type type)
        {
            _convertorGenerator = typeSerializers.ConvertorGenerator;
            _typeSerializers = typeSerializers;
            _type = type;
            _itemType = GetItemType(type);
        }

        public NullableTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, AbstractBufferedReader reader, Func<AbstractBufferedReader, ITypeDescriptor> nestedDescriptorReader)
            : this(typeSerializers, nestedDescriptorReader(reader))
        {
        }

        NullableTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, ITypeDescriptor itemDesc)
        {
            _convertorGenerator = typeSerializers.ConvertorGenerator;
            _typeSerializers = typeSerializers;
            InitFromItemDescriptor(itemDesc);
        }

        void InitFromItemDescriptor(ITypeDescriptor descriptor)
        {
            if (descriptor == _itemDescriptor && _name != null) return;
            _itemDescriptor = descriptor;
            if ((descriptor.Name?.Length ?? 0) == 0) return;
            _itemType = _itemDescriptor.GetPreferedType();
            Sealed = _itemDescriptor.Sealed;
            Name = $"Nullable<{_itemDescriptor.Name}>";
        }

        public bool Equals(ITypeDescriptor other)
        {
            return Equals(other, new HashSet<ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance));
        }

        public override int GetHashCode()
        {
#pragma warning disable RECS0025 // Non-readonly field referenced in 'GetHashCode()'
            // ReSharper disable once NonReadonlyMemberInGetHashCode
            return 17 * _itemDescriptor.GetHashCode();
#pragma warning restore RECS0025 // Non-readonly field referenced in 'GetHashCode()'
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

        public bool FinishBuildFromType(ITypeDescriptorFactory factory)
        {
            var descriptor = factory.Create(_itemType);
            if (descriptor == null) return false;
            InitFromItemDescriptor(descriptor);
            return true;
        }

        public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent)
        {
            text.Append("Nullable<");
            _itemDescriptor.BuildHumanReadableFullName(text, stack, indent);
            text.Append(">");
        }

        public bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack)
        {
            var o = other as NullableTypeDescriptor;
            if (o == null) return false;
            return _itemDescriptor.Equals(o._itemDescriptor, stack);
        }

        public Type GetPreferedType()
        {
            if (_type == null)
            {
                _itemType = _typeSerializers.LoadAsType(_itemDescriptor);
                _type = typeof(Nullable<>).MakeGenericType(_itemType);
            }
            return _type;
        }

        public Type GetPreferedType(Type targetType)
        {
            if (_type == targetType) return _type;
            var targetTypeArguments = targetType.GetGenericArguments();
            var itemType = _typeSerializers.LoadAsType(_itemDescriptor, targetTypeArguments[0]);
            return targetType.GetGenericTypeDefinition().MakeGenericType(itemType);
        }

        public bool AnyOpNeedsCtx()
        {
            return !_itemDescriptor.StoredInline || _itemDescriptor.AnyOpNeedsCtx();
        }

        public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type targetType)
        {
            var genericArguments = targetType.GetGenericArguments();
            var itemType = genericArguments.Length > 0 ? targetType.GetGenericArguments()[0] : typeof(object);

            if (itemType == typeof(object))
            {
                var noValue = ilGenerator.DefineLabel();
                var finish = ilGenerator.DefineLabel();

                ilGenerator
                    .Do(pushReader)
                    .Callvirt(() => default(AbstractBufferedReader).ReadBool())
                    .Brfalse(noValue);
                _itemDescriptor.GenerateLoadEx(ilGenerator, pushReader, pushCtx,
                    il => il.Do(pushDescriptor).LdcI4(0).Callvirt(() => default(ITypeDescriptor).NestedType(0)), typeof(object), _convertorGenerator);
                ilGenerator
                    .Br(finish)
                    .Mark(noValue)
                    .Ldnull()
                    .Mark(finish);
            }
            else
            {
                var localResult = ilGenerator.DeclareLocal(targetType);
                var finish = ilGenerator.DefineLabel();
                var noValue = ilGenerator.DefineLabel();
                
                var nullableType = typeof(Nullable<>).MakeGenericType(itemType);

                if (!targetType.IsAssignableFrom(nullableType))
                    throw new NotSupportedException();
                ilGenerator
                    .Do(pushReader)
                    .Callvirt(() => default(AbstractBufferedReader).ReadBool())
                    .Brfalse(noValue);
                _itemDescriptor.GenerateLoadEx(ilGenerator, pushReader, pushCtx,
                    il => il.Do(pushDescriptor).LdcI4(0).Callvirt(() => default(ITypeDescriptor).NestedType(0)), itemType, _convertorGenerator);
                ilGenerator
                    .Newobj(nullableType.GetConstructor(new[] { itemType }))
                    .Stloc(localResult)
                    .BrS(finish)
                    .Mark(noValue)
                    .Ldloca(localResult)
                    .InitObj(nullableType)
                    .Mark(finish)
                    .Ldloc(localResult);
            }
        }

        public ITypeNewDescriptorGenerator BuildNewDescriptorGenerator()
        {
            if (_itemDescriptor.Sealed) return null;
            return new NullableTypeDescriptor.TypeNewDescriptorGenerator(this);
        }

        class TypeNewDescriptorGenerator : ITypeNewDescriptorGenerator
        {
            readonly NullableTypeDescriptor _nullableTypeDescriptor;

            public TypeNewDescriptorGenerator(NullableTypeDescriptor nullableTypeDescriptor)
            {
                _nullableTypeDescriptor = nullableTypeDescriptor;
            }

            public void GenerateTypeIterator(IILGen ilGenerator, Action<IILGen> pushObj, Action<IILGen> pushCtx, Type type)
            {
                var finish = ilGenerator.DefineLabel();
                var itemType = _nullableTypeDescriptor.GetPreferedType(type).GetGenericArguments()[0];
                var nullableType = typeof(Nullable<>).MakeGenericType(itemType);
                var localValue = ilGenerator.DeclareLocal(nullableType);

                ilGenerator
                    .Do(pushObj)
                    .Stloc(localValue)
                    .Ldloca(localValue)
                    .Call(nullableType.GetMethod("get_HasValue"))
                    .Brfalse(finish)
                    .Ldloca(localValue)
                    .Call(nullableType.GetMethod("get_Value"))
                    .Callvirt(() => default(IDescriptorSerializerLiteContext).StoreNewDescriptors(null))
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
            var itemType = GetItemType(valueType);
            var localValue = ilGenerator.DeclareLocal(valueType);
            var localHasValue = ilGenerator.DeclareLocal(typeof(bool));

            var finish = ilGenerator.DefineLabel();
            ilGenerator
                .Do(pushValue)
                .Stloc(localValue)
                .Do(pushWriter)
                .Ldloca(localValue)
                .Call(valueType.GetMethod("get_HasValue"))
                .Dup()
                .Stloc(localHasValue)
                .Callvirt(() => default(AbstractBufferedWriter).WriteBool(false))
                .Ldloc(localHasValue)
                .Brfalse(finish);
            _itemDescriptor.GenerateSaveEx(ilGenerator, pushWriter, pushCtx,
                il => il.Ldloca(localValue).Call(valueType.GetMethod("get_Value")), itemType);
            ilGenerator
                .Mark(finish);
        }

        static Type GetItemType(Type valueType)
        {
            return Nullable.GetUnderlyingType(valueType);
        }

        public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
        {
            var finish = ilGenerator.DefineLabel();
            ilGenerator
                .Do(pushReader)
                .Callvirt(() => default(AbstractBufferedReader).ReadBool())
                .Brfalse(finish);
            _itemDescriptor.GenerateSkipEx(ilGenerator, pushReader, pushCtx);
            ilGenerator
                .Mark(finish);
        }

        public ITypeDescriptor CloneAndMapNestedTypes(ITypeDescriptorCallbacks typeSerializers, Func<ITypeDescriptor, ITypeDescriptor> map)
        {
            var itemDesc = map(_itemDescriptor);
            if (_typeSerializers == typeSerializers && itemDesc == _itemDescriptor)
                return this;
            return new NullableTypeDescriptor(typeSerializers, itemDesc);
        }
    }
}