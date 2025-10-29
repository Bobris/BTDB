using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using BTDB.Collections;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public class TupleTypeDescriptor : ITypeDescriptor, IPersistTypeDescriptor
{
    Type? _type;

    StructList<ITypeDescriptor> _itemDescriptors;
    Type[]? _itemTypes;

    readonly ITypeDescriptorCallbacks _typeSerializers;
    string? _name;

    public TupleTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, Type type)
    {
        _typeSerializers = typeSerializers;
        _type = type;
    }

    public TupleTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, ref MemReader reader,
        DescriptorReader nestedDescriptorReader)
    {
        _typeSerializers = typeSerializers;
        var fieldCount = reader.ReadVUInt32();
        while (fieldCount-- > 0)
        {
            _itemDescriptors.Add(nestedDescriptorReader(ref reader));
        }
    }

    TupleTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, ITypeDescriptor[] typeDescriptors)
    {
        _typeSerializers = typeSerializers;
        _itemDescriptors.AddRange(typeDescriptors);
    }

    public bool Equals(ITypeDescriptor other)
    {
        return Equals(other, null);
    }

    public string Name => _name ??= CreateName();

    string CreateName()
    {
        var sb = new StringBuilder();
        sb.Append("Tuple<");
        var first = true;
        foreach (var itemDescriptor in _itemDescriptors)
        {
            if (first) first = false;
            else
                sb.Append(',');
            sb.Append(itemDescriptor.Name);
        }

        sb.Append('>');
        return sb.ToString();
    }

    public bool FinishBuildFromType(ITypeDescriptorFactory factory)
    {
        _itemTypes = _type!.GetGenericArguments();
        _itemDescriptors.Clear();
        foreach (var itemType in _itemTypes)
        {
            var descriptor = factory.Create(itemType);
            if (descriptor != null)
            {
                _itemDescriptors.Add(descriptor);
            }
            else
            {
                return false;
            }
        }

        return true;
    }

    public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent)
    {
        text.Append("Tuple<");
        var first = true;
        foreach (var descriptor in _itemDescriptors)
        {
            if (first) first = false;
            else
                text.Append(',');
            descriptor.BuildHumanReadableFullName(text, stack, indent);
        }

        text.Append('>');
    }

    public bool Equals(ITypeDescriptor other, Dictionary<ITypeDescriptor, ITypeDescriptor>? equalities)
    {
        if (ReferenceEquals(this, other)) return true;
        var o = other as TupleTypeDescriptor;
        if (o == null) return false;
        if (_itemDescriptors.Count != o._itemDescriptors.Count) return false;
        for (var i = 0; i < _itemDescriptors.Count; i++)
        {
            if (!_itemDescriptors[i].Equals(o._itemDescriptors[i], equalities)) return false;
        }

        return true;
    }

    public Type? GetPreferredType()
    {
        if (_itemTypes == null)
        {
            _itemTypes = new Type[_itemDescriptors.Count];
            var i = 0;
            foreach (var descriptor in _itemDescriptors)
            {
                _itemTypes[i++] = descriptor.GetPreferredType()!;
            }
        }

        return _type ??= TupleFieldHandler.ValueTupleTypes[_itemTypes.Length - 1].MakeGenericType(_itemTypes);
    }

    public Type? GetPreferredType(Type targetType)
    {
        var targetItemTypes = targetType.GetGenericArguments();
        for (var i = 0; i < targetItemTypes.Length; i++)
        {
            targetItemTypes[i] = _itemDescriptors[i].GetPreferredType(targetItemTypes[i]!)!;
        }

        return targetType.IsValueType
            ? TupleFieldHandler.ValueTupleTypes[targetItemTypes.Length - 1].MakeGenericType(targetItemTypes)
            : TupleFieldHandler.TupleTypes[targetItemTypes.Length - 1].MakeGenericType(targetItemTypes);
    }

    public bool AnyOpNeedsCtx()
    {
        return !_itemDescriptors.All(p => p.StoredInline) ||
               _itemDescriptors.Any(p => p.AnyOpNeedsCtx());
    }

    public static bool IsCompatibleWith(Type type)
    {
        if (!type.IsGenericType) return false;
        return type.InheritsOrImplements(typeof(ITuple));
    }

    public unsafe Layer2Loader GenerateLoad(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        if (!IsCompatibleWith(targetType))
            return this.BuildConvertingLoader(GetPreferredType()!, targetType, typeConverterFactory);
        var metadata = ReflectionMetadata.FindByType(targetType);
        if (metadata == null)
        {
            throw new BTDBException("Cannot load " + targetType.ToSimpleName() +
                                    " as it is not registered in ReflectionMetadata");
        }

        if (targetType.IsValueType)
        {
            var loaders = new StructList<Layer2Loader>();
            for (var i = 0; i < _itemDescriptors.Count; i++)
            {
                var fieldHandler = _itemDescriptors[i];
                if (i >= metadata.Fields.Length)
                {
                    loaders.Add((ref MemReader reader, ITypeBinaryDeserializerContext? ctx, ref byte _) =>
                    {
                        fieldHandler.Skip(ref reader, ctx);
                    });
                    continue;
                }

                var field = metadata.Fields[i];
                var loader = fieldHandler.GenerateLoadEx(field.Type, typeConverterFactory);
                var offset = field.ByteOffset!.Value;
                loaders.Add((ref MemReader reader, ITypeBinaryDeserializerContext? ctx, ref byte value) =>
                {
                    loader(ref reader, ctx,
                        ref Unsafe.AddByteOffset(ref value, offset));
                });
            }

            var loadersArray = loaders.ToArray();
            return (ref MemReader reader, ITypeBinaryDeserializerContext? ctx, ref byte value) =>
            {
                foreach (var fieldHandlerLoad in loadersArray)
                {
                    fieldHandlerLoad(ref reader, ctx, ref value);
                }
            };
        }
        else
        {
            var creator = metadata.Creator;
            var loaders = new StructList<Layer2Loader>();
            for (var i = 0; i < _itemDescriptors.Count; i++)
            {
                var fieldHandler = _itemDescriptors[i];
                if (i >= metadata.Fields.Length)
                {
                    loaders.Add((ref MemReader reader, ITypeBinaryDeserializerContext? ctx, ref byte _) =>
                    {
                        fieldHandler.Skip(ref reader, ctx);
                    });
                    continue;
                }

                var field = metadata.Fields[i];
                var loader = fieldHandler.GenerateLoadEx(field.Type, typeConverterFactory);
                var offset = field.ByteOffset!.Value;
                loaders.Add((ref MemReader reader, ITypeBinaryDeserializerContext? ctx, ref byte value) =>
                {
                    loader(ref reader, ctx,
                        ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset));
                });
            }

            var loadersArray = loaders.ToArray();
            return (ref MemReader reader, ITypeBinaryDeserializerContext? ctx, ref byte value) =>
            {
                var tuple = creator();
                Unsafe.As<byte, object>(ref value) = tuple;
                foreach (var fieldHandlerLoad in loadersArray)
                {
                    fieldHandlerLoad(ref reader, ctx, ref value);
                }
            };
        }
    }

    public void Skip(ref MemReader reader, ITypeBinaryDeserializerContext? ctx)
    {
        foreach (var itemDescriptor in _itemDescriptors)
        {
            itemDescriptor.Skip(ref reader, ctx);
        }
    }

    public Layer2Saver GenerateSave(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        if (GetPreferredType() != targetType)
            return this.BuildConvertingSaver(targetType, GetPreferredType()!, typeConverterFactory);
        var metadata = ReflectionMetadata.FindByType(targetType);
        if (metadata == null)
        {
            throw new BTDBException("Cannot save " + targetType.ToSimpleName() +
                                    " as it is not registered in ReflectionMetadata");
        }

        if (targetType.IsValueType)
        {
            var savers = new StructList<Layer2Saver>();
            for (var i = 0; i < _itemDescriptors.Count; i++)
            {
                var fieldHandler = _itemDescriptors[i];
                var field = metadata.Fields[i];
                var saver = fieldHandler.GenerateSaveEx(field.Type, typeConverterFactory);
                var offset = field.ByteOffset!.Value;
                savers.Add((ref MemWriter writer, ITypeBinarySerializerContext? ctx, ref byte value) =>
                {
                    saver(ref writer, ctx,
                        ref Unsafe.AddByteOffset(ref value, offset));
                });
            }

            var saversArray = savers.ToArray();
            return (ref MemWriter writer, ITypeBinarySerializerContext? ctx, ref byte value) =>
            {
                foreach (var fieldHandlerSave in saversArray)
                {
                    fieldHandlerSave(ref writer, ctx, ref value);
                }
            };
        }
        else
        {
            var savers = new StructList<Layer2Saver>();
            for (var i = 0; i < _itemDescriptors.Count; i++)
            {
                var fieldHandler = _itemDescriptors[i];
                var field = metadata.Fields[i];
                var saver = fieldHandler.GenerateSaveEx(field.Type, typeConverterFactory);
                var offset = field.ByteOffset!.Value;
                savers.Add((ref MemWriter writer, ITypeBinarySerializerContext? ctx, ref byte value) =>
                {
                    saver(ref writer, ctx,
                        ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset));
                });
            }

            var saversArray = savers.ToArray();
            return (ref MemWriter writer, ITypeBinarySerializerContext? ctx, ref byte value) =>
            {
                foreach (var fieldHandlerSave in saversArray)
                {
                    fieldHandlerSave(ref writer, ctx, ref value);
                }
            };
        }
    }

    public Layer2NewDescriptor? GenerateNewDescriptor(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        if (_itemDescriptors.All(d => d.Sealed)) return null;
        var metadata = ReflectionMetadata.FindByType(targetType);
        if (metadata == null)
        {
            throw new BTDBException("Cannot save " + targetType.ToSimpleName() +
                                    " as it is not registered in ReflectionMetadata");
        }

        if (targetType.IsValueType)
        {
            var newDescriptors = new StructList<Layer2NewDescriptor>();
            for (var i = 0; i < _itemDescriptors.Count; i++)
            {
                var fieldHandler = _itemDescriptors[i];
                var field = metadata.Fields[i];
                var newDescriptorEx = fieldHandler.GenerateNewDescriptorEx(field.Type, typeConverterFactory);
                if (newDescriptorEx == null) continue;
                var offset = field.ByteOffset!.Value;
                newDescriptors.Add((IDescriptorSerializerLiteContext ctx, ref byte value) =>
                {
                    newDescriptorEx(ctx, ref Unsafe.AddByteOffset(ref value, offset));
                });
            }

            var newDescriptorArray = newDescriptors.ToArray();
            return (IDescriptorSerializerLiteContext ctx, ref byte value) =>
            {
                foreach (var newDescriptor in newDescriptorArray)
                {
                    newDescriptor(ctx, ref value);
                }
            };
        }
        else
        {
            var newDescriptors = new StructList<Layer2NewDescriptor>();
            for (var i = 0; i < _itemDescriptors.Count; i++)
            {
                var fieldHandler = _itemDescriptors[i];
                var field = metadata.Fields[i];
                var newDescriptorEx = fieldHandler.GenerateNewDescriptorEx(field.Type, typeConverterFactory);
                if (newDescriptorEx == null) continue;
                var offset = field.ByteOffset!.Value;
                newDescriptors.Add((IDescriptorSerializerLiteContext ctx, ref byte value) =>
                {
                    newDescriptorEx(ctx, ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset));
                });
            }

            var newDescriptorArray = newDescriptors.ToArray();
            return (IDescriptorSerializerLiteContext ctx, ref byte value) =>
            {
                foreach (var newDescriptor in newDescriptorArray)
                {
                    newDescriptor(ctx, ref value);
                }
            };
        }
    }

    public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx,
        Action<IILGen> pushDescriptor, Type targetType)
    {
        if (targetType == typeof(object))
        {
            var resultLoc = ilGenerator.DeclareLocal(typeof(DynamicTuple), "result");
            ilGenerator
                .Do(pushDescriptor)
                .Castclass(typeof(TupleTypeDescriptor))
                .Newobj(typeof(DynamicTuple).GetConstructor(new[] { typeof(TupleTypeDescriptor) })!)
                .Stloc(resultLoc);
            var idx = 0;
            foreach (var descriptor in _itemDescriptors)
            {
                var idxForCapture = idx;
                ilGenerator.Ldloc(resultLoc);
                ilGenerator.LdcI4(idx);
                descriptor.GenerateLoadEx(ilGenerator, pushReader, pushCtx,
                    il =>
                        il.Do(pushDescriptor)
                            .LdcI4(idxForCapture)
                            .Callvirt(() => default(ITypeDescriptor).NestedType(0)), typeof(object),
                    _typeSerializers.ConvertorGenerator);
                ilGenerator.Callvirt(() => default(DynamicTuple).SetFieldByIdxFast(0, null));
                idx++;
            }

            ilGenerator
                .Ldloc(resultLoc)
                .Castclass(typeof(object));
        }
        else
        {
            var genericArguments = targetType.GetGenericArguments();
            var valueType = TupleFieldHandler.ValueTupleTypes[genericArguments.Length - 1]
                .MakeGenericType(genericArguments);
            var localResult = ilGenerator.DeclareLocal(valueType);
            ilGenerator
                .Ldloca(localResult)
                .InitObj(valueType);
            for (var i = 0; i < genericArguments.Length; i++)
            {
                if (i >= _itemDescriptors.Count) break;

                if (!_typeSerializers.IsSafeToLoad(genericArguments[i]))
                {
                    _itemDescriptors[i].GenerateSkipEx(ilGenerator, pushReader, pushCtx);
                    continue;
                }

                ilGenerator.Ldloca(localResult);
                _itemDescriptors[i].GenerateLoadEx(ilGenerator, pushReader, pushCtx,
                    il => il.Do(pushDescriptor).LdcI4(i)
                        .Callvirt(() => default(ITypeDescriptor).NestedType(0)),
                    genericArguments[i], _typeSerializers.ConvertorGenerator);
                ilGenerator
                    .Stfld(valueType.GetField(TupleFieldHandler.TupleFieldName[i])!);
            }

            for (var i = genericArguments.Length; i < _itemDescriptors.Count; i++)
            {
                _itemDescriptors[i].GenerateSkipEx(ilGenerator, pushReader, pushCtx);
            }

            if (targetType.IsValueType)
            {
                ilGenerator.Ldloc(localResult);
            }
            else
            {
                for (var i = 0; i < genericArguments.Length; i++)
                {
                    ilGenerator.Ldloca(localResult).Ldfld(valueType.GetField(TupleFieldHandler.TupleFieldName[i])!);
                }

                ilGenerator.Newobj(targetType.GetConstructor(genericArguments)!);
            }
        }
    }

    public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        foreach (var descriptor in _itemDescriptors)
        {
            descriptor.GenerateSkipEx(ilGenerator, pushReader, pushCtx);
        }
    }

    public class DynamicTuple : IDynamicMetaObjectProvider, IKnowDescriptor,
        IEnumerable<object>
    {
        readonly TupleTypeDescriptor _ownerDescriptor;
        readonly object[] _fieldValues;

        public DynamicTuple(TupleTypeDescriptor ownerDescriptor)
        {
            _ownerDescriptor = ownerDescriptor;
            _fieldValues = new object[_ownerDescriptor._itemDescriptors.Count];
        }

        DynamicMetaObject IDynamicMetaObjectProvider.GetMetaObject(Expression parameter)
        {
            return new DynamicDictionaryMetaObject(parameter, this);
        }

        public void SetFieldByIdxFast(int idx, object value)
        {
            _fieldValues[idx] = value;
        }

        public void SetFieldByIdx(int idx, string fieldName, TupleTypeDescriptor descriptor, object value)
        {
            if (_ownerDescriptor == descriptor)
            {
                if (idx < 0)
                    ThrowMemberAccessException(fieldName);
                _fieldValues[idx] = value;
                return;
            }

            var realIndex = _ownerDescriptor.FindFieldIndexWithThrow(fieldName);
            _fieldValues[realIndex] = value;
        }

        public object GetFieldByIdx(int idx, string fieldName, TupleTypeDescriptor descriptor)
        {
            if (_ownerDescriptor == descriptor)
            {
                if (idx < 0)
                    ThrowMemberAccessException(fieldName);
                return _fieldValues[idx];
            }

            var realIndex = _ownerDescriptor.FindFieldIndexWithThrow(fieldName);
            return _fieldValues[realIndex];
        }

        internal void ThrowMemberAccessException(string fieldName)
        {
            throw new MemberAccessException($"{_ownerDescriptor.Name} does not have member {fieldName}");
        }

        class DynamicDictionaryMetaObject : DynamicMetaObject
        {
            internal DynamicDictionaryMetaObject(Expression parameter, DynamicTuple value)
                : base(parameter, BindingRestrictions.Empty, value)
            {
            }

            public override DynamicMetaObject BindSetMember(SetMemberBinder binder, DynamicMetaObject value)
            {
                var descriptor = ((DynamicTuple)Value)!._ownerDescriptor;
                var idx = TupleFieldHandler.TupleFieldName.IndexOf(binder.Name);
                return new(Expression.Call(Expression.Convert(Expression, LimitType),
                        typeof(DynamicTuple).GetMethod(nameof(SetFieldByIdx))!,
                        Expression.Constant(idx),
                        Expression.Constant(binder.Name),
                        Expression.Constant(descriptor),
                        Expression.Convert(value.Expression, typeof(object))),
                    BindingRestrictions.GetTypeRestriction(Expression, LimitType));
            }

            public override DynamicMetaObject BindGetMember(GetMemberBinder binder)
            {
                var descriptor = ((DynamicTuple)Value)!._ownerDescriptor;
                var idx = TupleFieldHandler.TupleFieldName.IndexOf(binder.Name);
                return new(Expression.Call(Expression.Convert(Expression, LimitType),
                        typeof(DynamicTuple).GetMethod(nameof(GetFieldByIdx))!,
                        Expression.Constant(idx),
                        Expression.Constant(binder.Name),
                        Expression.Constant(descriptor)),
                    BindingRestrictions.GetTypeRestriction(Expression, LimitType));
            }

            public override IEnumerable<string> GetDynamicMemberNames()
            {
                var descriptor = ((DynamicTuple)Value)!._ownerDescriptor;
                return TupleFieldHandler.TupleFieldName.Take((int)descriptor._itemDescriptors.Count);
            }
        }

        public IEnumerator<object> GetEnumerator()
        {
            foreach (var value in _fieldValues)
            {
                yield return value;
            }
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            var idx = 0;
            sb.Append('(');
            foreach (var item in _fieldValues)
            {
                if (idx > 0) sb.Append(", ");
                sb.AppendJsonLike(item);
                idx++;
            }

            sb.Append(')');
            return sb.ToString();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public ITypeDescriptor GetDescriptor()
        {
            return _ownerDescriptor;
        }
    }

    int FindFieldIndexWithThrow(string fieldName)
    {
        var idx = TupleFieldHandler.TupleFieldName.IndexOf(fieldName);
        if (idx < 0 || idx >= _itemDescriptors.Count)
            throw new MemberAccessException($"{Name} does not have member {fieldName}");
        return idx;
    }

    public ITypeNewDescriptorGenerator? BuildNewDescriptorGenerator()
    {
        if (_itemDescriptors.All(d => d.Sealed)) return null;
        return new TypeNewDescriptorGenerator(this);
    }

    class TypeNewDescriptorGenerator : ITypeNewDescriptorGenerator
    {
        readonly TupleTypeDescriptor _objectTypeDescriptor;

        public TypeNewDescriptorGenerator(TupleTypeDescriptor objectTypeDescriptor)
        {
            _objectTypeDescriptor = objectTypeDescriptor;
        }

        public void GenerateTypeIterator(IILGen ilGenerator, Action<IILGen> pushObj, Action<IILGen> pushCtx,
            Type type)
        {
            var idx = -1;
            foreach (var descriptor in _objectTypeDescriptor._itemDescriptors)
            {
                idx++;
                if (descriptor.Sealed) continue;
                ilGenerator
                    .Do(pushCtx)
                    .Do(pushObj);
                Type itemType = null;
                if (type.IsValueType)
                {
                    var fieldInfo = type.GetField(TupleFieldHandler.TupleFieldName[idx])!;
                    itemType = fieldInfo.FieldType;
                    ilGenerator.Ldfld(fieldInfo);
                }
                else
                {
                    var methodInfo = type.GetProperty(TupleFieldHandler.TupleFieldName[idx])!.GetGetMethod()!;
                    itemType = methodInfo.ReturnType;
                    ilGenerator.Callvirt(methodInfo);
                }

                if (itemType.IsValueType)
                {
                    ilGenerator.Box(itemType);
                }

                ilGenerator.Callvirt(typeof(IDescriptorSerializerLiteContext).GetMethod(
                    nameof(IDescriptorSerializerLiteContext.StoreNewDescriptors))!);
            }
        }
    }

    public ITypeDescriptor? NestedType(int index)
    {
        return index < _itemDescriptors.Count ? _itemDescriptors[index] : null;
    }

    public void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
    {
        for (var index = 0; index < _itemDescriptors.Count; index++)
        {
            _itemDescriptors[index] = map(_itemDescriptors[index]);
        }
    }

    public bool Sealed => _itemDescriptors.All(d => d.Sealed);

    public bool StoredInline => true;

    public bool LoadNeedsHelpWithConversion => false;

    public void ClearMappingToType()
    {
        _itemTypes = null;
        _type = null;
    }

    public bool ContainsField(string name)
    {
        return (uint)TupleFieldHandler.TupleFieldName.IndexOf(name) < _itemDescriptors.Count;
    }

    public IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields => _itemDescriptors.Select((d, i) =>
        new KeyValuePair<string, ITypeDescriptor>(TupleFieldHandler.TupleFieldName[i], d));

    public void Persist(ref MemWriter writer, DescriptorWriter nestedDescriptorWriter)
    {
        writer.WriteVUInt32(_itemDescriptors.Count);
        foreach (var descriptor in _itemDescriptors)
        {
            nestedDescriptorWriter(ref writer, descriptor);
        }
    }

    public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx,
        Action<IILGen> pushValue, Type valueType)
    {
        var locValue = ilGenerator.DeclareLocal(valueType, "value");
        ilGenerator
            .Do(pushValue)
            .Stloc(locValue);
        var itemTypes = valueType.GetGenericArguments();
        var idx = -1;
        foreach (var typeDescriptor in _itemDescriptors)
        {
            idx++;
            if (valueType.IsValueType)
            {
                typeDescriptor.GenerateSaveEx(ilGenerator, pushWriter, pushCtx,
                    il => il.Ldloc(locValue).Ldfld(valueType.GetField(TupleFieldHandler.TupleFieldName[idx])),
                    itemTypes[idx]);
            }
            else
            {
                typeDescriptor.GenerateSaveEx(ilGenerator, pushWriter, pushCtx,
                    il => il.Ldloc(locValue).Callvirt(valueType.GetProperty(TupleFieldHandler.TupleFieldName[idx])
                        .GetGetMethod()), itemTypes[idx]);
            }
        }
    }

    public ITypeDescriptor CloneAndMapNestedTypes(ITypeDescriptorCallbacks typeSerializers,
        Func<ITypeDescriptor, ITypeDescriptor> map)
    {
        var tds = new ITypeDescriptor[_itemDescriptors.Count];
        for (var i = 0; i < _itemDescriptors.Count; i++)
        {
            tds[i] = map(_itemDescriptors[i]);
        }

        if (typeSerializers == _typeSerializers && tds.SequenceEqual(_itemDescriptors))
            return this;
        var res = new TupleTypeDescriptor(typeSerializers, tds);
        return res;
    }
}
