using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using BTDB.Collections;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using BTDB.FieldHandler;

namespace BTDB.EventStoreLayer;

public class ObjectTypeDescriptor : ITypeDescriptor, IPersistTypeDescriptor
{
    Type? _type;

    StructList<KeyValuePair<string, ITypeDescriptor>> _fields;

    readonly ITypeDescriptorCallbacks _typeSerializers;
    readonly TypeDescriptorOptions? _typeDescriptorOptions;
    readonly BindingFlags _propertyBindingFlags;

    public ObjectTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, Type type,
        TypeDescriptorOptions? typeDescriptorOptions) :
        this(typeSerializers, typeSerializers.TypeNameMapper.ToName(type), type.IsSealed, typeDescriptorOptions)
    {
        _type = type;
    }

    public ObjectTypeDescriptor(ITypeDescriptorCallbacks typeSerializers,
        ref SpanReader reader,
        DescriptorReader nestedDescriptorReader,
        TypeDescriptorOptions? typeDescriptorOptions) :
        this(typeSerializers, reader.ReadString()!, false, typeDescriptorOptions)
    {
        var fieldCount = reader.ReadVUInt32();
        while (fieldCount-- > 0)
        {
            _fields.Add(
                new KeyValuePair<string, ITypeDescriptor>(reader.ReadString(), nestedDescriptorReader(ref reader)));
        }
    }

    ObjectTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, string name, bool @sealed,
        TypeDescriptorOptions? typeDescriptorOptions)
    {
        _typeSerializers = typeSerializers;
        _typeDescriptorOptions = typeDescriptorOptions;
        Sealed = @sealed;
        Name = name;

        _propertyBindingFlags = typeDescriptorOptions?.SerializeNonPublicProperties == true
            ? BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic
            : BindingFlags.Instance | BindingFlags.Public;
    }

    public bool Equals(ITypeDescriptor other)
    {
        return Equals(other, new HashSet<ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance));
    }

    public string Name { get; }

    public static void CheckObjectTypeIsGoodDto(Type type,
        BindingFlags propertyBindingFlags = BindingFlags.Instance | BindingFlags.Public)
    {
        var isInterface = type.IsInterface;
        foreach (var propertyInfo in type.GetProperties(propertyBindingFlags))
        {
            if (propertyInfo.GetIndexParameters().Length != 0) continue;
            if (ShouldNotBeStored(propertyInfo)) continue;
            if (propertyInfo.GetAnyGetMethod() == null)
                throw new InvalidOperationException("Trying to serialize type " + type.ToSimpleName() +
                                                    " and property " + propertyInfo.Name +
                                                    " does not have getter. If you don't want to serialize this property add [NotStored] attribute.");
            if (!isInterface && propertyInfo.GetAnySetMethod() == null)
                throw new InvalidOperationException("Trying to serialize type " + type.ToSimpleName() +
                                                    " and property " + propertyInfo.Name +
                                                    " does not have setter. If you don't want to serialize this property add [NotStored] attribute.");
        }

        foreach (var fieldInfo in type.GetFields(BindingFlags.NonPublic |
                                                 BindingFlags.Public |
                                                 BindingFlags.Instance))
        {
            if (fieldInfo.IsPrivate) continue;
            if (ShouldNotBeStored(fieldInfo)) continue;
            throw new InvalidOperationException("Serialize type " + type.ToSimpleName() +
                                                " with non-private field " + fieldInfo.Name +
                                                " is forbidden without marking it with [NotStored] attribute");
        }
    }

    static bool ShouldNotBeStored(ICustomAttributeProvider propertyInfo)
    {
        return propertyInfo.GetCustomAttributes(typeof(NotStoredAttribute), true).Length != 0;
    }

    public bool FinishBuildFromType(ITypeDescriptorFactory factory)
    {
        var props = _type!.GetProperties(_propertyBindingFlags);
#if DEBUG
        CheckObjectTypeIsGoodDto(_type, _propertyBindingFlags);
#endif
        foreach (var propertyInfo in props)
        {
            if (propertyInfo.GetIndexParameters().Length != 0) continue;
            if (ShouldNotBeStored(propertyInfo)) continue;
            var descriptor = factory.Create(propertyInfo.PropertyType);
            if (descriptor != null)
            {
                _fields.Add(new KeyValuePair<string, ITypeDescriptor>(GetPersistentName(propertyInfo), descriptor));
            }
        }

        _fields.Sort(Comparer<KeyValuePair<string, ITypeDescriptor>>.Create((l, r) =>
            string.Compare(l.Key, r.Key, StringComparison.InvariantCulture)));
        return true;
    }

    public static string GetPersistentName(PropertyInfo propertyInfo)
    {
        var a = propertyInfo.GetCustomAttribute<PersistedNameAttribute>();
        return a != null ? a.Name : propertyInfo.Name;
    }

    public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent)
    {
        if (stack.Contains(this))
        {
            text.Append(Name);
            return;
        }

        stack.Add(this);
        text.AppendLine(Name);
        AppendIndent(text, indent);
        text.AppendLine("{");
        indent++;
        foreach (var pair in _fields)
        {
            AppendIndent(text, indent);
            text.Append(pair.Key);
            text.Append(" : ");
            pair.Value.BuildHumanReadableFullName(text, stack, indent);
            text.AppendLine();
        }

        indent--;
        AppendIndent(text, indent);
        text.Append("}");
        stack.Remove(this);
    }

    static void AppendIndent(StringBuilder text, uint indent)
    {
        text.Append(' ', (int)(indent * 4));
    }

    public bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack)
    {
        var o = other as ObjectTypeDescriptor;
        if (o == null) return false;
        if (Name != o.Name) return false;
        if (stack.Contains(this)) return true;
        if (_fields.Count != o._fields.Count) return false;
        stack.Add(this);
        try
        {
            for (int i = 0; i < _fields.Count; i++)
            {
                if (_fields[i].Key != o._fields[i].Key) return false;
                if (!_fields[i].Value.Equals(o._fields[i].Value, stack)) return false;
            }
        }
        finally
        {
            stack.Remove(this);
        }

        return true;
    }

    public Type? GetPreferredType()
    {
        if (_type == null)
            _type = _typeSerializers.TypeNameMapper.ToType(Name);
        return _type;
    }

    public Type? GetPreferredType(Type targetType)
    {
        var res = GetPreferredType();
        if (res == targetType || res == null) return res;
        res = DBObjectFieldHandler.Unwrap(res);
        if (res == DBObjectFieldHandler.Unwrap(targetType)) return targetType;
        return res;
    }

    public bool AnyOpNeedsCtx()
    {
        return !_fields.All(p => p.Value.StoredInline) || _fields.Any(p => p.Value.AnyOpNeedsCtx());
    }

    public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx,
        Action<IILGen> pushDescriptor, Type targetType)
    {
        if (targetType == typeof(object))
        {
            var resultLoc = ilGenerator.DeclareLocal(typeof(DynamicObject), "result");
            var labelNoCtx = ilGenerator.DefineLabel();
            ilGenerator
                .Do(pushDescriptor)
                .Castclass(typeof(ObjectTypeDescriptor))
                .Newobj(typeof(DynamicObject).GetConstructor(new[] { typeof(ObjectTypeDescriptor) })!)
                .Stloc(resultLoc)
                .Do(pushCtx)
                .BrfalseS(labelNoCtx)
                .Do(pushCtx)
                .Ldloc(resultLoc)
                .Callvirt(() => default(ITypeBinaryDeserializerContext).AddBackRef(null))
                .Mark(labelNoCtx);
            var idx = 0;
            foreach (var pair in _fields)
            {
                var idxForCapture = idx;
                ilGenerator.Ldloc(resultLoc);
                ilGenerator.LdcI4(idx);
                pair.Value.GenerateLoadEx(ilGenerator, pushReader, pushCtx,
                    il =>
                        il.Do(pushDescriptor)
                            .LdcI4(idxForCapture)
                            .Callvirt(() => default(ITypeDescriptor).NestedType(0)), typeof(object),
                    _typeSerializers.ConvertorGenerator);
                ilGenerator.Callvirt(() => default(DynamicObject).SetFieldByIdxFast(0, null));
                idx++;
            }

            ilGenerator
                .Ldloc(resultLoc)
                .Castclass(typeof(object));
        }
        else
        {
            var resultLoc = ilGenerator.DeclareLocal(targetType, "result");
            var labelNoCtx = ilGenerator.DefineLabel();
            var defaultConstructor = targetType.GetDefaultConstructor();
            if (defaultConstructor == null)
            {
                ilGenerator
                    .Ldtoken(targetType)
                    .Call(() => Type.GetTypeFromHandle(new()))
                    .Call(() => RuntimeHelpers.GetUninitializedObject(null))
                    .Castclass(targetType);
            }
            else
            {
                ilGenerator
                    .Newobj(defaultConstructor);
            }

            ilGenerator
                .Stloc(resultLoc)
                .Do(pushCtx)
                .BrfalseS(labelNoCtx)
                .Do(pushCtx)
                .Ldloc(resultLoc)
                .Callvirt(() => default(ITypeBinaryDeserializerContext).AddBackRef(null))
                .Mark(labelNoCtx);
            var props = targetType.GetProperties(_propertyBindingFlags);
            for (var idx = 0; idx < _fields.Count; idx++)
            {
                var idxForCapture = idx;
                var pair = _fields[idx];
                var prop = props.FirstOrDefault(p => GetPersistentName(p) == pair.Key);
                if (prop == null || !_typeSerializers.IsSafeToLoad(prop.PropertyType))
                {
                    pair.Value.GenerateSkipEx(ilGenerator, pushReader, pushCtx);
                    continue;
                }

                ilGenerator.Ldloc(resultLoc);
                pair.Value.GenerateLoadEx(ilGenerator, pushReader, pushCtx,
                    il => il.Do(pushDescriptor).LdcI4(idxForCapture)
                        .Callvirt(() => default(ITypeDescriptor).NestedType(0)),
                    prop.PropertyType, _typeSerializers.ConvertorGenerator);
                ilGenerator.Callvirt(prop.GetAnySetMethod()!);
            }

            ilGenerator.Ldloc(resultLoc);
        }
    }

    public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        foreach (var pair in _fields)
        {
            pair.Value.GenerateSkipEx(ilGenerator, pushReader, pushCtx);
        }
    }

    int FindFieldIndex(string fieldName)
    {
        var f = _fields.AsReadOnlySpan();
        for (var i = 0; i < f.Length; i++)
        {
            if (f[i].Key == fieldName) return i;
        }

        return -1;
    }

    int FindFieldIndexWithThrow(string fieldName)
    {
        var index = FindFieldIndex(fieldName);
        if (index < 0)
            throw new MemberAccessException($"{Name} does not have member {fieldName}");
        return index;
    }

    public class DynamicObject : IDynamicMetaObjectProvider, IKnowDescriptor, IEnumerable<KeyValuePair<string, object>>
    {
        readonly ObjectTypeDescriptor _ownerDescriptor;
        readonly object[] _fieldValues;

        public DynamicObject(ObjectTypeDescriptor ownerDescriptor)
        {
            _ownerDescriptor = ownerDescriptor;
            _fieldValues = new object[_ownerDescriptor._fields.Count];
        }

        DynamicMetaObject IDynamicMetaObjectProvider.GetMetaObject(Expression parameter)
        {
            return new DynamicDictionaryMetaObject(parameter, this);
        }

        public void SetFieldByIdxFast(int idx, object value)
        {
            _fieldValues[idx] = value;
        }

        public void SetFieldByIdx(int idx, string fieldName, ObjectTypeDescriptor descriptor, object value)
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

        public object GetFieldByIdx(int idx, string fieldName, ObjectTypeDescriptor descriptor)
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

        void ThrowMemberAccessException(string fieldName)
        {
            throw new MemberAccessException($"{_ownerDescriptor.Name} does not have member {fieldName}");
        }

        class DynamicDictionaryMetaObject : DynamicMetaObject
        {
            internal DynamicDictionaryMetaObject(Expression parameter, DynamicObject value)
                : base(parameter, BindingRestrictions.Empty, value)
            {
            }

            public override DynamicMetaObject BindSetMember(SetMemberBinder binder, DynamicMetaObject value)
            {
                var descriptor = ((DynamicObject)Value)._ownerDescriptor;
                var idx = descriptor.FindFieldIndex(binder.Name);
                return new DynamicMetaObject(Expression.Call(Expression.Convert(Expression, LimitType),
                        typeof(DynamicObject).GetMethod(nameof(SetFieldByIdx))!,
                        Expression.Constant(idx),
                        Expression.Constant(binder.Name),
                        Expression.Constant(descriptor),
                        Expression.Convert(value.Expression, typeof(object))),
                    BindingRestrictions.GetTypeRestriction(Expression, LimitType));
            }

            public override DynamicMetaObject BindGetMember(GetMemberBinder binder)
            {
                var descriptor = ((DynamicObject)Value)._ownerDescriptor;
                var idx = descriptor.FindFieldIndex(binder.Name);
                return new DynamicMetaObject(Expression.Call(Expression.Convert(Expression, LimitType),
                        typeof(DynamicObject).GetMethod(nameof(GetFieldByIdx))!,
                        Expression.Constant(idx),
                        Expression.Constant(binder.Name),
                        Expression.Constant(descriptor)),
                    BindingRestrictions.GetTypeRestriction(Expression, LimitType));
            }

            public override IEnumerable<string> GetDynamicMemberNames()
            {
                var descriptor = ((DynamicObject)Value)._ownerDescriptor;
                return descriptor._fields.Select(p => p.Key);
            }
        }

        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            var idx = 0;
            foreach (var item in _ownerDescriptor._fields)
            {
                yield return new KeyValuePair<string, object>(item.Key, _fieldValues[idx]);
                idx++;
            }
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            var idx = 0;
            sb.Append("{ ");
            foreach (var item in _ownerDescriptor._fields)
            {
                if (idx > 0) sb.Append(", ");
                sb.Append($"\"{item.Key}\"").Append(": ").AppendJsonLike(_fieldValues[idx]);
                idx++;
            }

            sb.Append(" }");
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

    public ITypeNewDescriptorGenerator? BuildNewDescriptorGenerator()
    {
        if (_fields.Select(p => p.Value).All(d => d.Sealed)) return null;
        return new TypeNewDescriptorGenerator(this, _propertyBindingFlags);
    }

    class TypeNewDescriptorGenerator : ITypeNewDescriptorGenerator
    {
        readonly ObjectTypeDescriptor _objectTypeDescriptor;
        readonly BindingFlags _propertyBindingFlags;

        public TypeNewDescriptorGenerator(ObjectTypeDescriptor objectTypeDescriptor, BindingFlags propertyBindingFlags)
        {
            _objectTypeDescriptor = objectTypeDescriptor;
            _propertyBindingFlags = propertyBindingFlags;
        }

        public void GenerateTypeIterator(IILGen ilGenerator, Action<IILGen> pushObj, Action<IILGen> pushCtx,
            Type type)
        {
            var allProps = _objectTypeDescriptor.GetPreferredType()!.GetProperties(_propertyBindingFlags);
            foreach (var pair in _objectTypeDescriptor._fields)
            {
                if (pair.Value.Sealed) continue;
                var getter = allProps.First(p => GetPersistentName(p) == pair.Key).GetAnyGetMethod()!;
                var itemType = getter.ReturnType;
                ilGenerator
                    .Do(pushCtx)
                    .Do(pushObj)
                    .Castclass(_objectTypeDescriptor._type!)
                    .Callvirt(getter)
                    .Do(il =>
                    {
                        if (itemType.IsValueType)
                        {
                            il.Box(itemType);
                        }
                    })
                    .Callvirt(typeof(IDescriptorSerializerLiteContext).GetMethod(
                        nameof(IDescriptorSerializerLiteContext.StoreNewDescriptors))!);
            }
        }
    }

    public ITypeDescriptor? NestedType(int index)
    {
        return index < _fields.Count ? _fields[index].Value : null;
    }

    public void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
    {
        for (var index = 0; index < _fields.Count; index++)
        {
            var keyValuePair = _fields[index];
            var mapped = map(keyValuePair.Value);
            if (mapped == keyValuePair.Value) continue;
            keyValuePair = new KeyValuePair<string, ITypeDescriptor>(keyValuePair.Key, mapped);
            _fields[index] = keyValuePair;
        }
    }

    public bool Sealed { get; }

    public bool StoredInline => false;

    public bool LoadNeedsHelpWithConversion => false;

    public void ClearMappingToType()
    {
        _type = null;
    }

    public bool ContainsField(string name)
    {
        foreach (var pair in _fields)
        {
            if (pair.Key == name) return true;
        }

        return false;
    }

    public IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields => _fields;

    public void Persist(ref SpanWriter writer, DescriptorWriter nestedDescriptorWriter)
    {
        writer.WriteString(Name);
        writer.WriteVUInt32(_fields.Count);
        foreach (var pair in _fields)
        {
            writer.WriteString(pair.Key);
            nestedDescriptorWriter(ref writer, pair.Value);
        }
    }

    public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx,
        Action<IILGen> pushValue, Type valueType)
    {
        if (GetPreferredType() != valueType)
            throw new ArgumentException(
                $"value type {valueType.ToSimpleName()} does not match my type {GetPreferredType().ToSimpleName()}");
        var locValue = ilGenerator.DeclareLocal(_type!, "value");
        ilGenerator
            .Do(pushValue)
            .Stloc(locValue);
        foreach (var (name, typeDescriptor) in _fields)
        {
            var methodInfo = _type.GetProperties(_propertyBindingFlags).First(p => GetPersistentName(p) == name)
                .GetAnyGetMethod();
            typeDescriptor.GenerateSaveEx(ilGenerator, pushWriter, pushCtx,
                il => il.Ldloc(locValue).Callvirt(methodInfo), methodInfo!.ReturnType);
        }
    }

    public ITypeDescriptor CloneAndMapNestedTypes(ITypeDescriptorCallbacks typeSerializers,
        Func<ITypeDescriptor, ITypeDescriptor> map)
    {
        var tds = new ITypeDescriptor[_fields.Count];
        for (var i = 0; i < _fields.Count; i++)
        {
            tds[i] = map(_fields[i].Value);
        }

        if (typeSerializers == _typeSerializers && tds.SequenceEqual(_fields.Select(i => i.Value)))
            return this;
        var res = new ObjectTypeDescriptor(typeSerializers, Name, Sealed, _typeDescriptorOptions);
        for (var i = 0; i < _fields.Count; i++)
        {
            res._fields.Add(new KeyValuePair<string, ITypeDescriptor>(_fields[i].Key, tds[i]));
        }

        return res;
    }
}
