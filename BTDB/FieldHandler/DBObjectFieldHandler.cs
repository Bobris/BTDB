﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using BTDB.Collections;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class DBObjectFieldHandler : IFieldHandler, IFieldHandlerWithInit, IFieldHandlerWithRegister
{
    readonly IObjectDB _objectDb;
    readonly byte[] _configuration;
    readonly string? _typeName;
    readonly bool _indirect;
    Type? _type;

    public DBObjectFieldHandler(IObjectDB objectDb, Type type)
    {
        _objectDb = objectDb;
        _type = Unwrap(type);
        _indirect = _type != type;
        if (_type.IsInterface || _type.IsAbstract)
        {
            _typeName = null;
            _configuration = Array.Empty<byte>();
        }
        else
        {
            _typeName = (_objectDb as ObjectDB)?.RegisterType(_type, false);
            Span<byte> buf = stackalloc byte[256];
            var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
            writer.WriteString(_typeName);
            _configuration = writer.GetSpan().ToArray();
        }
    }

    public static Type Unwrap(Type type)
    {
        if (IsIIndirect(type))
        {
            return type.GetGenericArguments()[0];
        }

        var indType = type.GetInterfaces().FirstOrDefault(IsIIndirect);
        if (indType == null) return type;
        return indType.GetGenericArguments()[0];
    }

    static bool IsIIndirect(Type ti)
    {
        return ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(IIndirect<>);
    }

    public unsafe DBObjectFieldHandler(IObjectDB objectDb, byte[] configuration)
    {
        _objectDb = objectDb;
        _configuration = configuration;
        if (configuration.Length == 0)
        {
            _typeName = null;
        }
        else
        {
            fixed (void* confPtr = &configuration[0])
                _typeName = string.Intern(new MemReader(confPtr, configuration.Length).ReadString()!);
            _indirect = false;
        }

        CreateType();
    }

    public DBObjectFieldHandler(IObjectDB objectDb, Type type, bool indirect) : this(objectDb, type)
    {
        _objectDb = objectDb;
        _type = type;
        _typeName = null;
        _indirect = indirect;
    }

    Type? CreateType()
    {
        if (_typeName == null)
        {
            return _type = typeof(object);
        }

        return _type = _objectDb.TypeByName(_typeName);
    }

    public static string HandlerName => "Object";

    public string Name => HandlerName;

    public byte[] Configuration => _configuration;

    public static bool IsCompatibleWith(Type type)
    {
        type = Unwrap(type);
        return (!type.IsValueType && !type.IsArray && !type.IsSubclassOf(typeof(Delegate)));
    }

    public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        if ((options & FieldHandlerOptions.Orderable) != 0) return false;
        return IsCompatibleWith(type);
    }

    public Type HandledType()
    {
        var type = _type ?? CreateType() ?? typeof(object);
        if (_indirect) return typeof(IIndirect<>).MakeGenericType(type);
        return type;
    }

    public bool NeedsCtx()
    {
        return true;
    }

    public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        if (_indirect)
        {
            ilGenerator
                .Do(pushReader)
                .Do(pushCtx)
                .Call(typeof(DBIndirect<>).MakeGenericType(_type!).GetMethod(nameof(DBIndirect<object>.LoadImpl))!);
            return;
        }

        ilGenerator
            .Do(pushCtx)
            .Do(pushReader)
            .Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.ReadNativeObject))!);
        var type = HandledType();
        ilGenerator.Do(_objectDb.TypeConvertorGenerator.GenerateConversion(typeof(object), type)!);
    }

    public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        ilGenerator
            .Do(pushCtx)
            .Do(pushReader)
            .Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.SkipNativeObject))!);
    }

    public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue)
    {
        if (_indirect)
        {
            ilGenerator
                .Do(pushWriter)
                .Do(pushCtx)
                .Do(pushValue)
                .Call(typeof(DBIndirect<>).MakeGenericType(_type!).GetMethod(nameof(DBIndirect<object>.SaveImpl))!);
            return;
        }

        ilGenerator
            .Do(pushCtx)
            .Do(pushWriter)
            .Do(pushValue)
            .Do(_objectDb.TypeConvertorGenerator.GenerateConversion(HandledType(), typeof(object))!)
            .Callvirt(typeof(IWriterCtx).GetMethod(nameof(IWriterCtx.WriteNativeObject))!);
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        var needType = Unwrap(type);
        if (needType.IsInterface || needType.IsAbstract || type != needType && needType == HandledType())
        {
            return new DBObjectFieldHandler(_objectDb, needType, needType != type);
        }

        return this;
    }

    public IFieldHandler SpecializeSaveForType(Type type)
    {
        var needType = Unwrap(type);
        if (needType.IsInterface)
        {
            return new DBObjectFieldHandler(_objectDb, needType, needType != type);
        }

        return this;
    }

    public bool NeedInit()
    {
        return _indirect;
    }

    public void Init(IILGen ilGenerator, Action<IILGen> pushReaderCtx)
    {
        ilGenerator.Newobj(typeof(DBIndirect<>).MakeGenericType(_type!).GetDefaultConstructor()!);
    }

    public NeedsFreeContent FreeContent(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        var needsFreeContent = NeedsFreeContent.No;
        var type = HandledType();
        if (type == typeof(object))
        {
            needsFreeContent = NeedsFreeContent.Yes;
        }
        else
        {
            foreach (var st in _objectDb.GetPolymorphicTypes(type))
            {
                UpdateNeedsFreeContent(st, ref needsFreeContent);
                if (needsFreeContent == NeedsFreeContent.Yes) break;
            }

            if (!type.IsInterface && !type.IsAbstract)
                UpdateNeedsFreeContent(type, ref needsFreeContent);
        }
        ilGenerator
            .Do(pushCtx)
            .Do(pushReader)
            .Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.FreeContentInNativeObject))!);
        return needsFreeContent;
    }

    static ThreadLocal<HashSet<Type>> StackOverflowProtection { get; } = new(() => new());

    void UpdateNeedsFreeContent(Type type, ref NeedsFreeContent needsFreeContent)
    {
        if (type.IsValueType) return;
        //decides upon current version  (null for object types never stored in DB)
        var tableInfo = ((ObjectDB)_objectDb).TablesInfo.FindByType(type);
        if (tableInfo == null)
        {
            if (StackOverflowProtection.Value!.Contains(type)) return;
            StackOverflowProtection.Value.Add(type);
            try
            {
                if (type.GetCustomAttribute(typeof(RequireContentFreeAttribute)) != null)
                {
                    Extensions.UpdateNeedsFreeContent(NeedsFreeContent.Yes, ref needsFreeContent);
                    return;
                }

                var publicFields = type.GetFields(BindingFlags.Public | BindingFlags.Instance);
                foreach (var field in publicFields)
                {
                    if (field.GetCustomAttribute<NotStoredAttribute>(true) != null) continue;
                    throw new BTDBException(
                        $"Public field {type.ToSimpleName()}.{field.Name} must have NotStoredAttribute. It is just intermittent, until they can start to be supported.");
                }

                var props = type.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                var fields = new StructList<TableFieldInfo>();
                fields.Reserve((uint)props.Length);
                var method = ILBuilder.Instance.NewMethod<ObjectFreeContent>($"Dummy");
                var ilGenerator = method.Generator;
                foreach (var pi in props)
                {
                    if (pi.GetCustomAttribute<NotStoredAttribute>(true) != null) continue;
                    if (pi.GetIndexParameters().Length != 0) continue;
                    var fieldInfo = TableFieldInfo.Build(Name, pi, _objectDb.FieldHandlerFactory,
                        FieldHandlerOptions.None, pi.GetCustomAttribute<PrimaryKeyAttribute>()?.InKeyValue ?? false);
                    Extensions.UpdateNeedsFreeContent(
                        fieldInfo.Handler!.FreeContent(ilGenerator, _ => { }, _ => { }),
                        ref needsFreeContent);
                }
            }
            finally
            {
                StackOverflowProtection.Value.Remove(type);
            }

            return;
        }

        var needsContentPartial =
            tableInfo?.IsFreeContentNeeded(tableInfo.ClientTypeVersion) ?? NeedsFreeContent.Unknown;
        Extensions.UpdateNeedsFreeContent(needsContentPartial, ref needsFreeContent);
    }

    public void Register(object owner)
    {
        if (_type != null)
            (owner as ObjectDB)?.RegisterType(_type, false);
    }
}
