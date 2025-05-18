using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class ListFieldHandler : IFieldHandler, IFieldHandlerWithNestedFieldHandlers
{
    readonly IFieldHandlerFactory _fieldHandlerFactory;
    readonly ITypeConvertorGenerator _typeConvertGenerator;
    readonly IFieldHandler _itemsHandler;
    Type? _type;
    readonly bool _isSet;

    [SkipLocalsInit]
    public ListFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertGenerator,
        Type type)
    {
        _fieldHandlerFactory = fieldHandlerFactory;
        _typeConvertGenerator = typeConvertGenerator;
        _type = type;
        _isSet = type.InheritsOrImplements(typeof(ISet<>));
        _itemsHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0], FieldHandlerOptions.None);
        Span<byte> buf = stackalloc byte[1024];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        writer.WriteFieldHandler(_itemsHandler);
        Configuration = writer.GetSpan().ToArray();
    }

    public unsafe ListFieldHandler(IFieldHandlerFactory fieldHandlerFactory,
        ITypeConvertorGenerator typeConvertGenerator, byte[] configuration)
    {
        _fieldHandlerFactory = fieldHandlerFactory;
        _typeConvertGenerator = typeConvertGenerator;
        Configuration = configuration;
        fixed (void* confPtr = configuration)
        {
            var reader = new MemReader(confPtr, configuration.Length);
            _itemsHandler = _fieldHandlerFactory.CreateFromReader(ref reader, FieldHandlerOptions.None);
        }
    }

    ListFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertGenerator, Type type,
        IFieldHandler itemSpecialized)
    {
        _fieldHandlerFactory = fieldHandlerFactory;
        _typeConvertGenerator = typeConvertGenerator;
        _type = type;
        _isSet = type.InheritsOrImplements(typeof(ISet<>));
        _itemsHandler = itemSpecialized;
        Configuration = [];
    }

    public static string HandlerName => "List";

    public string Name => HandlerName;

    public byte[] Configuration { get; }

    public static bool IsCompatibleWith(Type type)
    {
        if (!type.IsGenericType) return false;
        return type.InheritsOrImplements(typeof(IList<>)) || type.InheritsOrImplements(typeof(ISet<>));
    }

    public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return IsCompatibleWith(type);
    }

    public Type HandledType()
    {
        return _type ??= _isSet
            ? typeof(ISet<>).MakeGenericType(_itemsHandler.HandledType()!)
            : typeof(IList<>).MakeGenericType(_itemsHandler.HandledType()!);
    }

    public bool NeedsCtx()
    {
        return true;
    }

    public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        var localCount = ilGenerator.DeclareLocal(typeof(uint));
        var localResultOfObject = ilGenerator.DeclareLocal(typeof(object));
        var localResult = ilGenerator.DeclareLocal(HandledType());
        var loadSkipped = ilGenerator.DefineLabel();
        var loadFinished = ilGenerator.DefineLabel();
        var finish = ilGenerator.DefineLabel();
        var next = ilGenerator.DefineLabel();
        var collectionInterface = _type!.SpecializationOf(typeof(ICollection<>));
        var itemType = collectionInterface!.GetGenericArguments()[0];
        ilGenerator
            .Do(pushCtx)
            .Do(pushReader)
            .Ldloca(localResultOfObject)
            .Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.ReadObject))!)
            .Brfalse(loadSkipped)
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt32))!)
            .Stloc(localCount)
            .Ldloc(localCount)
            .Newobj((_isSet ? typeof(HashSet<>) : typeof(List<>)).MakeGenericType(itemType)
                .GetConstructor(new[] { typeof(int) })!)
            .Stloc(localResult)
            .Do(pushCtx)
            .Ldloc(localResult)
            .Castclass(typeof(object))
            .Callvirt(() => default(IReaderCtx).RegisterObject(null))
            .Mark(next)
            .Ldloc(localCount)
            .Brfalse(loadFinished)
            .Ldloc(localCount)
            .LdcI4(1)
            .Sub()
            .ConvU4()
            .Stloc(localCount)
            .Ldloc(localResult)
            .GenerateLoad(_itemsHandler, itemType, pushReader, pushCtx, _typeConvertGenerator)
            .Callvirt(collectionInterface!.GetMethod("Add")!)
            .Br(next)
            .Mark(loadFinished)
            .Do(pushCtx)
            .Do(pushReader)
            .Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.ReadObjectDone))!)
            .Br(finish)
            .Mark(loadSkipped)
            .Ldloc(localResultOfObject)
            .Isinst(_type)
            .Stloc(localResult)
            .Mark(finish)
            .Ldloc(localResult);
    }

    public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        var localCount = ilGenerator.DeclareLocal(typeof(uint));
        var finish = ilGenerator.DefineLabel();
        var next = ilGenerator.DefineLabel();
        ilGenerator
            .Do(pushCtx)
            .Do(pushReader)
            .Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.SkipObject))!)
            .Brfalse(finish)
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt32))!)
            .Stloc(localCount)
            .Mark(next)
            .Ldloc(localCount)
            .Brfalse(finish)
            .Ldloc(localCount)
            .LdcI4(1)
            .Sub()
            .ConvU4()
            .Stloc(localCount)
            .GenerateSkip(_itemsHandler, pushReader, pushCtx)
            .Br(next)
            .Mark(finish);
    }

    public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue)
    {
        var realFinish = ilGenerator.DefineLabel();
        var finish = ilGenerator.DefineLabel();
        var next = ilGenerator.DefineLabel();
        var localValue = ilGenerator.DeclareLocal(_type!);
        var typeAsICollection = _type.GetInterface("ICollection`1");
        var typeAsIEnumerable = _type.GetInterface("IEnumerable`1");
        var getEnumeratorMethod = typeAsIEnumerable!.GetMethod("GetEnumerator");
        var typeAsIEnumerator = getEnumeratorMethod!.ReturnType;
        var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
        ilGenerator
            .Do(pushValue)
            .Stloc(localValue)
            .Do(pushCtx)
            .Do(pushWriter)
            .Ldloc(localValue)
            .Castclass(typeof(object))
            .Callvirt(typeof(IWriterCtx).GetMethod(nameof(IWriterCtx.WriteObject))!)
            .Brfalse(realFinish)
            .Do(pushWriter)
            .Ldloc(localValue)
            .Callvirt(typeAsICollection!.GetProperty("Count")!.GetGetMethod()!)
            .ConvU4()
            .Call(typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVUInt32))!)
            .Ldloc(localValue)
            .Callvirt(getEnumeratorMethod)
            .Stloc(localEnumerator)
            .Try()
            .Mark(next)
            .Ldloc(localEnumerator)
            .Callvirt(() => default(IEnumerator).MoveNext())
            .Brfalse(finish);
        _itemsHandler.Save(ilGenerator, pushWriter, pushCtx, il => il
            .Ldloc(localEnumerator)
            .Callvirt(typeAsIEnumerator.GetProperty("Current")!.GetGetMethod()!)
            .Do(_typeConvertGenerator.GenerateConversion(_type.GetGenericArguments()[0],
                _itemsHandler.HandledType())!));
        ilGenerator
            .Br(next)
            .Mark(finish)
            .Finally()
            .Ldloc(localEnumerator)
            .Callvirt(() => default(IDisposable).Dispose())
            .EndTry()
            .Mark(realFinish);
    }

    ref struct ListItemLoaderCtx
    {
        internal nint StoragePtr;
        internal object Object;
        internal FieldHandlerLoad ItemLoader;
        internal unsafe delegate*<object, ref byte, void> Adder;
        internal IReaderCtx Ctx;
        internal ref MemReader Reader;
    }

    public unsafe FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        var collectionMetadata = ReflectionMetadata.FindCollectionByType(asType!);
        if (collectionMetadata == null)
            throw new BTDBException("Cannot find collection metadata for " + _type.ToSimpleName());
        var itemLoad = _itemsHandler.Load(collectionMetadata.ElementKeyType, typeConverterFactory);
        var itemStackAllocator = ReflectionMetadata.FindStackAllocatorByType(collectionMetadata.ElementKeyType);

        return (ref MemReader reader, IReaderCtx? ctx, ref byte value) =>
        {
            if (ctx!.ReadObject(ref reader, out var obj))
            {
                var count = reader.ReadVUInt32();
                obj = collectionMetadata!.Creator(count);
                ctx.RegisterObject(obj);
                var loaderCtx = new ListItemLoaderCtx
                {
                    Adder = collectionMetadata.Adder,
                    ItemLoader = itemLoad,
                    Object = obj,
                    Ctx = ctx,
                    Reader = ref reader,
                };
                for (var i = 0; i != count; i++)
                {
                    itemStackAllocator(ref Unsafe.As<ListItemLoaderCtx, byte>(ref loaderCtx), ref loaderCtx.StoragePtr,
                        &Nested);

                    static void Nested(ref byte value)
                    {
                        ref var context = ref Unsafe.As<byte, ListItemLoaderCtx>(ref value);
                        context.ItemLoader(ref context.Reader, context.Ctx,
                            ref Unsafe.AsRef<byte>((void*)context.StoragePtr));
                        context.Adder(context.Object, ref Unsafe.AsRef<byte>((void*)context.StoragePtr));
                    }
                }

                ctx.ReadObjectDone(ref reader);
            }
            else
            {
                if (obj == null || !asType!.IsAssignableFrom(obj.GetType()))
                {
                    obj = null;
                }
            }

            Unsafe.As<byte, object?>(ref value) = obj;
        };
    }

    public void Skip(ref MemReader reader, IReaderCtx? ctx)
    {
        if (ctx!.SkipObject(ref reader))
        {
            var count = reader.ReadVUInt32();
            while (count != 0)
            {
                count--;
                _itemsHandler.Skip(ref reader, ctx);
            }
        }
    }

    public FieldHandlerSave Save(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (!IsCompatibleWith(asType))
            throw new InvalidOperationException("ListFieldHandler cannot save " + asType.ToSimpleName());
        var itemType = asType.GenericTypeArguments[0];
        var hashSetType = typeof(HashSet<>).MakeGenericType(itemType);
        var listType = typeof(List<>).MakeGenericType(itemType);
        var saveItem = _itemsHandler.Save(itemType, typeConverterFactory);
        var layout = RawData.GetHashSetEntriesLayout(itemType);
        return (ref MemWriter writer, IWriterCtx? ctx, ref byte value) =>
        {
            var obj = Unsafe.As<byte, object>(ref value);
            if (ctx!.WriteObject(ref writer, obj))
            {
                var objType = obj.GetType();
                if (listType.IsAssignableFrom(objType))
                {
                    var count = (uint)Unsafe.As<ICollection>(obj).Count;
                    writer.WriteVUInt32(count);
                    obj = RawData.ListItems(Unsafe.As<List<object>>(obj));
                    ref readonly var mt = ref RawData.MethodTableRef(obj);
                    var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                    var offsetDelta = mt.ComponentSize;
                    for (var i = 0; i < count; i++, offset += offsetDelta)
                    {
                        saveItem(ref writer, ctx, ref RawData.Ref(obj, offset));
                    }
                }
                else if (hashSetType.IsAssignableFrom(objType))
                {
                    var count = Unsafe.As<byte, uint>(ref RawData.Ref(obj,
                        RawData.Align(8 + 4 * (uint)Unsafe.SizeOf<nint>(), 8)));
                    writer.WriteVUInt32(count);
                    if (count != 0)
                    {
                        obj = RawData.HashSetEntries(Unsafe.As<HashSet<object>>(obj));
                        ref readonly var mt = ref RawData.MethodTableRef(obj);
                        var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                        var offsetDelta = mt.ComponentSize;
                        if (offsetDelta != layout.Size)
                            throw new BTDBException("Invalid HashSet layout " + offsetDelta + " != " + layout.Size);
                        for (var i = 0; i < count; i++, offset += offsetDelta)
                        {
                            if (Unsafe.As<byte, int>(ref RawData.Ref(obj, offset + 4)) < -1)
                            {
                                continue;
                            }

                            saveItem(ref writer, ctx, ref RawData.Ref(obj, offset + layout.Offset));
                        }
                    }
                }
                else throw new BTDBException("Cannot save type " + objType.ToSimpleName());
            }
        };
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        if (_type == type) return this;
        if (!IsCompatibleWith(type))
        {
            return this;
        }

        var wantedItemType = type.GetGenericArguments()[0];
        var wantedItemHandler = default(IFieldHandler);
        if (typeHandler is ListFieldHandler listFieldHandler)
        {
            wantedItemHandler = listFieldHandler._itemsHandler;
        }

        var itemSpecialized = _itemsHandler.SpecializeLoadForType(wantedItemType, wantedItemHandler, logger);
        if (itemSpecialized == wantedItemHandler)
        {
            return typeHandler;
        }

        if (_typeConvertGenerator.GenerateConversion(itemSpecialized.HandledType(), wantedItemType) == null)
        {
            return this;
        }

        return new ListFieldHandler(_fieldHandlerFactory, _typeConvertGenerator, type,
            itemSpecialized);
    }

    public IFieldHandler SpecializeSaveForType(Type type)
    {
        if (_type == type) return this;
        if (!IsCompatibleWith(type))
        {
            Debug.Fail("strange");
            return this;
        }

        var wantedItemType = type.GetGenericArguments()[0];
        var itemSpecialized = _itemsHandler.SpecializeSaveForType(wantedItemType);
        if (_typeConvertGenerator.GenerateConversion(wantedItemType, itemSpecialized.HandledType()) == null)
        {
            Debug.Fail("even more strange");
            return this;
        }

        return new ListFieldHandler(_fieldHandlerFactory, _typeConvertGenerator, type,
            itemSpecialized);
    }

    public IEnumerable<IFieldHandler> EnumerateNestedFieldHandlers()
    {
        yield return _itemsHandler;
    }

    public void FreeContent(ref MemReader reader, IReaderCtx? ctx)
    {
        if (ctx!.SkipObject(ref reader))
        {
            var count = reader.ReadVUInt32();
            for (var i = 0; i != count; i++)
            {
                _itemsHandler.FreeContent(ref reader, ctx);
            }
        }
    }

    public bool DoesNeedFreeContent(HashSet<Type> visitedTypes)
    {
        return _itemsHandler.DoesNeedFreeContent(visitedTypes);
    }
}
