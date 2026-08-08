using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public class ODBLazyUlongListFieldHandler : IFieldHandler, IFieldHandlerWithInit
{
    public static string HandlerName => "ODBLazyUlongList";

    public string Name => HandlerName;

    public byte[] Configuration => [];

    public static bool IsCompatibleWithStatic(Type type, FieldHandlerOptions options)
    {
        if ((options & FieldHandlerOptions.Orderable) != 0) return false;
        return type == typeof(ILazyUlongList);
    }

    public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return IsCompatibleWithStatic(type, options);
    }

    public Type HandledType()
    {
        return typeof(ILazyUlongList);
    }

    public bool NeedsCtx()
    {
        return true;
    }

    public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        ilGenerator
            .Do(pushCtx)
            .Castclass(typeof(IDBReaderCtx))
            .Callvirt(() => default(IDBReaderCtx).GetTransaction())
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt64))!)
            .Newobj(() => new ODBLazyUlongList(null!, 0))
            .Castclass(typeof(ILazyUlongList));
    }

    public bool NeedInit()
    {
        return true;
    }

    public void Init(IILGen ilGenerator, Action<IILGen> pushReaderCtx)
    {
        ilGenerator
            .Do(pushReaderCtx)
            .Castclass(typeof(IDBReaderCtx))
            .Callvirt(() => default(IDBReaderCtx).GetTransaction())
            .Newobj(() => new ODBLazyUlongList(null!))
            .Castclass(typeof(ILazyUlongList));
    }

    public unsafe FieldHandlerInit Init()
    {
        return (ctx, ref value) =>
        {
            var transaction = ((IDBReaderCtx)ctx!).GetTransaction();
            Unsafe.As<byte, object>(ref value) = new ODBLazyUlongList(transaction);
        };
    }

    public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        ilGenerator
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.SkipVUInt64))!);
    }

    public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue)
    {
        ilGenerator
            .Do(pushWriter)
            .Do(pushCtx)
            .Do(pushValue)
            .Call(typeof(ODBLazyUlongList).GetMethod(nameof(ODBLazyUlongList.DoSave))!);
    }

    public unsafe FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (!IsCompatibleWithStatic(asType, FieldHandlerOptions.None))
            throw new BTDBException("Type " + asType.ToSimpleName() +
                                    " is not compatible with ODBLazyUlongListFieldHandler.Load");
        return (ref reader, ctx, ref value) =>
        {
            var id = reader.ReadVUInt64();
            Unsafe.As<byte, object>(ref value) = new ODBLazyUlongList(((IDBReaderCtx)ctx!).GetTransaction(), id);
        };
    }

    public void Skip(ref MemReader reader, IReaderCtx? ctx)
    {
        reader.SkipVUInt64();
    }

    public unsafe FieldHandlerSave Save(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (!IsCompatibleWithStatic(asType, FieldHandlerOptions.None))
            throw new BTDBException("Type " + asType.ToSimpleName() +
                                    " is not compatible with ODBLazyUlongListFieldHandler.Save");
        return (ref writer, ctx, ref value) =>
        {
            var list = Unsafe.As<byte, ILazyUlongList?>(ref value);
            ODBLazyUlongList.DoSave(ref writer, ctx!, list);
        };
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        return this;
    }

    public IFieldHandler SpecializeSaveForType(Type type)
    {
        return this;
    }

    public void FreeContent(ref MemReader reader, IReaderCtx? ctx)
    {
        ctx!.RegisterDict(reader.ReadVUInt64());
    }

    public bool DoesNeedFreeContent(HashSet<Type> visitedTypes)
    {
        return true;
    }
}
