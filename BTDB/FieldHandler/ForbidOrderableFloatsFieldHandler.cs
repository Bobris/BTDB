using System;
using System.Collections.Generic;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class ForbidOrderableFloatsFieldHandler : IFieldHandler
{
    public string Name => "ForbidOrderableFloats";
    public byte[]? Configuration => null;

    public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        if ((options & FieldHandlerOptions.Orderable) == 0) return false;
        if (type == typeof(float) || type == typeof(double))
        {
            throw new NotSupportedException($"Type {type} is not supported as orderable field.");
        }

        return false;
    }

    public Type? HandledType() => null;

    public bool NeedsCtx() => false;

    public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        throw new NotSupportedException();
    }

    public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        throw new NotSupportedException();
    }

    public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx, Action<IILGen> pushValue)
    {
        throw new NotSupportedException();
    }

    public void Skip(ref MemReader reader, IReaderCtx? ctx)
    {
        throw new NotSupportedException();
    }

    public void FreeContent(ref MemReader reader, IReaderCtx? ctx)
    {
        throw new NotSupportedException();
    }

    public bool DoesNeedFreeContent(HashSet<Type> visitedTypes)
    {
        throw new NotSupportedException();
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        throw new NotSupportedException();
    }

    public IFieldHandler SpecializeSaveForType(Type type)
    {
        throw new NotSupportedException();
    }
}
