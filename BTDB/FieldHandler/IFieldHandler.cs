using System;
using System.Collections.Generic;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public interface IFieldHandler
{
    string Name { get; }
    byte[]? Configuration { get; }
    bool IsCompatibleWith(Type type, FieldHandlerOptions options);
    Type? HandledType();
    bool NeedsCtx();
    void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx);
    void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx);
    void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx, Action<IILGen> pushValue);

    void Skip(ref MemReader reader, IReaderCtx? ctx);

    void FreeContent(ref MemReader reader, IReaderCtx? ctx);
    bool DoesNeedFreeContent(HashSet<Type> visitedTypes);

    // typeHandler is preferred FieldHandler for type could be null if unknown
    IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger);
    IFieldHandler SpecializeSaveForType(Type type);
    bool DoesPreferLoadAsMemory() => false;
}
