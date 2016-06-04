﻿using System;
using BTDB.IL;

namespace BTDB.FieldHandler
{
    public interface IFieldHandler
    {
        string Name { get; }
        byte[] Configuration { get; }
        bool IsCompatibleWith(Type type, FieldHandlerOptions options);
        Type HandledType();
        bool NeedsCtx();
        void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx);
        void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx);
        void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue);

        bool FreeContent(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx); //returns true if needs to be called

        // typeHandler is preferred FieldHandler for type could be null if unknown
        IFieldHandler SpecializeLoadForType(Type type, IFieldHandler typeHandler);
        IFieldHandler SpecializeSaveForType(Type type);
    }
}
