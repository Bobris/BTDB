using System;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class DBObjectFieldHandler : IFieldHandler
    {
        public string Name
        {
            get { return "DBObject"; }
        }

        public byte[] Configuration
        {
            get { return null; }
        }

        public bool IsCompatibleWith(Type type)
        {
            return (!type.IsInterface && !type.IsValueType && !type.IsGenericType);
        }

        public Type HandledType()
        {
            return null;
        }

        public void Load(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushCtx)
        {
            throw new NotImplementedException();
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushCtx)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(() => ((AbstractBufferedReader)null).SkipVInt64());
        }

        public void Save(ILGenerator ilGenerator, Action<ILGenerator> pushWriter, Action<ILGenerator> pushCtx, Action<ILGenerator> pushValue)
        {
            throw new NotImplementedException();
        }

        public void InformAboutDestinationHandler(IFieldHandler dstHandler)
        {
        }
    }
}