using System;
using BTDB.Encrypted;
using BTDB.IL;

namespace BTDB.FieldHandler
{
    public class EncryptedStringHandler : IFieldHandler
    {
        public string Name => "EncryptedString";

        public byte[]? Configuration => null;

        public virtual bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return typeof(EncryptedString) == type;
        }

        public Type HandledType()
        {
            return typeof(EncryptedString);
        }

        public bool NeedsCtx()
        {
            return true;
        }

        public void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Callvirt(()=>((IReaderCtx)null).ReadEncryptedString());
        }

        public void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Callvirt(()=>((IReaderCtx)null).SkipEncryptedString());
        }

        public void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
        {
            pushWriterOrCtx(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Callvirt(()=>((IWriterCtx)null).WriteEncryptedString(default));
        }

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler)
        {
            if (HandledType() == type || DefaultTypeConvertorGenerator.Instance.GenerateConversion(typeof(EncryptedString), type)==null)
            {
                return this;
            }
            return new ConvertingHandler(this, type);
        }

        public NeedsFreeContent FreeContent(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            Skip(ilGenerator, pushReaderOrCtx);
            return NeedsFreeContent.No;
        }

        public class ConvertingHandler : IFieldHandler
        {
            readonly IFieldHandler _fieldHandler;
            readonly Type _type;

            public ConvertingHandler(IFieldHandler fieldHandler, Type type)
            {
                _fieldHandler = fieldHandler;
                _type = type;
            }

            public string Name => _fieldHandler.Name;
            public byte[]? Configuration => _fieldHandler.Configuration;

            public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
            {
                return _type == type;
            }

            public Type HandledType()
            {
                return _type;
            }

            public bool NeedsCtx()
            {
                return true;
            }

            public void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
            {
                _fieldHandler.Load(ilGenerator, pushReaderOrCtx);
                DefaultTypeConvertorGenerator.Instance.GenerateConversion(_fieldHandler.HandledType(), _type)!(ilGenerator);
            }

            public void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
            {
                _fieldHandler.Skip(ilGenerator, pushReaderOrCtx);
            }

            public void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
            {
                _fieldHandler.Save(ilGenerator, pushWriterOrCtx, il => il.Do(pushValue).Do(DefaultTypeConvertorGenerator.Instance.GenerateConversion(_type, _fieldHandler.HandledType())!));
            }

            public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler)
            {
                throw new InvalidOperationException();
            }

            public IFieldHandler SpecializeSaveForType(Type type)
            {
                throw new InvalidOperationException();
            }

            public NeedsFreeContent FreeContent(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
            {
                _fieldHandler.Skip(ilGenerator, pushReaderOrCtx);
                return NeedsFreeContent.No;
            }
        }

        public IFieldHandler SpecializeSaveForType(Type type)
        {
            if (HandledType() == type || DefaultTypeConvertorGenerator.Instance.GenerateConversion(type, typeof(EncryptedString))==null)
            {
                return this;
            }
            return new ConvertingHandler(this, type);
        }
    }
}
