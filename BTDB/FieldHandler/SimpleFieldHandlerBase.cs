using System;
using System.Reflection;
using BTDB.IL;

namespace BTDB.FieldHandler
{
    public class SimpleFieldHandlerBase : IFieldHandler
    {
        readonly string _name;
        readonly MethodInfo _loader;
        readonly MethodInfo _skipper;
        readonly MethodInfo _saver;

        public SimpleFieldHandlerBase(string name, MethodInfo loader, MethodInfo skipper, MethodInfo saver)
        {
            _name = name;
            _loader = loader;
            _skipper = skipper;
            _saver = saver;
        }

        public string Name => _name;

        public byte[] Configuration => null;

        public virtual bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return _loader.ReturnType == type;
        }

        public Type HandledType()
        {
            return _loader.ReturnType;
        }

        public bool NeedsCtx()
        {
            return false;
        }

        public void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(_loader);
        }

        public void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            if (_skipper == null) return;
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(_skipper);
        }

        public void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
        {
            pushWriterOrCtx(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(_saver);
        }

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler typeHandler)
        {
            if (HandledType() == type || !IsCompatibleWith(type, FieldHandlerOptions.None))
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
            public byte[] Configuration => _fieldHandler.Configuration;

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
                return false;
            }

            public void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
            {
                _fieldHandler.Load(ilGenerator, pushReaderOrCtx);
                new DefaultTypeConvertorGenerator().GenerateConversion(_fieldHandler.HandledType(), _type)(ilGenerator);
            }

            public void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
            {
                _fieldHandler.Skip(ilGenerator, pushReaderOrCtx);
            }

            public void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
            {
                _fieldHandler.Save(ilGenerator, pushWriterOrCtx, il => il.Do(pushValue).Do(new DefaultTypeConvertorGenerator().GenerateConversion(_type, _fieldHandler.HandledType())));
            }

            public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler typeHandler)
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
            if (HandledType() == type || !IsCompatibleWith(type, FieldHandlerOptions.None))
            {
                return this;
            }
            return new ConvertingHandler(this, type);
        }
    }
}