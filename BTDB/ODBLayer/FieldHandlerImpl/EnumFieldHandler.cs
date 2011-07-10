using System;
using System.Linq;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class EnumFieldHandler : IFieldHandler
    {
        readonly bool _signed;
        Type _enumType;
        readonly byte[] _configuration;

        public EnumFieldHandler(Type enumType)
        {
            if (!IsCompatibleWith(enumType)) throw new ArgumentException("enumType");
            _enumType = enumType;
            var enumUnderlyingType = _enumType.GetEnumUnderlyingType();
            _signed = SignedFieldHandler.IsCompatibleWith(enumUnderlyingType);
            var writer = new ByteArrayWriter();
            writer.WriteBool(_signed);
            writer.Dispose();
            _configuration = writer.Data;
        }

        public EnumFieldHandler(byte[] configuration)
        {
            _configuration = configuration;
            var reader = new ByteArrayReader(_configuration);
            _signed = reader.ReadBool();
        }

        public static string HandlerName
        {
            get { return "Enum"; }
        }

        public string Name
        {
            get { return HandlerName; }
        }

        public byte[] Configuration
        {
            get { return _configuration; }
        }

        public static bool IsCompatibleWith(Type type)
        {
            if (!type.IsEnum) return false;
            var enumUnderlyingType = type.GetEnumUnderlyingType();
            return SignedFieldHandler.IsCompatibleWith(enumUnderlyingType) || UnsignedFieldHandler.IsCompatibleWith(enumUnderlyingType);
        }

        bool IFieldHandler.IsCompatibleWith(Type type)
        {
            return IsCompatibleWith(type);
        }

        public bool LoadToSameHandler(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushThis, Type implType, string destFieldName)
        {
            return false;
        }

        public Type WillLoad()
        {
            if (_enumType == null)
            {
                throw new NotImplementedException();
            }
            return _enumType;
        }

        public void LoadToWillLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            Type typeRead;
            if (_signed)
            {
                ilGenerator.Call(() => ((AbstractBufferedReader)null).ReadVInt64());
                typeRead = typeof(long);
            }
            else
            {
                ilGenerator.Call(() => ((AbstractBufferedReader)null).ReadVUInt64());
                typeRead = typeof(ulong);
            }
            new DefaultTypeConvertorGenerator().GenerateConversion(typeRead, _enumType.GetEnumUnderlyingType())(ilGenerator);
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            if (_signed)
            {
                ilGenerator.Call(() => ((AbstractBufferedReader)null).SkipVInt64());
            }
            else
            {
                ilGenerator.Call(() => ((AbstractBufferedReader)null).SkipVUInt64());
            }
        }

        public void SaveFromWillLoad(ILGenerator ilGenerator, Action<ILGenerator> pushWriter, Action<ILGenerator> pushValue)
        {
            pushWriter(ilGenerator);
            pushValue(ilGenerator);
            if (_signed)
            {
                ilGenerator
                    .ConvI8()
                    .Call(() => ((AbstractBufferedWriter)null).WriteVInt64(0));
            }
            else
            {
                ilGenerator
                    .ConvU8()
                    .Call(() => ((AbstractBufferedWriter)null).WriteVUInt64(0));
            }
        }

        public void CreateStorage(FieldHandlerCreateImpl ctx)
        {
            ctx.CreateSimpleStorage();
        }

        public void CreatePropertyGetter(FieldHandlerCreateImpl ctx)
        {
            ctx.CreateSimplePropertyGetter();
        }

        public void CreatePropertySetter(FieldHandlerCreateImpl ctx)
        {
            ctx.CreateSimplePropertySetter();
        }

        public void CreateSaver(FieldHandlerCreateImpl ctx)
        {
            ctx.Generator
                .Ldloc(1)
                .Ldloc(0)
                .Ldfld(ctx.DefaultFieldBuilder);
            if (_signed)
            {
                ctx.Generator
                    .ConvI8()
                    .Call(() => ((AbstractBufferedWriter)null).WriteVInt64(0));
            }
            else
            {
                ctx.Generator
                    .ConvU8()
                    .Call(() => ((AbstractBufferedWriter)null).WriteVUInt64(0));
            }
        }

        public void InformAboutDestinationHandler(IFieldHandler dstHandler)
        {
            if (_enumType != null) return;
            if ((dstHandler is EnumFieldHandler) == false) return;
            if (dstHandler.Configuration.SequenceEqual(Configuration))
            {
                _enumType = dstHandler.WillLoad();
            }
        }
    }
}