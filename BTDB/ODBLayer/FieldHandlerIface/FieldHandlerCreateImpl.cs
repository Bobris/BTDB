using System;
using System.Collections.Generic;
using System.Diagnostics.SymbolStore;
using System.Reflection;
using System.Reflection.Emit;
using BTDB.IL;

namespace BTDB.ODBLayer
{
    public class FieldHandlerCreateImpl
    {
        public TypeBuilder ImplType { get; set; }
        public ISymbolDocumentWriter SymbolDocWriter { get; set; }
        public string FieldName { get; set; }
        public ILGenerator Generator { get; set; }
        public Dictionary<string, object> ObjectStorage { get; set; }
        public PropertyInfo PropertyInfo { get; set; }
        public Action<ILGenerator> CallObjectModified { get; set; }
        public FieldBuilder FieldMidLevelDBTransaction { get; set; }

        public string DefaultFieldName { get { return "_FieldStorage_" + FieldName; } }
        
        public FieldBuilder DefaultFieldBuilder { get { return ObjectStorage[DefaultFieldName] as FieldBuilder; } }

        public void CreateSimpleStorage()
        {
            CreateSimpleStorage(PropertyInfo.PropertyType);
        }

        public void CreateSimpleStorage(Type ofType)
        {
            var defaultFieldName = DefaultFieldName;
            var fieldBuilder = ImplType.DefineField(defaultFieldName, ofType, FieldAttributes.Public);
            ObjectStorage[defaultFieldName] = fieldBuilder;
        }

        public void CreateSimplePropertyGetter()
        {
            Generator
                .Ldarg(0)
                .Ldfld(DefaultFieldBuilder);
        }

        public void CreateSimplePropertySetter()
        {
            var labelNoChange = Generator.DefineLabel();
            EmitHelpers.GenerateJumpIfEqual(Generator, PropertyInfo.PropertyType, labelNoChange,
                                            g => g.Ldarg(0).Ldfld(DefaultFieldBuilder),
                                            g => g.Ldarg(1));
            Generator
                .Ldarg(0)
                .Ldarg(1)
                .Stfld(DefaultFieldBuilder);
            CallObjectModified(Generator);
            Generator.Mark(labelNoChange);
        }
    }
}