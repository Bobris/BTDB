﻿//HintName: TestNamespace.RecordSealedGenerated.g.cs
// <auto-generated/>
#pragma warning disable 612,618
using System;
using System.Runtime.CompilerServices;

namespace TestNamespace;

[CompilerGenerated]
static file class RecordSealedGeneratedRegistration
{
    [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
    extern static global::TestNamespace.RecordSealedGenerated Creator();
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "<Name>k__BackingField")]
    extern static ref string Field1(global::TestNamespace.RecordSealedGenerated @this);
    [ModuleInitializer]
    internal static unsafe void Register4BTDB()
    {
        global::BTDB.IOC.IContainer.RegisterFactory(typeof(global::TestNamespace.RecordSealedGenerated), (container, ctx) =>
        {
            return (container2, ctx2) =>
            {
                var res = new global::TestNamespace.RecordSealedGenerated();
                return res;
            };
        });
        var metadata = new global::BTDB.Serialization.ClassMetadata();
        metadata.Name = "RecordSealedGenerated";
        metadata.Type = typeof(global::TestNamespace.RecordSealedGenerated);
        metadata.Namespace = "TestNamespace";
        metadata.Implements = [typeof(global::System.IEquatable<global::TestNamespace.RecordSealedGenerated>)];
        metadata.Creator = &Creator;
        var dummy = Unsafe.As<global::TestNamespace.RecordSealedGenerated>(metadata);
        metadata.Fields = [
            new global::BTDB.Serialization.FieldMetadata
            {
                Name = "Name",
                Type = typeof(string),
                ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref Field1(dummy)),
            },
        ];
        global::BTDB.Serialization.ReflectionMetadata.Register(metadata);
    }
}
