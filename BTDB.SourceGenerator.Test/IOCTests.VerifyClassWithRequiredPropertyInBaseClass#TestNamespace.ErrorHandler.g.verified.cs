﻿//HintName: TestNamespace.ErrorHandler.g.cs
// <auto-generated/>
#pragma warning disable 612,618
using System;
using System.Runtime.CompilerServices;

namespace TestNamespace;

[CompilerGenerated]
static file class ErrorHandlerRegistration
{
    [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
    extern static global::TestNamespace.ErrorHandler Constr();
    [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
    extern static global::TestNamespace.ErrorHandler Creator();
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "<Prop>k__BackingField")]
    extern static ref int Field1(global::TestNamespace.BaseClass @this);
    [ModuleInitializer]
    internal static unsafe void Register4BTDB()
    {
        global::BTDB.IOC.IContainer.RegisterFactory(typeof(global::TestNamespace.ErrorHandler), (container, ctx) =>
        {
            return (container2, ctx2) =>
            {
                var res = Constr();
                return res;
            };
        });
        var metadata = new global::BTDB.Serialization.ClassMetadata();
        metadata.Name = "ErrorHandler";
        metadata.Type = typeof(global::TestNamespace.ErrorHandler);
        metadata.Namespace = "TestNamespace";
        metadata.Implements = [];
        metadata.Creator = &Creator;
        var dummy = Unsafe.As<global::TestNamespace.ErrorHandler>(metadata);
        metadata.Fields = [
            new global::BTDB.Serialization.FieldMetadata
            {
                Name = "Prop",
                Type = typeof(int),
                ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref Field1(dummy)),
            },
        ];
        global::BTDB.Serialization.ReflectionMetadata.Register(metadata);
    }
}
