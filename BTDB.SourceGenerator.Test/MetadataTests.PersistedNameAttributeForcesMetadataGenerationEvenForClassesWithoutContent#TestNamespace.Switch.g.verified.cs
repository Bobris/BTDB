﻿//HintName: TestNamespace.Switch.g.cs
// <auto-generated/>
#pragma warning disable 612,618
using System;
using System.Runtime.CompilerServices;

namespace TestNamespace;

[CompilerGenerated]
static file class SwitchRegistration
{
    [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
    extern static global::TestNamespace.Switch Creator();
    [ModuleInitializer]
    internal static unsafe void Register4BTDB()
    {
        global::BTDB.IOC.IContainer.RegisterFactory(typeof(global::TestNamespace.Switch), (container, ctx) =>
        {
            return (container2, ctx2) =>
            {
                var res = new global::TestNamespace.Switch();
                return res;
            };
        });
        var metadata = new global::BTDB.Serialization.ClassMetadata();
        metadata.Name = "Switch";
        metadata.Type = typeof(global::TestNamespace.Switch);
        metadata.Namespace = "TestNamespace";
        metadata.PersistedName = "Hello";
        metadata.Implements = [];
        metadata.Creator = &Creator;
        var dummy = Unsafe.As<global::TestNamespace.Switch>(metadata);
        metadata.Fields = new[]
        {
        };
        global::BTDB.Serialization.ReflectionMetadata.Register(metadata);
    }
}
