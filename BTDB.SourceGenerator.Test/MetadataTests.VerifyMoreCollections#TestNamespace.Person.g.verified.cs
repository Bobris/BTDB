﻿//HintName: TestNamespace.Person.g.cs
// <auto-generated/>
#pragma warning disable 612,618
using System;
using System.Runtime.CompilerServices;

namespace TestNamespace;

[CompilerGenerated]
static file class PersonRegistration
{
    [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
    extern static global::TestNamespace.Person Creator();
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "<Name>k__BackingField")]
    extern static ref string Field1(global::TestNamespace.Person @this);
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "<Id2Names>k__BackingField")]
    extern static ref global::System.Collections.Generic.IDictionary<int, global::System.Collections.Generic.IList<string>> Field2(global::TestNamespace.Person @this);
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "<Id2Names2>k__BackingField")]
    extern static ref global::System.Collections.Generic.IReadOnlyDictionary<int, global::System.Collections.Generic.IReadOnlyList<string>> Field3(global::TestNamespace.Person @this);
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "<Names>k__BackingField")]
    extern static ref global::System.Collections.Generic.IEnumerable<string> Field4(global::TestNamespace.Person @this);
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "<NameSet>k__BackingField")]
    extern static ref global::System.Collections.Generic.ISet<string> Field5(global::TestNamespace.Person @this);
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "<NameSet2>k__BackingField")]
    extern static ref global::System.Collections.Generic.IReadOnlySet<string> Field6(global::TestNamespace.Person @this);
    [ModuleInitializer]
    internal static unsafe void Register4BTDB()
    {
        global::BTDB.IOC.IContainer.RegisterFactory(typeof(global::TestNamespace.Person), (container, ctx) =>
        {
            return (container2, ctx2) =>
            {
                var res = new global::TestNamespace.Person();
                return res;
            };
        });
        var metadata = new global::BTDB.Serialization.ClassMetadata();
        metadata.Name = "Person";
        metadata.Type = typeof(global::TestNamespace.Person);
        metadata.Namespace = "TestNamespace";
        metadata.Implements = [];
        metadata.Creator = &Creator;
        var dummy = Unsafe.As<global::TestNamespace.Person>(metadata);
        metadata.Fields = new global::BTDB.Serialization.FieldMetadata[]
        {
            new global::BTDB.Serialization.FieldMetadata
            {
                Name = "Name",
                Type = typeof(string),
                ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref Field1(dummy)),
            },
            new global::BTDB.Serialization.FieldMetadata
            {
                Name = "Id2Names",
                Type = typeof(global::System.Collections.Generic.IDictionary<int, global::System.Collections.Generic.IList<string>>),
                ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref Field2(dummy)),
            },
            new global::BTDB.Serialization.FieldMetadata
            {
                Name = "Id2Names2",
                Type = typeof(global::System.Collections.Generic.IReadOnlyDictionary<int, global::System.Collections.Generic.IReadOnlyList<string>>),
                ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref Field3(dummy)),
            },
            new global::BTDB.Serialization.FieldMetadata
            {
                Name = "Names",
                Type = typeof(global::System.Collections.Generic.IEnumerable<string>),
                ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref Field4(dummy)),
            },
            new global::BTDB.Serialization.FieldMetadata
            {
                Name = "NameSet",
                Type = typeof(global::System.Collections.Generic.ISet<string>),
                ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref Field5(dummy)),
            },
            new global::BTDB.Serialization.FieldMetadata
            {
                Name = "NameSet2",
                Type = typeof(global::System.Collections.Generic.IReadOnlySet<string>),
                ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref Field6(dummy)),
            },
        };
        global::BTDB.Serialization.ReflectionMetadata.Register(metadata);
    }
}
