﻿//HintName: System.Tuple[int,string].g.cs
// <auto-generated/>
#pragma warning disable 612,618
using System;
using System.Runtime.CompilerServices;

namespace System;

[CompilerGenerated]
static file class TupleRegistration
{
    class TupleStunt
    {
        public int Item1;
        public string Item2;
    }

    static object Creator()
    {
        return RuntimeHelpers.GetUninitializedObject(typeof(global::System.Tuple<int, string>));
    }
    [ModuleInitializer]
    internal static unsafe void Register4BTDB()
    {
        global::BTDB.IOC.IContainer.RegisterFactory(typeof(global::System.Tuple<int, string>), (container, ctx) =>
        {
            var f0 = container.CreateFactory(ctx, typeof(int), "item1");
            if (f0 == null) throw new global::System.ArgumentException("Cannot resolve int item1 parameter of System.Tuple<int, string>");
            var f1 = container.CreateFactory(ctx, typeof(string), "item2");
            if (f1 == null) throw new global::System.ArgumentException("Cannot resolve string item2 parameter of System.Tuple<int, string>");
            return (container2, ctx2) =>
            {
                var res = new global::System.Tuple<int, string>((int)(f0(container2, ctx2)), Unsafe.As<string>(f1(container2, ctx2)));
                return res;
            };
        });
        var metadata = new global::BTDB.Serialization.ClassMetadata();
        metadata.Name = "Tuple";
        metadata.Type = typeof(global::System.Tuple<int, string>);
        metadata.Namespace = "System";
        metadata.Implements = [typeof(global::System.Collections.IStructuralEquatable), typeof(global::System.Collections.IStructuralComparable), typeof(global::System.IComparable), typeof(global::System.Runtime.CompilerServices.ITuple)];
        metadata.Creator = &Creator;
        var dummy = Unsafe.As<TupleStunt>(metadata);
        metadata.Fields = [
            new global::BTDB.Serialization.FieldMetadata
            {
                Name = "Item1",
                Type = typeof(int),
                ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref dummy.Item1),
            },
            new global::BTDB.Serialization.FieldMetadata
            {
                Name = "Item2",
                Type = typeof(string),
                ByteOffset = global::BTDB.Serialization.RawData.CalcOffset(dummy, ref dummy.Item2),
            },
        ];
        global::BTDB.Serialization.ReflectionMetadata.Register(metadata);
    }
}
