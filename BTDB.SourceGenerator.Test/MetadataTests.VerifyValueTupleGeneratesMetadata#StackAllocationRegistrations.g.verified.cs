﻿//HintName: StackAllocationRegistrations.g.cs
// <auto-generated/>
#nullable enable
#pragma warning disable 612, 618, CS0649, CS8500
using System;
using System.Runtime.CompilerServices;
[CompilerGenerated]
static file class StackAllocationRegistrations
{
    struct ValueTuple1
    {
       public int Item1;
       public string Item2;
    }

    [ModuleInitializer]
    internal static unsafe void Register4BTDB()
    {
        BTDB.Serialization.ReflectionMetadata.RegisterStackAllocator(typeof((int, string)), &Allocate1);
        static void Allocate1(ref byte ctx, ref nint ptr, delegate*<ref byte, void> chain)
        {
            (int, string) value = default;
            ptr = (nint)Unsafe.AsPointer(ref value);
            chain(ref ctx);
            ptr = 0;
        }

        ValueTuple1 valueTuple1 = new();
        BTDB.Serialization.ReflectionMetadata.Register(new()
        {
            Type = typeof((int, string)),
            Name = "ValueTuple",
            Fields =
            [
                new()
                {
                    Name = "Item1",
                    Type = typeof(int),
                    ByteOffset = (uint)Unsafe.ByteOffset(ref Unsafe.As<ValueTuple1, byte>(ref valueTuple1),
                        ref Unsafe.As<int, byte>(ref valueTuple1.Item1)),
                },
                new()
                {
                    Name = "Item2",
                    Type = typeof(string),
                    ByteOffset = (uint)Unsafe.ByteOffset(ref Unsafe.As<ValueTuple1, byte>(ref valueTuple1),
                        ref Unsafe.As<string, byte>(ref valueTuple1.Item2)),
                },
            ]
        });
    }
}
