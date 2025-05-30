﻿//HintName: CollectionRegistrations.g.cs
// <auto-generated/>
#nullable enable
#pragma warning disable 612, 618, CS0649
using System;
using System.Runtime.CompilerServices;
[CompilerGenerated]
static file class CollectionRegistrations
{
    struct DictEntry<TKey, TValue>
    {
        public uint HashCode;
        public int Next;
        public TKey Key;
        public TValue Value;
    }

    struct HashSetEntry<T>
    {
        public uint HashCode;
        public int Next;
        public T Value;
    }

    [ModuleInitializer]
    internal static unsafe void Register4BTDB()
    {
        HashSetEntry<global::TestNamespace.Person> e1 = new();
        BTDB.Serialization.ReflectionMetadata.RegisterCollection(new()
        {
            Type = typeof(global::System.Collections.Generic.List<global::TestNamespace.Person>),
            ElementKeyType = typeof(global::TestNamespace.Person),
            OffsetNext = (uint)Unsafe.ByteOffset(ref Unsafe.As<HashSetEntry<global::TestNamespace.Person>, byte>(ref e1),
                ref Unsafe.As<int, byte>(ref e1.Next)),
            OffsetKey = (uint)Unsafe.ByteOffset(ref Unsafe.As<HashSetEntry<global::TestNamespace.Person>, byte>(ref e1),
                ref Unsafe.As<global::TestNamespace.Person, byte>(ref e1.Value)),
            SizeOfEntry = (uint)Unsafe.SizeOf<HashSetEntry<global::TestNamespace.Person>>(),
            Creator = &Create1,
            Adder = &Add1,
            ODBCreator = &ODBCreate1
        });

        static object Create1(uint capacity)
        {
            return new global::System.Collections.Generic.List<global::TestNamespace.Person>((int)capacity);
        }

        static void Add1(object c, ref byte value)
        {
            Unsafe.As<global::System.Collections.Generic.List<global::TestNamespace.Person>>(c).Add(Unsafe.As<byte, global::TestNamespace.Person>(ref value));
        }

        static object ODBCreate1(BTDB.ODBLayer.IInternalObjectDBTransaction tr, BTDB.ODBLayer.ODBDictionaryConfiguration config, ulong id)
        {
            return new BTDB.ODBLayer.ODBSet<global::TestNamespace.Person>(tr, config, id);
        }
    }
}
