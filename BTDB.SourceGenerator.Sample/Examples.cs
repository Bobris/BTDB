using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BTDB;
using BTDB.IL;
using BTDB.IOC;
using BTDB.Serialization;
using Sample3rdPartyLib;

[assembly: GenerateFor(typeof(Class3rdPartyWithKeyedDependency))]

DynamicValueWrapperRegistration.Register4BTDB();

unsafe
{
    var builder = new ContainerBuilder();
//builder.RegisterType<Klass>();
    builder.AutoRegisterTypes().Where(t => !t.InheritsOrImplements(typeof(I3rdPartyInterface))).AsSelf();
    builder.RegisterType<Class3rdPartyWithKeyedDependency>().AsSelf();
    builder.RegisterType<K3rdPartyDependencyWrong>().As<I3rdPartyInterface>();
    builder.RegisterType<K3rdPartyDependency>().Named<I3rdPartyInterface>("Key1");
    var container = builder.Build();
    var name = container.Resolve<Class3rdPartyWithKeyedDependency>().Name;
    Console.WriteLine(name);
    container.Resolve<Klass>();
    {
        var h = IAnyHandler.CreateConsumeDispatcher(container);
        h(container, "Hello");
    }

    {
        DictEntry<int, string> e1 = new();
        ReflectionMetadata.RegisterCollection(new()
        {
            Type = typeof(Dictionary<int, string>),
            ElementKeyType = typeof(int),
            ElementValueType = typeof(string),
            OffsetNext = (uint)Unsafe.ByteOffset(ref Unsafe.As<DictEntry<int, string>, byte>(ref e1),
                ref Unsafe.As<int, byte>(ref e1.Next)),
            OffsetKey = (uint)Unsafe.ByteOffset(ref Unsafe.As<DictEntry<int, string>, byte>(ref e1),
                ref Unsafe.As<int, byte>(ref e1.Key)),
            OffsetValue = (uint)Unsafe.ByteOffset(ref Unsafe.As<DictEntry<int, string>, byte>(ref e1),
                ref Unsafe.As<string, byte>(ref e1.Value)),
            SizeOfEntry = (uint)Unsafe.SizeOf<DictEntry<int, string>>(),
            Creator = &Create1,
            AdderKeyValue = &Add1
        });
        ReflectionMetadata.RegisterStackAllocator(typeof((int, string)), &AllocateValueType);
    }

    {
        TupleIntString value = new();
        ReflectionMetadata.Register(new()
        {
            Type = typeof(Tuple<int, string>),
            Creator = &Create2,
            Name = "Tuple<int,string>",
            Namespace = "System",
            Fields =
            [
                new()
                {
                    Name = "Item1",
                    Type = typeof(int),
                    ByteOffset = RawData.CalcOffset(value, ref value.Item1),
                },
                new()
                {
                    Name = "Item2",
                    Type = typeof(string),
                    ByteOffset = RawData.CalcOffset(value, ref value.Item2),
                }
            ]
        });
    }

    {
        ValueTupleIntString value = new();
        ReflectionMetadata.Register(new()
        {
            Type = typeof(ValueTuple<int, string>),
            Name = "ValueTuple<int,string>",
            Namespace = "System",
            Fields =
            [
                new()
                {
                    Name = "Item1",
                    Type = typeof(int),
                    ByteOffset = (uint)Unsafe.ByteOffset(ref Unsafe.As<ValueTupleIntString, byte>(ref value),
                        ref Unsafe.As<int, byte>(ref value.Item1)),
                },
                new()
                {
                    Name = "Item2",
                    Type = typeof(string),
                    ByteOffset = (uint)Unsafe.ByteOffset(ref Unsafe.As<ValueTupleIntString, byte>(ref value),
                        ref Unsafe.As<string, byte>(ref value.Item2)),
                }
            ]
        });
    }


    MyCtx myCtx = default;
    AllocateValueType(ref Unsafe.As<MyCtx, byte>(ref myCtx), ref myCtx.Ptr, &WorkWithAllocatedValueType);

    static object Create2()
    {
        return new Tuple<int, string>(default, default!);
    }

    static object Create1(uint capacity)
    {
        return new Dictionary<int, string>((int)capacity);
    }

    static void Add1(object c, ref byte key, ref byte value)
    {
        Unsafe.As<Dictionary<int, string>>(c).Add(Unsafe.As<byte, int>(ref key), Unsafe.As<byte, string>(ref value));
    }

    static void AllocateValueType(ref byte ctx, ref nint ptr, delegate*<ref byte, void> chain)
    {
        (int, string) value;
#pragma warning disable CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        ptr = (nint)(&value);
#pragma warning restore CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        chain(ref ctx);
        ptr = 0;
    }

    static void WorkWithAllocatedValueType(ref byte ctx)
    {
#pragma warning disable CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        ref var value = ref *((int, string)*)Unsafe.As<byte, MyCtx>(ref ctx).Ptr;
#pragma warning restore CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        value = (42, "Hello");
        Console.WriteLine(value);
    }
}

public struct MyCtx
{
    public nint Ptr;
}

[Generate]
public class DynamicEventWrapper
{
    public dynamic DynamicEvent { get; set; }

    public DynamicEventWrapper(dynamic dynamicEvent)
    {
        DynamicEvent = dynamicEvent;
    }
}

[Generate]
public class Person
{
    string _name = "";
    int _age;

    public string Name
    {
        get => _name;
        set => _name = value;
    }

    public int Age
    {
        get => _age;
        set => _age = value + 1;
    }
}

[Generate]
public partial interface IAnyHandler
{
    public static unsafe partial delegate*<IContainer, object, object?> CreateConsumeDispatcher(IContainer container);
}

class Example : IAnyHandler
{
    internal void Consume(string message)
    {
        Console.WriteLine(message);
    }
}

[Generate]
public class Klass
{
    private Klass()
    {
        Console.WriteLine("Ok");
    }
}

public interface ILogger
{
}

[Generate]
public class ErrorHandler
{
    [BTDB.IOC.Dependency]
    public ILogger? Logger
    {
        get => null;
        init => Console.WriteLine(value!.ToString());
    }
}

[Generate]
public class K3rdPartyDependency : I3rdPartyInterface
{
    public string Name => "K3rdPartyDependency";
}

[Generate]
public class K3rdPartyDependencyWrong : I3rdPartyInterface
{
    public string Name => "K3rdPartyDependencyWrong";
}

public struct DictEntry<TKey, TValue>
{
    public uint HashCode;
    public int Next;
    public TKey Key;
    public TValue Value;
}

public class TupleIntString
{
    public int Item1;
    public string Item2;
}

public struct ValueTupleIntString
{
    public int Item1;
    public string Item2;
}

public interface IDynamicValue
{
}

public class DynamicValueWrapper<TValueType> : IDynamicValue
{
    public TValueType Value { get; set; }
}

static file class DynamicValueWrapperRegistration
{
    class Accessor<T1>
    {
        [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
        public static extern DynamicValueWrapper<T1> Creator();

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "<Value>k__BackingField")]
        public static extern ref T1 Field1(DynamicValueWrapper<T1> @this);
    }

    internal static unsafe void Register4BTDB()
    {
        var metadata = new ClassMetadata();
        metadata.Name = "DynamicValueWrapper<System.Enum>";
        metadata.Type = typeof(DynamicValueWrapper<Enum>);
        metadata.Namespace = "BTDBTest";
        metadata.Implements = [typeof(IDynamicValue)];
        metadata.Creator = &Accessor<Enum>.Creator;
        var dummy = Unsafe.As<DynamicValueWrapper<Enum>>(metadata);
        metadata.Fields =
        [
            new()
            {
                Name = "Value",
                Type = typeof(Enum),
                ByteOffset = RawData.CalcOffset(dummy, ref Accessor<Enum>.Field1(dummy)),
            },
        ];
        ReflectionMetadata.Register(metadata);
    }
}
