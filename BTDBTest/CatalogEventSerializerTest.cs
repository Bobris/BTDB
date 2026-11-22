using System;
using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.Buffer;
using BTDB.EventStore2Layer;
using BTDB.EventStoreLayer;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

public class CatalogEventSerializerTest
{
    [Generate]
    public class Item
    {
        public string? Name { get; set; }
    }

    [Generate]
    public class Order
    {
        public int Number { get; set; }
        public Item? Main { get; set; }
        public List<object>? Extras { get; set; }
    }

    [Generate]
    public class Note
    {
        public string? Text { get; set; }
    }

    public enum Priority { Low, High }

    [Generate]
    public class Holder
    {
        public object? Payload { get; set; }
    }

    [Generate]
    public class Animal
    {
        public string? Name { get; set; }
    }

    [Generate]
    public class Dog : Animal
    {
        public bool Barks { get; set; }
    }

    [Generate]
    public class Zoo
    {
        public Animal? Star { get; set; }
    }

    [Generate]
    public class Node
    {
        public string? Name { get; set; }
        public Node? Parent { get; set; }
        public List<Node>? Children { get; set; }
    }

    [Generate]
    public class Failing
    {
        public Item? A { get; set; }
        public object? B { get; set; }
    }

    [Generate]
    public class EventV1
    {
        public int Amount { get; set; }
    }

    [Generate]
    public class EventV2
    {
        public int Amount { get; set; }
        public string? Note { get; set; }
    }

    // Publishes like EventPublisher: a proposal is written (always applied here) and installed, then serialized again.
    static ByteBuffer Publish(CatalogEventSerializer serializer, object value, ref Layer2Metadata metadata)
    {
        var data = serializer.Serialize(out var proposed, value).ToAsyncSafe();
        if (proposed == null) return data;
        metadata = Layer2Metadata.Load(proposed.Bytes.ToArray());
        serializer.Update(metadata);
        data = serializer.Serialize(out proposed, value).ToAsyncSafe();
        Assert.Null(proposed);
        return data;
    }

    static object? Read(Layer2Metadata metadata, ByteBuffer data, ITypeNameMapper? mapper = null)
    {
        var deserializer = new CatalogEventDeserializer(metadata, mapper);
        Assert.True(deserializer.Deserialize(out var result, data));
        return result;
    }

    [Fact]
    public void AllNewTypesOfOneEventGoIntoOneProposal()
    {
        var empty = Layer2Metadata.Create("orders");
        var serializer = new CatalogEventSerializer(empty);
        var order = new Order
        {
            Number = 7, Main = new() { Name = "main" },
            Extras = [new Note { Text = "a" }, Priority.High, new Item { Name = "extra" }]
        };
        var data = serializer.Serialize(out var proposed, order);
        Assert.Equal(0, data.Length);
        Assert.NotNull(proposed);
        // Order, Item, Note, Priority and System.Object, the item type of List<object>; the list itself is inline.
        Assert.Equal(5, proposed.TypeCount);
        Assert.Same(empty, serializer.Metadata);
        var metadata = Layer2Metadata.Load(proposed.Bytes.ToArray());
        serializer.Update(metadata);
        var loaded = (Order)Read(metadata, serializer.Serialize(out proposed, order))!;
        Assert.Null(proposed);
        Assert.Equal(7, loaded.Number);
        Assert.Equal("main", loaded.Main!.Name);
        Assert.Equal("a", ((Note)loaded.Extras![0]).Text);
        Assert.Equal(Priority.High, loaded.Extras[1]);
        Assert.Equal("extra", ((Item)loaded.Extras[2]).Name);
    }

    [Fact]
    public void AfterALostRaceTheOtherWritersTypesAreUsed()
    {
        var empty = Layer2Metadata.Create("orders");
        var first = new CatalogEventSerializer(empty);
        var second = new CatalogEventSerializer(empty);
        first.Serialize(out var firstProposal, new Item { Name = "x" });
        second.Serialize(out var secondProposal, new Order { Main = new() { Name = "y" } });
        // The first proposal is written; the second loses the race, reads it and serializes again.
        var written = Layer2Metadata.Load(firstProposal!.Bytes.ToArray());
        Assert.NotNull(secondProposal);
        second.Update(written);
        second.Serialize(out secondProposal, new Order { Main = new() { Name = "y" } });
        // Only Order and System.Object, the item type of its List<object>, are new now.
        Assert.Equal(written.TypeCount + 2, secondProposal!.TypeCount);
        Assert.True(secondProposal.Extends(written));
        var both = Layer2Metadata.Load(secondProposal.Bytes.ToArray());
        first.Update(both);
        second.Update(both);
        var item = new Item { Name = "same" };
        Assert.Equal(first.Serialize(out _, item).ToByteArray(), second.Serialize(out _, item).ToByteArray());
    }

    [Fact]
    public void ObjectTypedFieldsStoreStructuralEnumAndBuiltInValuesById()
    {
        var metadata = Layer2Metadata.Create("orders");
        var serializer = new CatalogEventSerializer(metadata);
        var data = Publish(serializer, new Holder { Payload = new List<int> { 1, 2 } }, ref metadata);
        Assert.Equal(new List<int> { 1, 2 }, ((Holder)Read(metadata, data)!).Payload);
        // Holder, System.Object (the field's type) and the standalone List<int> entry.
        Assert.Equal(3, metadata.TypeCount);
        data = Publish(serializer, new Holder { Payload = Priority.Low }, ref metadata);
        Assert.Equal(Priority.Low, ((Holder)Read(metadata, data)!).Payload);
        data = Publish(serializer, new Holder { Payload = 42 }, ref metadata);
        Assert.Equal(42, ((Holder)Read(metadata, data)!).Payload);
        Assert.Equal(4, metadata.TypeCount); // the enum was added; int is built in
    }

    [Fact]
    public void SubclassInABaseTypedFieldKeepsItsType()
    {
        var metadata = Layer2Metadata.Create("zoo");
        var serializer = new CatalogEventSerializer(metadata);
        var data = Publish(serializer, new Zoo { Star = new Dog { Name = "Rex", Barks = true } }, ref metadata);
        var dog = Assert.IsType<Dog>(((Zoo)Read(metadata, data)!).Star);
        Assert.Equal("Rex", dog.Name);
        Assert.True(dog.Barks);
    }

    [Fact]
    public void ObjectCyclesKeepReferences()
    {
        var metadata = Layer2Metadata.Create("tree");
        var root = new Node { Name = "root", Children = [] };
        root.Children.Add(new() { Name = "child", Parent = root });
        var data = Publish(new CatalogEventSerializer(metadata), root, ref metadata);
        var loaded = (Node)Read(metadata, data)!;
        Assert.Same(loaded, loaded.Children!.Single().Parent);
        Assert.Equal(1, metadata.TypeCount);
    }

    [Fact]
    public void FailedSerializationLeavesNoProposal()
    {
        var metadata = Layer2Metadata.Create("orders");
        var serializer = new CatalogEventSerializer(metadata);
        Publish(serializer, new Note { Text = "known" }, ref metadata);
        // A (Item) is resolved as a new type before B fails.
        Assert.Throws<BTDBException>(() =>
            serializer.Serialize(out _, new Failing { A = new() { Name = "new" }, B = new Action(() => { }) }));
        var data = serializer.Serialize(out var proposed, new Note { Text = "again" });
        Assert.Null(proposed);
        Assert.Equal("again", ((Note)Read(metadata, data)!).Text);
    }

    [Fact]
    public void OldVersionLoadsIntoTheCurrentType()
    {
        var mapper = new RenamingTypeMapper().Map(typeof(EventV1), "Shop.Event");
        var metadata = Layer2Metadata.Create("shop");
        var data = Publish(new CatalogEventSerializer(metadata, mapper), new EventV1 { Amount = 5 }, ref metadata);
        var reader = new RenamingTypeMapper().Map(typeof(EventV2), "Shop.Event");
        var loaded = Assert.IsType<EventV2>(Read(metadata, data, reader));
        Assert.Equal(5, loaded.Amount);
        Assert.Null(loaded.Note);
    }

    [Fact]
    public void UpdateRequiresMetadataThatExtendsTheInstalledOne()
    {
        var metadata = Layer2Metadata.Create("orders");
        var serializer = new CatalogEventSerializer(metadata);
        Publish(serializer, new Item { Name = "x" }, ref metadata);
        var unrelated = Layer2Metadata.Create("orders");
        Assert.Throws<ArgumentException>(() => serializer.Update(unrelated));
        var deserializer = new CatalogEventDeserializer(metadata);
        Assert.Throws<ArgumentException>(() => deserializer.Update(unrelated));
        serializer.Update(metadata);
        deserializer.Update(metadata);
    }

    [Generate]
    public class TupleWithEnumHolder
    {
        public (Priority, int)? MaybePair { get; set; }
        public List<(Priority, string)>? Pairs { get; set; }
    }

    public class InvalidWithDelegate
    {
        public int Before { get; set; }
        public Action? Callback { get; set; }
        public int After { get; set; }
    }

    [Fact]
    public void NullableAndListOfTupleWithEnumRoundTrip()
    {
        var metadata = Layer2Metadata.Create("orders");
        var serializer = new CatalogEventSerializer(metadata);
        var value = new TupleWithEnumHolder
        {
            MaybePair = (Priority.High, 42),
            Pairs = [(Priority.Low, "a"), (Priority.High, "b")]
        };
        var data = Publish(serializer, value, ref metadata);
        var loaded = Assert.IsType<TupleWithEnumHolder>(Read(metadata, data));
        Assert.Equal((Priority.High, 42), loaded.MaybePair);
        Assert.Equal([(Priority.Low, "a"), (Priority.High, "b")], loaded.Pairs);
    }

    [Fact]
    public void RepeatedFailedSerializationOfInvalidTypeKeepsThrowing()
    {
        var metadata = Layer2Metadata.Create("orders");
        var serializer = new CatalogEventSerializer(metadata);
        Assert.Throws<BTDBException>(() => serializer.Serialize(out _, new InvalidWithDelegate { Before = 1, After = 2 }));
        // A failed FinishBuildFromType must not leave a half-built descriptor cached for subsequent calls.
        Assert.Throws<BTDBException>(() => serializer.Serialize(out _, new InvalidWithDelegate { Before = 1, After = 2 }));
    }

    [Fact]
    public void DeserializerBuildsOnlyTypesItReads()
    {
        var metadata = Layer2Metadata.Create("orders");
        var serializer = new CatalogEventSerializer(metadata);
        Publish(serializer, new Order { Main = new(), Extras = [new Note()] }, ref metadata);
        var data = Publish(serializer, new Item { Name = "only" }, ref metadata);
        var deserializer = new CatalogEventDeserializer(metadata);
        Assert.True(deserializer.Deserialize(out var item, data));
        Assert.Equal("only", ((Item)item!).Name);
        var decoder = (Layer2DescriptorDecoder)typeof(CatalogEventDeserializer)
            .GetField("_decoder", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(deserializer)!;
        var types = (ITypeDescriptor?[])typeof(Layer2DescriptorDecoder)
            .GetField("_types", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(decoder)!;
        Assert.Single(types, t => t != null);
    }
}
