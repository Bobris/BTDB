using System;
using System.Collections.Generic;
using BTDB.Bon;
using BTDB.StreamLayer;

namespace SimpleTester;

enum EvStructureType
{
    String,
    Integer,
    Double,
    Boolean,
    DateTime,
    Binary,
    SubTree,
    Optional,
    Array,
}

class EvStructure
{
    public string Name = "";
    public EvStructureType Type = EvStructureType.SubTree;
    public List<EvStructure>? SubItems = null;

    public void Serialize(ref BonBuilder bon)
    {
        bon.StartClass("Structure");
        bon.WriteKey("Type");
        bon.Write(Type.ToString());
        bon.WriteKey("Name");
        bon.Write(Name);
        if (SubItems != null)
        {
            bon.WriteKey("SubItems");
            bon.StartArray();
            foreach (var subItem in SubItems)
            {
                subItem.Serialize(ref bon);
            }
            bon.FinishArray();
        }
        bon.FinishClass();
    }

    public static EvStructure Deserialize(ref Bon root)
    {
        throw new NotImplementedException();
    }
}

public static class BigBonTest
{
    public static void Run()
    {
        Create();
        Read();
    }

    static void Read()
    {
        using var fileReader = new MemoryMappedMemReader("data.eds");
        var reader = new MemReader(fileReader);
        if (!reader.CheckMagic("EVDS\0\0\0\x1"u8)) throw new Exception("Invalid magic");
        var bon = new Bon(reader);
        if (!bon.TryGetClass(out var root, out var rootClassName) || rootClassName != "EvolveData")
            throw new Exception("Invalid root class");
        if (!root.TryGet("Structure", out var structureBon)) throw new Exception("Missing Structure");
        var structure = EvStructure.Deserialize(ref structureBon);
        root.TryGet("Data", out var dataBon);
        // context in root
        dataBon.TryGetObject(out var rootSubTree);
        rootSubTree.TryGet("Clients", out var clientsBon);
        clientsBon.TryGetArray(out var clientsArray);
        clientsArray.TryGet(0, out var clientBon);
        clientBon.TryGetObject(out var clientSubTree);
        clientSubTree.TryGet("Name", out var nameBon);
        nameBon.TryGetString(out var name);
    }

    static void Create()
    {
        var structure = new EvStructure() { SubItems = [
            new EvStructure { Name = "Clients", Type = EvStructureType.Array, SubItems = [
                new EvStructure { Name = "Name", Type = EvStructureType.String }
            ]}
        ] };
        using var fileWriter = new FileMemWriter("data.eds");
        var writer = new MemWriter(fileWriter);
        writer.WriteBlock("EVDS\0\0\0\x1"u8);
        var bonBuilder = new BonBuilder(writer);
        bonBuilder.StartClass("EvolveData");
        bonBuilder.WriteKey("Structure");
        structure.Serialize(ref bonBuilder);
        bonBuilder.WriteKey("Data");
        // Writing data
        bonBuilder.StartObject();
        if (!bonBuilder.TryWriteKey("Clients")) throw new Exception("Duplicate key 'Clients'");
        bonBuilder.StartArray();
        bonBuilder.StartObject();
        bonBuilder.WriteKey("Name");
        bonBuilder.Write("John");
        bonBuilder.FinishObject();
        bonBuilder.FinishArray();
        bonBuilder.FinishObject();
        bonBuilder.FinishClass();
        writer = bonBuilder.Finish();
        Console.WriteLine("Length = " + writer.GetCurrentPosition());
    }
}
