using System;
using System.Collections.Generic;
using System.IO;
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

    public override string ToString()
    {
        if (SubItems == null)
            return Name + " : " + Type;
        var sb = new System.Text.StringBuilder();
        sb.Append(Name);
        sb.Append(" : ");
        sb.Append(Type);
        sb.Append(" {\n");
        foreach (var subItem in SubItems)
        {
            // Append with indent
            sb.Append("  ");
            var str = subItem.ToString();
            str = str.Replace("\n", "\n  ");
            sb.Append(str);
            sb.Append("\n");
        }

        sb.Append("}");
        return sb.ToString();
    }

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
        root.TryGetClass(out var structureBon, out var structureClassName);
        if (!structureClassName.SequenceEqual("Structure"u8)) throw new InvalidDataException("Invalid structure class");
        var structure = new EvStructure();
        if (!structureBon.TryGet("Type", out var typeBon)) throw new InvalidDataException("Missing Type");
        if (!typeBon.TryGetString(out var typeStr)) throw new InvalidDataException("Type is not string");
        structure.Type = typeStr switch
        {
            "String" => EvStructureType.String,
            "Integer" => EvStructureType.Integer,
            "Double" => EvStructureType.Double,
            "Boolean" => EvStructureType.Boolean,
            "DateTime" => EvStructureType.DateTime,
            "Binary" => EvStructureType.Binary,
            "SubTree" => EvStructureType.SubTree,
            "Optional" => EvStructureType.Optional,
            "Array" => EvStructureType.Array,
            _ => throw new Exception("Invalid structure type")
        };
        if (!structureBon.TryGet("Name", out var nameBon)) throw new InvalidDataException("Missing Name");
        if (!nameBon.TryGetString(out structure.Name)) throw new InvalidDataException("Name is not string");
        if (structureBon.TryGet("SubItems", out var subItemsBon))
        {
            if (!subItemsBon.TryGetArray(out var subItemsArray))
                throw new InvalidDataException("SubItems is not array");
            structure.SubItems = new();
            for (var i = 0u; i < subItemsArray.Items; i++)
            {
                if (!subItemsArray.TryGet(i, out var subItemBon)) throw new InvalidDataException("Missing SubItem");
                structure.SubItems.Add(Deserialize(ref subItemBon));
            }
        }

        return structure;
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
        if (!bon.TryGetClass(out var root, out var rootClassName) || !rootClassName.SequenceEqual("EvolveData"u8))
            throw new Exception("Invalid root class");
        if (!root.TryGet("Structure", out var structureBon)) throw new Exception("Missing Structure");
        var structure = EvStructure.Deserialize(ref structureBon);
        Console.WriteLine("Structure" + structure);
        root.TryGet("Data", out var dataBon);
        // context in root
        dataBon.TryGetObject(out var rootSubTree);
        rootSubTree.TryGet("Clients", out var clientsBon);
        clientsBon.TryGetArray(out var clientsArray);
        clientsArray.TryGet(clientsArray.Items - 1, out var clientBon);
        clientBon.TryGetObject(out var clientSubTree);
        clientSubTree.TryGet("Name", out var nameBon);
        nameBon.TryGetString(out var name);
        Console.WriteLine("Name = " + name);
    }

    static void Create()
    {
        var structure = new EvStructure
        {
            SubItems =
            [
                new()
                {
                    Name = "Clients", Type = EvStructureType.Array, SubItems =
                    [
                        new() { Name = "Name", Type = EvStructureType.String }
                    ]
                }
            ]
        };
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
        for (var i = 0; i < 100000000; i++)
        {
            bonBuilder.StartObject();
            bonBuilder.WriteKey("Name");
            bonBuilder.Write("Lorem ipsum " + i);
            bonBuilder.FinishObject();
        }

        bonBuilder.FinishArray();
        bonBuilder.FinishObject();
        bonBuilder.FinishClass();
        writer = bonBuilder.Finish();
        writer.Flush();
        Console.WriteLine("Length = " + writer.GetCurrentPosition());
    }
}
