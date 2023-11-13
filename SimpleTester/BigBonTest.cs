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
    public List<EvStructure>? Fields = null;

    public void Serialize(ref BonBuilder bon)
    {
        bon.StartClass("Structure");
        bon.WriteKey("Name");
        // TODO
        bon.FinishClass();
    }
}

public static class BigBonTest
{
    public static void Run()
    {
        Create();
    }

    static void Create()
    {
        using var fileWriter = new BTDB.StreamLayer.FileMemWriter("data.eds");
        var writer = new MemWriter(fileWriter);
        writer.WriteBlock("EVDS\0\0\0\x1"u8);
        var bonBuilder = new BonBuilder(writer);
        bonBuilder.StartClass("EvolveData");
        bonBuilder.WriteKey("Structure");

        bonBuilder.FinishClass();
        bonBuilder.Finish();
        Console.WriteLine("Length = " + writer.GetCurrentPosition());
    }
}
