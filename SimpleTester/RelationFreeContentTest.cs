using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace SimpleTester;

public interface ITestTable : IRelation<Test>
{
    void Insert(Test test);
    void Update(Test test);
    void ShallowUpdate(Test test);
    Test FindByIdOrDefault(ulong companyId, ulong id);
}

public class Test
{
    [PrimaryKey(1)]
    public ulong CompanyId { get; set; }
    [PrimaryKey(2)]
    public ulong Id { get; set; }
    public IDictionary<int, SimpleObject>? Simple { get; set; }
    public IDictionary<int, ComplexObject>? Complex { get; set; }
    public IDictionary<int, IIndirect<SimpleObject>>? IndirectSimple { get; set; }
    public IDictionary<int, IIndirect<ComplexObject>>? IndirectComplex { get; set; }
}

public class SimpleObject
{
    public string? Text { get; set; }
}

public class ComplexObject
{
    public SimpleObject? SimpleObject { get; set; }
}

public enum DictValueType
{
    Simple,
    Complex,
    IndirectSimple,
    IndirectComplex
}

[SimpleJob(RuntimeMoniker.Net50)]
[RPlotExporter, RankColumn]
public class RelationFreeContentTest
{
    [Params(100ul, 5000ul)]
    public int Count;

    [Params(DictValueType.Simple, DictValueType.Complex, DictValueType.IndirectSimple, DictValueType.IndirectComplex)]
    public DictValueType ValueType;

    ObjectDB? _db;
    Func<IObjectDBTransaction, ITestTable>? _creator;

    [GlobalSetup]
    public void Setup()
    {
        var tmp = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tmp);
        _db = CreateDb(new OnDiskFileCollection(tmp));

        using (var tr = _db.StartTransaction())
        {
            _creator = tr.InitRelation<ITestTable>("Test");
            tr.Commit();
        }

        using (var tr = _db.StartWritingTransaction().Result)
        {
            var table = _creator(tr);
            var test = new Test { CompanyId = 1, Id = 1 };
            switch (ValueType)
            {
                case DictValueType.Simple:
                    test.Simple = Enumerable.Range(1, Count).ToDictionary(item => item, item => new SimpleObject());
                    break;
                case DictValueType.Complex:
                    test.Complex = Enumerable.Range(1, Count).ToDictionary(item => item, item => new ComplexObject());
                    break;
                case DictValueType.IndirectSimple:
                    test.IndirectSimple = Enumerable.Range(1, Count).ToDictionary(item => item, item => (IIndirect<SimpleObject>)new DBIndirect<SimpleObject>(new SimpleObject()));
                    break;
                case DictValueType.IndirectComplex:
                    test.IndirectComplex = Enumerable.Range(1, Count).ToDictionary(item => item, item => (IIndirect<ComplexObject>)new DBIndirect<ComplexObject>(new ComplexObject()));
                    break;
            }
            table.Insert(test);
            tr.Commit();
        }
    }

    static ObjectDB CreateDb(IFileCollection fc)
    {
        var lowDb = new KeyValueDB(fc);
        var db = new ObjectDB();
        db.Open(lowDb, true);
        return db;
    }

    [Benchmark]
    public void UpdateInRelation()
    {
        using var tr = _db!.StartTransaction();
        var table = _creator!(tr);
        var test = table.FindByIdOrDefault(1, 1);
        table.Update(test);
        tr.Commit();
    }

    [Benchmark]
    public void ShallowUpdateInRelation()
    {
        using var tr = _db!.StartTransaction();
        var table = _creator!(tr);
        var test = table.FindByIdOrDefault(1, 1);
        table.ShallowUpdate(test);
        tr.Commit();
    }

}
