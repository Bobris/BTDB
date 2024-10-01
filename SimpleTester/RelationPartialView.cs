using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Order;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace SimpleTester;

public class Person
{
    [PrimaryKey(1)] public int ParentId { get; set; }
    [PrimaryKey(2)] public int PersonId { get; set; }
    public string Name { get; set; } = null!;
    public ulong Age { get; set; }
    public IList<Person> Children { get; set; } = null!;
}

public class PersonOnlyId
{
    public int ParentId { get; set; }
    public int PersonId { get; set; }
}

public class PersonOnlyName : PersonOnlyId
{
    public string Name { get; set; } = null!;
}

public interface IPersonTable : IRelation<Person>
{
    bool RemoveById(int parentId, int personId);
    IEnumerable<Person> FindById(int parentId);
    IEnumerable<PersonOnlyId> FindByIdOnlyId(int parentId);
    IEnumerable<PersonOnlyName> FindByIdOnlyName(int parentId);
}

[ShortRunJob]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[MemoryDiagnoser]
public class BenchmarkRelationPartialView : IDisposable
{
    readonly InMemoryFileCollection _fc;
    readonly ObjectDB _db;
    readonly IObjectDBTransaction _tr;
    readonly IPersonTable _table;

    public BenchmarkRelationPartialView()
    {
        _fc = new InMemoryFileCollection();
        var lowDb = new BTreeKeyValueDB(_fc);
        _db = new ObjectDB();
        _db.Open(lowDb, true);
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();
        for (var i = 0; i < 10000; i++)
        {
            var p = new Person
            {
                ParentId = 1,
                PersonId = i,
                Age = (ulong)(i / 128),
                Name = "Lorem ipsum " + i,
                Children = Enumerable.Range(0, 100).Select(j => new Person
                    { ParentId = i, PersonId = i * 100 + j, Name = "Lorem ipsum child " + j, Age = (ulong)j }).ToList()
            };
            table.Upsert(p);
        }

        tr.Commit();
        _tr = _db.StartReadOnlyTransaction();
        _table = _tr.GetRelation<IPersonTable>();
    }

    [Benchmark]
    public int WholeUsers()
    {
        var r = 0;
        foreach (var person in _table.FindById(1))
        {
            r += person.PersonId;
        }

        return r;
    }

    [Benchmark]
    public int PrimaryKeysAndName()
    {
        var r = 0;
        foreach (var person in _table.FindByIdOnlyName(1))
        {
            r += person.PersonId;
        }

        return r;
    }

    [Benchmark]
    public int OnlyPrimaryKeys()
    {
        var r = 0;
        foreach (var person in _table.FindByIdOnlyId(1))
        {
            r += person.PersonId;
        }

        return r;
    }

#pragma warning disable CA1816
    public void Dispose()
#pragma warning restore CA1816
    {
        _tr.Dispose();
        _db.Dispose();
        _fc.Dispose();
    }
}
