using System;
using System.Linq;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Relation queries whose string predicates run on the interpreter's string opcodes must match LINQ-to-Objects.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class InterpreterStringQueryEndToEndTest : ObjectDbTestBase
{
    public InterpreterStringQueryEndToEndTest(ITestOutputHelper output) : base(output)
    {
    }

    [Generate]
    public class CharRow
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public string? Note { get; set; }
    }

    public interface ICharRowTable : IRelation<CharRow>
    {
        void Insert(CharRow row);
    }

    static readonly string[] Notes = ["abc", "ABC", "xyz", "", "Žluť", "žluť"];

    void Fill()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<ICharRowTable>();
        for (var i = 0; i < Notes.Length; i++) table.Insert(new() { Id = (ulong)i, Note = Notes[i] });
        tr.Commit();
    }

    [Fact]
    public void ContainsCharWithComparisonMatchesLinq()
    {
        Fill();
        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<ICharRowTable>();
        var rows = table.ToList();

        foreach (var comparison in new[]
                 {
                     StringComparison.Ordinal, StringComparison.OrdinalIgnoreCase,
                     StringComparison.InvariantCultureIgnoreCase
                 })
        {
            foreach (var c in new[] { 'a', 'A', 'ž', 'Ž' })
            {
                var expected = rows.Where(r => r.Note!.Contains(c, comparison)).Select(r => r.Id).ToList();
                var actual = table.Query<CharRow>().Where(r => r.Note!.Contains(c, comparison)).Select(r => r.Id)
                    .ToList();
                Assert.True(expected.SequenceEqual(actual), $"Contains('{c}', {comparison})");
            }
        }
    }

    [Fact]
    public void ConstantFoldedContainsCharWithComparisonMatchesLinq()
    {
        Fill();
        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<ICharRowTable>();

        // The receiver does not depend on the row, so the query layer evaluates the call while compiling.
        Assert.Equal(Notes.Length,
            table.Query<CharRow>().Count(r => "ABC".Contains('a', StringComparison.OrdinalIgnoreCase)));
        Assert.Equal(0, table.Query<CharRow>().Count(r => "ABC".Contains('a', StringComparison.Ordinal)));
    }
}
