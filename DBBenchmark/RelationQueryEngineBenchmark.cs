using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using BTDB.Interpreter;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace DBBenchmark;

/// Evaluates one value predicate over serialized relation values that were copied out of the database up front, so
/// only predicate evaluation is measured. Times and allocations are reported per row.
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.Declared)]
[ShortRunJob]
public class RelationQueryEngineBenchmark
{
    [Params("Numeric", "String")] public string Case { get; set; } = "Numeric";

    InMemoryFileCollection _fileCollection = null!;
    ObjectDB _db = null!;
    byte[] _values = null!;
    int[] _offsets = null!;
    RelationQueryPlan _plan = null!;
    long[] _frame = null!;
    object?[] _objects = null!;
    ClosureProgram _closureProgram = null!;
    readonly ClosureFrame _closureFrame = new();
    byte[] _encodedAge = null!;
    byte[] _encodedCity = null!;
    int _expectedMatches;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _fileCollection = new();
        _db = QueryBenchData.Create(_fileCollection);
        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IQueryBenchTable>();
        var relationInfo = ((IRelationDbManipulator)table).RelationInfo;
        CopyValues(tr.KeyValueDBTransaction, relationInfo.Prefix);

        Expression<Func<QueryBenchRow, bool>> predicate = Case == "String"
            ? p => p.Age == QueryBenchData.SelectiveAge && p.City == QueryBenchData.SelectedCity
            : p => p.Age == QueryBenchData.SelectiveAge && p.Active == true;
        var expression = table.Query<QueryBenchRow>().Where(predicate).Expression;
        var ast = RelationQueryAstBuilder.Build(expression, new(relationInfo.Id, typeof(QueryBenchRow)));
        _plan = new RelationQueryCompiler(relationInfo, typeof(QueryBenchRow)).Compile(
            (IInternalObjectDBTransaction)tr, ast);
        if (_plan.KeyProgram != null || _plan.ValuePrograms == null)
            throw new InvalidOperationException("Benchmark predicate must use value fields only.");
        _frame = new long[_plan.FrameSize / sizeof(long)];
        _objects = new object?[_plan.ObjectCount];

        // Value fields are serialized in declaration order: Name, City, Age, Score, Active, Created.
        _closureProgram = Case == "String"
            ? new([new ClosureSkipStringStep(), new ClosureReadStringStep(0), new ClosureReadVInt64Step(0)],
                new ClosureAnd(new ClosureEqualNumber(0, QueryBenchData.SelectiveAge),
                    new ClosureEqualString(0, QueryBenchData.SelectedCity)))
            : new([
                    new ClosureSkipStringStep(), new ClosureSkipStringStep(), new ClosureReadVInt64Step(0),
                    new ClosureSkipVInt64Step(), new ClosureReadBoolStep(1)
                ],
                new ClosureAnd(new ClosureEqualNumber(0, QueryBenchData.SelectiveAge), new ClosureEqualNumber(1, 1)));

        var writer = new MemWriter();
        writer.WriteVInt64(QueryBenchData.SelectiveAge);
        _encodedAge = writer.GetSpan().ToArray();
        writer = new();
        writer.WriteString(QueryBenchData.SelectedCity);
        _encodedCity = writer.GetSpan().ToArray();

        _expectedMatches = Case == "String"
            ? table.Count(p => p.Age == QueryBenchData.SelectiveAge && p.City == QueryBenchData.SelectedCity)
            : table.Count(p => p.Age == QueryBenchData.SelectiveAge && p.Active);
        Verify(nameof(HandWritten), HandWritten());
        Verify(nameof(HandWrittenEncodedMatch), HandWrittenEncodedMatch());
        Verify(nameof(Interpreter), Interpreter());
        Verify(nameof(ClosureTree), ClosureTree());
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _db.Dispose();
        _fileCollection.Dispose();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = QueryBenchData.RowCount)]
    public int Interpreter()
    {
        var matches = 0;
        var programs = _plan.ValuePrograms!;
        ref var frame = ref Unsafe.As<long, byte>(ref MemoryMarshal.GetArrayDataReference(_frame));
        ref var objects = ref MemoryMarshal.GetArrayDataReference(_objects);
        for (var i = 0; i < _offsets.Length - 1; i++)
        {
            var keyReader = new MemReader();
            var valueReader = CreateValueReader(i);
            var program = programs[valueReader.ReadVUInt32()]!.Program!;
            var ctx = new InterpreterCtx(program, ref frame, ref objects, ref keyReader, ref valueReader);
            BTDB.Interpreter.Interpreter.Run(ref ctx);
            if (ctx.BoolResult) matches++;
        }

        return matches;
    }

    [Benchmark(OperationsPerInvoke = QueryBenchData.RowCount)]
    public int ClosureTree()
    {
        var matches = 0;
        for (var i = 0; i < _offsets.Length - 1; i++)
        {
            var valueReader = CreateValueReader(i);
            if (_closureProgram.Evaluate(ref valueReader, _closureFrame)) matches++;
        }

        return matches;
    }

    [Benchmark(OperationsPerInvoke = QueryBenchData.RowCount)]
    public int HandWritten()
    {
        var matches = 0;
        var isString = Case == "String";
        for (var i = 0; i < _offsets.Length - 1; i++)
        {
            var reader = CreateValueReader(i);
            reader.SkipVUInt32();
            reader.SkipString();
            if (isString)
            {
                var city = reader.ReadString();
                var age = (int)reader.ReadVInt64();
                if (age == QueryBenchData.SelectiveAge && city == QueryBenchData.SelectedCity) matches++;
            }
            else
            {
                reader.SkipString();
                var age = (int)reader.ReadVInt64();
                reader.SkipVInt64();
                var active = reader.ReadBool();
                if (age == QueryBenchData.SelectiveAge && active) matches++;
            }
        }

        return matches;
    }

    /// Upper bound for Phase 2 `MatchEncodedConstBr`: equality is tested on serialized bytes without decoding.
    [Benchmark(OperationsPerInvoke = QueryBenchData.RowCount)]
    public int HandWrittenEncodedMatch()
    {
        var matches = 0;
        var isString = Case == "String";
        for (var i = 0; i < _offsets.Length - 1; i++)
        {
            var reader = CreateValueReader(i);
            reader.SkipVUInt32();
            reader.SkipString();
            if (isString)
            {
                if (!MatchEncodedString(ref reader, _encodedCity)) continue;
                if (MatchEncodedVInt64(ref reader, _encodedAge)) matches++;
            }
            else
            {
                reader.SkipString();
                if (!MatchEncodedVInt64(ref reader, _encodedAge)) continue;
                reader.SkipVInt64();
                if (reader.ReadBool()) matches++;
            }
        }

        return matches;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static unsafe bool MatchEncodedString(ref MemReader reader, byte[] encoded)
    {
        var start = reader.Current;
        reader.SkipString();
        return new ReadOnlySpan<byte>((void*)start, (int)(reader.Current - start)).SequenceEqual(encoded);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static unsafe bool MatchEncodedVInt64(ref MemReader reader, byte[] encoded)
    {
        var start = reader.Current;
        reader.SkipVInt64();
        return new ReadOnlySpan<byte>((void*)start, (int)(reader.Current - start)).SequenceEqual(encoded);
    }

    unsafe MemReader CreateValueReader(int index)
    {
        // _values is allocated on the pinned object heap.
        var start = (nint)Unsafe.AsPointer(ref _values[_offsets[index]]);
        return new(start, _offsets[index + 1] - _offsets[index]);
    }

    void CopyValues(IKeyValueDBTransaction transaction, byte[] prefix)
    {
        var values = new List<byte[]>();
        using var cursor = transaction.CreateCursor();
        var buffer = new Memory<byte>();
        var found = cursor.FindFirstKey(prefix);
        while (found)
        {
            values.Add(cursor.GetValueMemory(ref buffer, true).ToArray());
            found = cursor.FindNextKey(prefix);
        }

        _offsets = new int[values.Count + 1];
        for (var i = 0; i < values.Count; i++) _offsets[i + 1] = _offsets[i] + values[i].Length;
        _values = GC.AllocateUninitializedArray<byte>(_offsets[^1], pinned: true);
        for (var i = 0; i < values.Count; i++) values[i].CopyTo(_values, _offsets[i]);
    }

    void Verify(string name, int matches)
    {
        if (matches != _expectedMatches)
            throw new InvalidOperationException($"{name} found {matches} matches, expected {_expectedMatches}.");
    }
}
