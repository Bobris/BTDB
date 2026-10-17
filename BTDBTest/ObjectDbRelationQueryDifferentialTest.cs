using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Generates random rows and random predicates and checks that relation queries return the same rows, in the same
/// order, as LINQ-to-Objects over the materialized relation, including throwing the same exception at the same row.
/// Extend `RelationQueryPredicateGenerator` whenever the query layer learns a new expression shape or field type.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbRelationQueryDifferentialTest : ObjectDbTestBase
{
    const int RowCount = 400;
    const int PredicatesPerSeed = 200;
    static readonly DateTime BaseDate = new(2025, 3, 1, 0, 0, 0, DateTimeKind.Utc);
    static readonly Guid[] Tokens = [Guid.Empty, new("6f9619ff-8b86-d011-b42d-00cf4fc964ff"), new("00000000-0000-0000-0000-000000000001")];
    static readonly string?[] Names = ["n0", "n1", "n2", "Ádám", "😀x", null];
    static readonly string?[] Notes = ["", "abc", "Abc", "x😀", "žluť", "hello world", null];

    public ObjectDbRelationQueryDifferentialTest(ITestOutputHelper output) : base(output)
    {
    }

    public enum DiffKind
    {
        A,
        B,
        C,
        D
    }

    [Generate]
    public class DiffRow
    {
        [PrimaryKey(1)] public ulong Tenant { get; set; }
        [PrimaryKey(2)] public string Code { get; set; } = "";
        public string? Name { get; set; }
        public int Age { get; set; }
        public long Score { get; set; }
        public uint Rank { get; set; }
        public bool Active { get; set; }
        public DateTime Created { get; set; }
        public double Ratio { get; set; }
        public byte Level { get; set; }
        public short Delta { get; set; }
        public DiffKind Kind { get; set; }
        public int? Bonus { get; set; }
        public bool? Flag { get; set; }
        public Guid Token { get; set; }
        public decimal Price { get; set; }
        public TimeSpan Duration { get; set; }
        public DateTimeOffset Seen { get; set; }
        public string? Note { get; set; }
    }

    public interface IDiffRowTable : IRelation<DiffRow>
    {
        void Insert(DiffRow row);
    }

    public static IEnumerable<object[]> Seeds => Enumerable.Range(1, 10).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void QueryMatchesLinqToObjects(int seed)
    {
        var random = new Random(seed);
        FillRows(random);

        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IDiffRowTable>();
        var rows = table.ToList();
        var generator = new RelationQueryPredicateGenerator(random);
        var selectivePredicates = 0;
        var throwingPredicates = 0;
        for (var i = 0; i < PredicatesPerSeed; i++)
        {
            var predicate = generator.Generate();
            var expected = Run(() => rows.Where(predicate.Compile()));
            if (expected.Exception != null) throwingPredicates++;
            else if (expected.Rows.Count > 0 && expected.Rows.Count < rows.Count) selectivePredicates++;
            var actual = Run(() => generator.ApplyAsWhereChain(table.Query<DiffRow>(), predicate).AsEnumerable());
            if (actual.Exception is NotSupportedException notSupported)
                throw new InvalidOperationException($"Seed {seed}, predicate #{i} is not supported: {predicate}",
                    notSupported);
            if (expected.Rows.SequenceEqual(actual.Rows) && expected.Exception?.GetType() == actual.Exception?.GetType())
                continue;
            Assert.Fail($"Seed {seed}, predicate #{i}: {predicate}\n" +
                        $"expected ({expected.Rows.Count}, {expected.Exception?.GetType().Name}): {string.Join(", ", expected.Rows.Take(5))}\n" +
                        $"actual ({actual.Rows.Count}, {actual.Exception?.GetType().Name}): {string.Join(", ", actual.Rows.Take(5))}\n" +
                        $"{actual.Exception}");
        }

        // Guards the generator itself: predicates that match nothing or everything would hide query bugs.
        Assert.True(selectivePredicates >= PredicatesPerSeed / 4,
            $"Only {selectivePredicates} of {PredicatesPerSeed} predicates selected a proper subset of rows.");
        Assert.True(throwingPredicates < PredicatesPerSeed / 2,
            $"{throwingPredicates} of {PredicatesPerSeed} predicates threw.");
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void TerminalOperatorsMatchLinqToObjects(int seed)
    {
        var random = new Random(seed);
        FillRows(random);

        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IDiffRowTable>();
        var rows = table.ToList();
        var generator = new RelationQueryPredicateGenerator(random);
        var terminals = Terminals();
        for (var i = 0; i < PredicatesPerSeed; i++)
        {
            var source = random.Next(4) == 0 ? null : generator.Generate();
            var argument = generator.Generate();
            var (name, terminal) = terminals[random.Next(terminals.Count)];
            var expected = RunTerminal(() => terminal(source == null ? rows.AsQueryable() : rows.AsQueryable().Where(source),
                argument));
            var actual = RunTerminal(() => terminal(source == null
                ? table.Query<DiffRow>()
                : generator.ApplyAsWhereChain(table.Query<DiffRow>(), source), argument));
            if (actual.Exception is NotSupportedException notSupported)
                throw new InvalidOperationException($"Seed {seed}, #{i} {name} is not supported: {source} / {argument}",
                    notSupported);
            if (expected.Value == actual.Value && expected.Exception?.GetType() == actual.Exception?.GetType() &&
                expected.Exception?.Message == actual.Exception?.Message)
                continue;
            Assert.Fail($"Seed {seed}, #{i} {name}: {source} / {argument}\n" +
                        $"expected {expected.Value} {expected.Exception?.GetType().Name}: {expected.Exception?.Message}\n" +
                        $"actual {actual.Value} {actual.Exception?.GetType().Name}: {actual.Exception?.Message}\n" +
                        $"{actual.Exception}");
        }
    }

    static readonly DiffRow DefaultRow = new() { Code = "default" };

    // Each terminal gets the source and a predicate it may use as its argument.
    static List<(string, Func<IQueryable<DiffRow>, Expression<Func<DiffRow, bool>>, object?>)> Terminals() =>
    [
        ("Count", (q, _) => q.Count()),
        ("Count(p)", (q, p) => q.Count(p)),
        ("LongCount(p)", (q, p) => q.LongCount(p)),
        ("Any", (q, _) => q.Any()),
        ("Any(p)", (q, p) => q.Any(p)),
        ("All(p)", (q, p) => q.All(p)),
        ("First", (q, _) => q.First()),
        ("First(p)", (q, p) => q.First(p)),
        ("FirstOrDefault", (q, _) => q.FirstOrDefault()),
        ("FirstOrDefault(p, d)", (q, p) => q.FirstOrDefault(p, DefaultRow)),
        ("Single", (q, _) => q.Single()),
        ("Single(p)", (q, p) => q.Single(p)),
        ("SingleOrDefault(p)", (q, p) => q.SingleOrDefault(p)),
        ("Last", (q, _) => q.Last()),
        ("Last(p)", (q, p) => q.Last(p)),
        ("LastOrDefault(d)", (q, _) => q.LastOrDefault(DefaultRow)),
        ("Sum(Age)", (q, _) => q.Sum(r => r.Age)),
        ("Sum(Score)", (q, _) => q.Sum(r => r.Score)),
        ("Sum(Ratio)", (q, _) => q.Sum(r => r.Ratio)),
        ("Sum(Price)", (q, _) => q.Sum(r => r.Price)),
        ("Sum(Bonus)", (q, _) => q.Sum(r => r.Bonus)),
        ("Sum(float)", (q, _) => q.Sum(r => (float)r.Ratio)),
        ("Sum(Level)", (q, _) => q.Sum(r => r.Level)),
        ("Average(Age)", (q, _) => q.Average(r => r.Age)),
        ("Average(Score)", (q, _) => q.Average(r => r.Score)),
        ("Average(Ratio)", (q, _) => q.Average(r => r.Ratio)),
        ("Average(Price)", (q, _) => q.Average(r => r.Price)),
        ("Average(Bonus)", (q, _) => q.Average(r => r.Bonus)),
        ("Average(float)", (q, _) => q.Average(r => (float?)r.Ratio)),
        ("Min(Age)", (q, _) => q.Min(r => r.Age)),
        ("Max(Score * 2)", (q, _) => q.Max(r => r.Score * 2)),
        ("Min(Created)", (q, _) => q.Min(r => r.Created)),
        ("Max(Kind)", (q, _) => q.Max(r => r.Kind)),
        ("Min(Bonus)", (q, _) => q.Min(r => r.Bonus)),
        ("Max(Bonus)", (q, _) => q.Max(r => r.Bonus)),
        ("Max(Token)", (q, _) => q.Max(r => r.Token)),
        ("Min(Seen)", (q, _) => q.Min(r => r.Seen)),
        ("Max(Duration)", (q, _) => q.Max(r => r.Duration)),
        ("Min(Price)", (q, _) => q.Min(r => r.Price)),
        ("Max(Ratio)", (q, _) => q.Max(r => r.Ratio)),
        ("Min(Active)", (q, _) => q.Min(r => r.Active)),
        ("Max(Tenant)", (q, _) => q.Max(r => r.Tenant)),
    ];

    static (string? Value, Exception? Exception) RunTerminal(Func<object?> terminal)
    {
        try
        {
            var value = terminal();
            return (value is DiffRow row ? Describe(row) : value is null ? "null" : $"{value.GetType().Name}:{value}",
                null);
        }
        catch (Exception exception)
        {
            return (null, exception);
        }
    }

    static (List<string> Rows, Exception? Exception) Run(Func<IEnumerable<DiffRow>> query)
    {
        var result = new List<string>();
        try
        {
            foreach (var row in query()) result.Add(Describe(row));
            return (result, null);
        }
        catch (Exception exception)
        {
            return (result, exception);
        }
    }

    void FillRows(Random random)
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IDiffRowTable>();
        for (var i = 0; i < RowCount; i++)
        {
            table.Insert(new()
            {
                Tenant = (ulong)random.Next(4),
                // Some keys are non-ASCII, so ordered string decoding sees multi-byte and surrogate chars.
                Code = i % 9 == 0 ? $"ž{i:D3}😀" : $"c{i:D3}",
                Name = Names[random.Next(Names.Length)],
                Age = 20 + random.Next(10),
                Score = random.Next(-5, 6),
                Rank = (uint)random.Next(8),
                Active = random.Next(2) == 0,
                Created = BaseDate.AddDays(random.Next(5)),
                Ratio = RelationQueryPredicateGenerator.Ratios[random.Next(RelationQueryPredicateGenerator.Ratios.Length - 1)],
                Level = (byte)random.Next(250, 256),
                Delta = (short)random.Next(-3, 4),
                Kind = (DiffKind)random.Next(4),
                Bonus = random.Next(3) == 0 ? null : random.Next(-2, 3),
                Flag = random.Next(3) == 0 ? null : random.Next(2) == 0,
                Token = Tokens[random.Next(Tokens.Length)],
                Price = random.Next(-4, 5) * 1.25m,
                Duration = TimeSpan.FromMinutes(random.Next(-2, 3)),
                Seen = new(BaseDate.AddHours(random.Next(3)).Ticks, TimeSpan.FromHours(random.Next(-1, 2))),
                Note = Notes[random.Next(Notes.Length)]
            });
        }

        tr.Commit();
    }

    static string Describe(DiffRow row) =>
        $"{row.Tenant}/{row.Code}/{row.Name ?? "null"}/{row.Age}/{row.Score}/{row.Rank}/{row.Active}/" +
        $"{row.Created:yyyy-MM-dd}/{row.Ratio}/{row.Level}/{row.Delta}/{row.Kind}/{row.Bonus}/{row.Flag}/" +
        $"{row.Token}/{row.Price}/{row.Duration}/{row.Seen:O}/{row.Note ?? "null"}";

    /// Holds captured variables, as the C# compiler does: a field access on a constant closure object.
    sealed class Captured<T>
    {
        public T Value = default!;
    }

    sealed class RelationQueryPredicateGenerator
    {
        // The last ratio never appears in data, so it always produces a miss.
        public static readonly double[] Ratios = [-1.0, 0.5, 1.5, 2.25, 3.0];

        static readonly QueryOperator[] AllComparisons =
            [QueryOperator.Equal, QueryOperator.NotEqual, QueryOperator.Less, QueryOperator.LessOrEqual,
                QueryOperator.Greater, QueryOperator.GreaterOrEqual];

        static readonly QueryOperator[] Equalities = [QueryOperator.Equal, QueryOperator.NotEqual];

        enum QueryOperator
        {
            Equal,
            NotEqual,
            Less,
            LessOrEqual,
            Greater,
            GreaterOrEqual
        }

        readonly Random _random;
        readonly ParameterExpression _parameter = Expression.Parameter(typeof(DiffRow), "p");
        readonly List<Func<Expression>> _leaves = [];

        public RelationQueryPredicateGenerator(Random random)
        {
            _random = random;
            // Constant domains are a bit wider than the generated data, so predicates also miss.
            AddCompare(nameof(DiffRow.Tenant), () => (ulong)Pick(5), AllComparisons);
            AddCompare(nameof(DiffRow.Code), () => $"c{Pick(RowCount + 20):D3}", Equalities);
            AddCompare(nameof(DiffRow.Name), () => Names[Pick(Names.Length)], Equalities);
            AddCompare(nameof(DiffRow.Note), () => Notes[Pick(Notes.Length)], Equalities);
            AddCompare(nameof(DiffRow.Age), () => 19 + Pick(12), AllComparisons);
            AddCompare(nameof(DiffRow.Score), () => (long)Pick(13) - 6, AllComparisons);
            AddCompare(nameof(DiffRow.Rank), () => (uint)Pick(9), AllComparisons);
            AddCompare(nameof(DiffRow.Active), () => Pick(2) == 0, Equalities);
            AddCompare(nameof(DiffRow.Created), () => BaseDate.AddDays(Pick(6)), AllComparisons);
            AddCompare(nameof(DiffRow.Ratio), () => Ratios[Pick(Ratios.Length)], AllComparisons);
            AddCompare(nameof(DiffRow.Token), () => Tokens[Pick(Tokens.Length)], Equalities);
            AddCompare(nameof(DiffRow.Price), () => (Pick(11) - 5) * 1.25m, AllComparisons);
            AddCompare(nameof(DiffRow.Duration), () => TimeSpan.FromMinutes(Pick(5) - 2), AllComparisons);
            AddCompare(nameof(DiffRow.Seen), () => new DateTimeOffset(BaseDate.AddHours(Pick(4)).Ticks,
                TimeSpan.FromHours(Pick(3) - 1)), AllComparisons);
            AddCompare(nameof(DiffRow.Bonus), () => Pick(4) == 0 ? null : (int?)(Pick(5) - 2), AllComparisons);
            AddCompare(nameof(DiffRow.Flag), () => Pick(3) == 0 ? null : (bool?)(Pick(2) == 0), Equalities);
            // C# compares byte, short and enum fields after converting them to int.
            _leaves.Add(() => Compare(Expression.Convert(Property(nameof(DiffRow.Level)), typeof(int)),
                Constant(250 + Pick(7)), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Convert(Property(nameof(DiffRow.Delta)), typeof(int)),
                Constant(Pick(9) - 4), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Convert(Property(nameof(DiffRow.Kind)), typeof(int)),
                Expression.Convert(Constant((DiffKind)Pick(5)), typeof(int)), PickOf(AllComparisons)));
            // Comparing two fields.
            _leaves.Add(() => Compare(Property(nameof(DiffRow.Score)),
                Expression.Convert(Property(nameof(DiffRow.Delta)), typeof(long)), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Property(nameof(DiffRow.Bonus)),
                Expression.Convert(Property(nameof(DiffRow.Delta)), typeof(int?)), PickOf(AllComparisons)));
            // Both operands nullable: null equals only null.
            _leaves.Add(() => Compare(Property(nameof(DiffRow.Bonus)),
                Expression.Add(Property(nameof(DiffRow.Bonus)), Constant((int?)(Pick(3) - 1), typeof(int?))),
                PickOf(AllComparisons)));
            // Arithmetic, including division by zero and checked overflow.
            _leaves.Add(() => Compare(Expression.Add(Property(nameof(DiffRow.Age)), Constant(Pick(5))),
                Constant(20 + Pick(15)), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Multiply(Property(nameof(DiffRow.Price)), Constant(2m)),
                Constant((Pick(11) - 5) * 2.5m), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Divide(Constant(12u), Property(nameof(DiffRow.Rank))),
                Constant((uint)Pick(13)), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Subtract(Property(nameof(DiffRow.Ratio)), Constant(0.5)),
                Constant(Ratios[Pick(Ratios.Length)]), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Add(Property(nameof(DiffRow.Bonus)), Constant((int?)1, typeof(int?))),
                Constant((int?)(Pick(5) - 2), typeof(int?)), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.AddChecked(Property(nameof(DiffRow.Score)),
                Constant(Pick(10) == 0 ? long.MaxValue : 1L)), Constant(0L), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Negate(Property(nameof(DiffRow.Score))), Constant((long)Pick(5) - 2),
                PickOf(AllComparisons)));
            // Bool members and nullable members.
            _leaves.Add(() => Property(nameof(DiffRow.Active)));
            _leaves.Add(() => Expression.Property(Property(nameof(DiffRow.Flag)), "HasValue"));
            _leaves.Add(() => Expression.Property(Property(nameof(DiffRow.Bonus)), "HasValue"));
            _leaves.Add(() => Expression.Property(Property(nameof(DiffRow.Flag)), "Value"));
            _leaves.Add(() => Compare(Expression.Property(Property(nameof(DiffRow.Bonus)), "Value"),
                Constant(Pick(5) - 2), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Convert(Property(nameof(DiffRow.Bonus)), typeof(int)),
                Constant(Pick(5) - 2), PickOf(AllComparisons)));
            // Strings.
            _leaves.Add(() => StringCall(nameof(string.StartsWith), StringProperty(), PickPrefix(),
                StringComparison.Ordinal));
            _leaves.Add(() => StringCall(nameof(string.StartsWith), StringProperty(), PickPrefix(), null));
            _leaves.Add(() => StringCall(nameof(string.EndsWith), StringProperty(), PickSuffix(),
                StringComparison.OrdinalIgnoreCase));
            _leaves.Add(() => StringCall(nameof(string.Contains), StringProperty(), PickSuffix(), null));
            _leaves.Add(() => Expression.Call(StringProperty(), typeof(string).GetMethod(nameof(string.Contains), [typeof(char)])!,
                Constant("xaé😀"[Pick(3)])));
            _leaves.Add(() => StringCall(nameof(string.Equals), StringProperty(), Notes[Pick(Notes.Length - 1)],
                StringComparison.OrdinalIgnoreCase));
            _leaves.Add(() => Expression.Call(typeof(string).GetMethod(nameof(string.Equals),
                    [typeof(string), typeof(string), typeof(StringComparison)])!, StringProperty(),
                Constant(Notes[Pick(Notes.Length)]), Constant(StringComparison.OrdinalIgnoreCase)));
            _leaves.Add(() => Expression.Call(typeof(string).GetMethod(nameof(string.IsNullOrEmpty))!,
                StringProperty()));
            _leaves.Add(() => Compare(Expression.Property(StringProperty(), nameof(string.Length)), Constant(Pick(6)),
                PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Call(typeof(string).GetMethod(nameof(string.CompareOrdinal),
                        [typeof(string), typeof(string)])!, Property(nameof(DiffRow.Code)),
                    Constant($"c{Pick(RowCount):D3}")), Constant(0), PickOf(AllComparisons)));
            // Contains on constant collections.
            _leaves.Add(() => Expression.Call(EnumerableContains<int>(), Constant(new[] { 19 + Pick(12), 19 + Pick(12) }),
                Property(nameof(DiffRow.Age))));
            _leaves.Add(() => Expression.Call(EnumerableContains<DiffKind>(),
                Constant(new[] { (DiffKind)Pick(4), (DiffKind)Pick(4) }), Property(nameof(DiffRow.Kind))));
            _leaves.Add(() => Expression.Call(EnumerableContains<string?>(),
                Constant(new List<string?> { Names[Pick(Names.Length)], Names[Pick(Names.Length)] }),
                Property(nameof(DiffRow.Name))));
            _leaves.Add(() => Expression.Call(Constant(new HashSet<long> { Pick(13) - 6, Pick(13) - 6 }),
                typeof(HashSet<long>).GetMethod(nameof(HashSet<long>.Contains))!, Property(nameof(DiffRow.Score))));
            _leaves.Add(() => Expression.Call(EnumerableContains<Guid>(), Constant(new[] { Tokens[Pick(Tokens.Length)] }),
                Property(nameof(DiffRow.Token))));
        }

        int Pick(int count) => _random.Next(count);
        T PickOf<T>(T[] values) => values[_random.Next(values.Length)];

        string PickPrefix() => PickOf(["", "c0", "c01", "C0", "n", "Á", "😀", "a", "x😀", "hello", "ž0", "ž"]);
        string PickSuffix() => PickOf(["", "1", "x", "c", "😀", "ť", "BC", "world", "o w"]);

        Expression StringProperty() =>
            Property(PickOf([nameof(DiffRow.Code), nameof(DiffRow.Name), nameof(DiffRow.Note)]));

        MemberExpression Property(string name) => Expression.Property(_parameter, name);

        static MethodInfo EnumerableContains<T>() =>
            new Func<IEnumerable<T>, T, bool>(Enumerable.Contains).Method;

        void AddCompare<T>(string name, Func<T> value, QueryOperator[] operators)
        {
            _leaves.Add(() => Compare(Property(name), Constant(value()), PickOf(operators)));
        }

        Expression Constant<T>(T value) => Constant(value, typeof(T));

        Expression Constant(object? value, Type type)
        {
            if (Pick(2) == 0) return Expression.Constant(value, type);
            var capturedType = typeof(Captured<>).MakeGenericType(type);
            var captured = Activator.CreateInstance(capturedType)!;
            var field = capturedType.GetField(nameof(Captured<int>.Value))!;
            field.SetValue(captured, value);
            return Expression.Field(Expression.Constant(captured), field);
        }

        Expression StringCall(string method, Expression target, string argument, StringComparison? comparison)
        {
            return comparison == null
                ? Expression.Call(target, typeof(string).GetMethod(method, [typeof(string)])!, Constant(argument))
                : Expression.Call(target, typeof(string).GetMethod(method, [typeof(string), typeof(StringComparison)])!,
                    Constant(argument), Expression.Constant(comparison.Value));
        }

        Expression Compare(Expression left, Expression right, QueryOperator op)
        {
            if (Pick(4) == 0)
            {
                (left, right) = (right, left);
                op = op switch
                {
                    QueryOperator.Less => QueryOperator.Greater,
                    QueryOperator.LessOrEqual => QueryOperator.GreaterOrEqual,
                    QueryOperator.Greater => QueryOperator.Less,
                    QueryOperator.GreaterOrEqual => QueryOperator.LessOrEqual,
                    _ => op
                };
            }

            return op switch
            {
                QueryOperator.Equal => Expression.Equal(left, right),
                QueryOperator.NotEqual => Expression.NotEqual(left, right),
                QueryOperator.Less => Expression.LessThan(left, right),
                QueryOperator.LessOrEqual => Expression.LessThanOrEqual(left, right),
                QueryOperator.Greater => Expression.GreaterThan(left, right),
                _ => Expression.GreaterThanOrEqual(left, right)
            };
        }

        public Expression<Func<DiffRow, bool>> Generate()
        {
            return Expression.Lambda<Func<DiffRow, bool>>(GenerateNode(_random.Next(1, 5)), _parameter);
        }

        public IQueryable<DiffRow> ApplyAsWhereChain(IQueryable<DiffRow> source,
            Expression<Func<DiffRow, bool>> predicate)
        {
            if (_random.Next(2) == 0) return source.Where(predicate);
            var conjuncts = new List<Expression>();
            Flatten(predicate.Body, conjuncts);
            foreach (var conjunct in conjuncts)
                source = source.Where(Expression.Lambda<Func<DiffRow, bool>>(conjunct, _parameter));
            return source;
        }

        static void Flatten(Expression expression, List<Expression> conjuncts)
        {
            if (expression.NodeType == ExpressionType.AndAlso)
            {
                var binary = (BinaryExpression)expression;
                Flatten(binary.Left, conjuncts);
                Flatten(binary.Right, conjuncts);
                return;
            }

            conjuncts.Add(expression);
        }

        Expression GenerateNode(int depth)
        {
            if (depth <= 1 || _random.Next(3) == 0)
            {
                if (_random.Next(25) == 0) return Expression.Constant(_random.Next(2) == 0);
                var leaf = _leaves[_random.Next(_leaves.Count)]();
                return _random.Next(6) == 0 ? Expression.Not(leaf) : leaf;
            }

            var left = GenerateNode(depth - 1);
            var right = GenerateNode(depth - 1);
            var node = _random.Next(2) == 0 ? Expression.AndAlso(left, right) : Expression.OrElse(left, right);
            return _random.Next(8) == 0 ? Expression.Not(node) : node;
        }
    }
}
