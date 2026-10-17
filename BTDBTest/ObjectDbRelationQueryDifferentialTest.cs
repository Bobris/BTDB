using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Generates random rows and random predicates and checks that relation queries return the same rows, in the same
/// order, as LINQ-to-Objects over the materialized relation. Extend `RelationQueryPredicateGenerator` whenever the
/// query layer learns a new expression shape or field type.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbRelationQueryDifferentialTest : ObjectDbTestBase
{
    const int RowCount = 400;
    const int PredicatesPerSeed = 150;
    static readonly DateTime BaseDate = new(2025, 3, 1, 0, 0, 0, DateTimeKind.Utc);

    public ObjectDbRelationQueryDifferentialTest(ITestOutputHelper output) : base(output)
    {
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
        for (var i = 0; i < PredicatesPerSeed; i++)
        {
            var predicate = generator.Generate();
            var expected = rows.Where(predicate.Compile()).Select(Describe).ToList();
            if (expected.Count > 0 && expected.Count < rows.Count) selectivePredicates++;
            List<string> actual;
            try
            {
                actual = generator.ApplyAsWhereChain(table.Query<DiffRow>(), predicate).AsEnumerable()
                    .Select(Describe).ToList();
            }
            catch (Exception exception)
            {
                throw new InvalidOperationException($"Seed {seed}, predicate #{i} threw: {predicate}", exception);
            }

            if (expected.SequenceEqual(actual)) continue;
            Assert.Fail($"Seed {seed}, predicate #{i}: {predicate}\n" +
                        $"expected ({expected.Count}): {string.Join(", ", expected.Take(10))}\n" +
                        $"actual ({actual.Count}): {string.Join(", ", actual.Take(10))}");
        }

        // Guards the generator itself: predicates that match nothing or everything would hide query bugs.
        Assert.True(selectivePredicates >= PredicatesPerSeed / 4,
            $"Only {selectivePredicates} of {PredicatesPerSeed} predicates selected a proper subset of rows.");
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
                Code = $"c{i:D3}",
                Name = random.Next(7) == 0 ? null : $"n{random.Next(6)}",
                Age = 20 + random.Next(10),
                Score = random.Next(-5, 6),
                Rank = (uint)random.Next(8),
                Active = random.Next(2) == 0,
                Created = BaseDate.AddDays(random.Next(5)),
                Ratio = RelationQueryPredicateGenerator.Ratios[random.Next(RelationQueryPredicateGenerator.Ratios.Length - 1)]
            });
        }

        tr.Commit();
    }

    static string Describe(DiffRow row) =>
        $"{row.Tenant}/{row.Code}/{row.Name ?? "null"}/{row.Age}/{row.Score}/{row.Rank}/{row.Active}/" +
        $"{row.Created:yyyy-MM-dd}/{row.Ratio}";

    /// Captured variables, as the C# compiler produces them: a field access on a constant closure object.
    sealed class Captured
    {
        public ulong UInt64;
        public string? String;
        public int Int32;
        public long Int64;
        public uint UInt32;
        public bool Boolean;
        public DateTime DateTime;
        public double Double;

        public string Set(Type type, object? value)
        {
            if (type == typeof(ulong)) UInt64 = (ulong)value!;
            else if (type == typeof(string)) String = (string?)value;
            else if (type == typeof(int)) Int32 = (int)value!;
            else if (type == typeof(long)) Int64 = (long)value!;
            else if (type == typeof(uint)) UInt32 = (uint)value!;
            else if (type == typeof(bool)) Boolean = (bool)value!;
            else if (type == typeof(DateTime)) DateTime = (DateTime)value!;
            else if (type == typeof(double)) Double = (double)value!;
            else throw new NotSupportedException($"Captured {type.Name} is not supported.");
            return type.Name;
        }
    }

    sealed class RelationQueryPredicateGenerator
    {
        // The last ratio never appears in data, so it always produces a miss.
        public static readonly double[] Ratios = [-1.0, 0.5, 1.5, 2.25, 3.0];

        readonly Random _random;
        readonly ParameterExpression _parameter = Expression.Parameter(typeof(DiffRow), "p");
        readonly (string Name, Func<Random, object?> Value)[] _fields;

        public RelationQueryPredicateGenerator(Random random)
        {
            _random = random;
            // Constant domains are a bit wider than the generated data, so predicates also miss.
            _fields =
            [
                (nameof(DiffRow.Tenant), r => (ulong)r.Next(5)),
                (nameof(DiffRow.Code), r => $"c{r.Next(RowCount + 20):D3}"),
                (nameof(DiffRow.Name), r => r.Next(5) == 0 ? null : $"n{r.Next(7)}"),
                (nameof(DiffRow.Age), r => 19 + r.Next(12)),
                (nameof(DiffRow.Score), r => (long)r.Next(-6, 7)),
                (nameof(DiffRow.Rank), r => (uint)r.Next(9)),
                (nameof(DiffRow.Active), r => r.Next(2) == 0),
                (nameof(DiffRow.Created), r => BaseDate.AddDays(r.Next(6))),
                (nameof(DiffRow.Ratio), r => Ratios[r.Next(Ratios.Length)])
            ];
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
            if (depth <= 1 || _random.Next(3) == 0) return GenerateLeaf();
            var left = GenerateNode(depth - 1);
            var right = GenerateNode(depth - 1);
            return _random.Next(2) == 0 ? Expression.AndAlso(left, right) : Expression.OrElse(left, right);
        }

        Expression GenerateLeaf()
        {
            if (_random.Next(20) == 0) return Expression.Constant(_random.Next(2) == 0);
            var (name, valueFactory) = _fields[_random.Next(_fields.Length)];
            var property = Expression.Property(_parameter, name);
            var constant = CreateConstant(property.Type, valueFactory(_random));
            return _random.Next(4) == 0
                ? Expression.Equal(constant, property)
                : Expression.Equal(property, constant);
        }

        Expression CreateConstant(Type type, object? value)
        {
            if (_random.Next(2) == 0) return Expression.Constant(value, type);
            var captured = new Captured();
            return Expression.Field(Expression.Constant(captured), captured.Set(type, value));
        }
    }
}
