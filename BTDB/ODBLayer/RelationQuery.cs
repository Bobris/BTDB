using System;
using System.Buffers;
using System.Collections;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.Interpreter;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

abstract class QueryNode
{
    protected QueryNode(Type type)
    {
        Type = type;
    }

    public Type Type { get; }
}

class QueryByIdQueryNode : QueryNode
{
    public QueryByIdQueryNode(uint relationId, Type type) : base(type)
    {
        RelationId = relationId;
    }

    public uint RelationId { get; }
}

class WhereQueryNode : QueryNode
{
    public WhereQueryNode(QueryNode source, QueryNode predicate) : base(source.Type)
    {
        Source = source;
        Predicate = predicate;
    }

    public QueryNode Source { get; }
    public QueryNode Predicate { get; }
}

class BoolAndQueryNode : QueryNode
{
    public BoolAndQueryNode(QueryNode left, QueryNode right) : base(typeof(bool))
    {
        Left = left;
        Right = right;
    }

    public QueryNode Left { get; }
    public QueryNode Right { get; }
}

class BoolOrQueryNode : QueryNode
{
    public BoolOrQueryNode(QueryNode left, QueryNode right) : base(typeof(bool))
    {
        Left = left;
        Right = right;
    }

    public QueryNode Left { get; }
    public QueryNode Right { get; }
}

sealed class NotQueryNode : QueryNode
{
    public NotQueryNode(QueryNode operand) : base(typeof(bool))
    {
        Operand = operand;
    }

    public QueryNode Operand { get; }
}

/// Comparison with C# semantics, lifted when an operand is nullable.
sealed class CompareQueryNode : QueryNode
{
    public CompareQueryNode(QueryNode left, QueryNode right, QueryCompare compare) : base(typeof(bool))
    {
        Left = left;
        Right = right;
        Compare = compare;
    }

    public QueryNode Left { get; }
    public QueryNode Right { get; }
    public QueryCompare Compare { get; }
}

class ConstantQueryNode : QueryNode
{
    readonly object? _value;

    public ConstantQueryNode(object? value, Type type) : base(type)
    {
        _value = value;
    }

    public virtual object? Value => _value;
}

class PropQueryNode : QueryNode
{
    public PropQueryNode(QueryByIdQueryNode query, string name, Type type) : base(type)
    {
        Query = query;
        Name = name;
    }

    public QueryByIdQueryNode Query { get; }
    public string Name { get; }
}

/// Numeric conversion, enum to underlying type or lifting to and from Nullable.
sealed class ConvertQueryNode : QueryNode
{
    public ConvertQueryNode(QueryNode operand, Type type, bool isChecked) : base(type)
    {
        Operand = operand;
        IsChecked = isChecked;
    }

    public QueryNode Operand { get; }
    public bool IsChecked { get; }
}

sealed class ArithmeticQueryNode : QueryNode
{
    public ArithmeticQueryNode(QueryNode left, QueryNode? right, QueryArithmetic operation, Type type) : base(type)
    {
        Left = left;
        Right = right;
        Operation = operation;
    }

    public QueryNode Left { get; }

    /// Null for negations.
    public QueryNode? Right { get; }

    public QueryArithmetic Operation { get; }
}

sealed class HasValueQueryNode : QueryNode
{
    public HasValueQueryNode(QueryNode operand) : base(typeof(bool))
    {
        Operand = operand;
    }

    public QueryNode Operand { get; }
}

/// Nullable.Value, which throws InvalidOperationException for null.
sealed class NullableValueQueryNode : QueryNode
{
    public NullableValueQueryNode(QueryNode operand) : base(Nullable.GetUnderlyingType(operand.Type)!)
    {
        Operand = operand;
    }

    public QueryNode Operand { get; }
}

sealed class StringLengthQueryNode : QueryNode
{
    public StringLengthQueryNode(QueryNode operand) : base(typeof(int))
    {
        Operand = operand;
    }

    public QueryNode Operand { get; }
}

/// String method with a constant argument (string or char) on a string operand.
sealed class StringMethodQueryNode : QueryNode
{
    public StringMethodQueryNode(QueryStringOp operation, StringComparison comparison, QueryNode operand,
        object? argument) : base(typeof(bool))
    {
        Operation = operation;
        Comparison = comparison;
        Operand = operand;
        Argument = argument;
    }

    public QueryStringOp Operation { get; }
    public StringComparison Comparison { get; }
    public QueryNode Operand { get; }
    public object? Argument { get; }
}

/// string.CompareOrdinal of a string operand and a constant; only comparing its result to zero is supported.
sealed class StringCompareOrdinalQueryNode : QueryNode
{
    public StringCompareOrdinalQueryNode(QueryNode operand, string? argument, bool reversed) : base(typeof(int))
    {
        Operand = operand;
        Argument = argument;
        Reversed = reversed;
    }

    public QueryNode Operand { get; }
    public string? Argument { get; }

    /// The constant was the first argument, so the order is reversed.
    public bool Reversed { get; }
}

sealed class StringIsNullOrEmptyQueryNode : QueryNode
{
    public StringIsNullOrEmptyQueryNode(QueryNode operand) : base(typeof(bool))
    {
        Operand = operand;
    }

    public QueryNode Operand { get; }
}

/// Membership in a constant collection; Set is a QueryStringSet for strings and an IReadOnlySet of the storage type
/// otherwise.
sealed class InSetQueryNode : QueryNode
{
    public InSetQueryNode(QueryNode operand, object set) : base(typeof(bool))
    {
        Operand = operand;
        Set = set;
    }

    public QueryNode Operand { get; }
    public object Set { get; }
}

/// Produced by constant folding of operations which throw, for example a string method on a null constant.
sealed class ThrowQueryNode : QueryNode
{
    public ThrowQueryNode(QueryException exception, Type type) : base(type)
    {
        Exception = exception;
    }

    public QueryException Exception { get; }
}

class RelationQuery<T> : IOrderedQueryable<T>, IQueryProvider, IRelationQueryChain where T : class
{
    readonly IInternalObjectDBTransaction _transaction;
    readonly RelationInfo _relationInfo;
    readonly RelationInfo.ItemLoaderInfo _loaderInfo;
    Expression? _expression;
    readonly QueryByIdQueryNode _queryNode;

    // Set for a query made by an intercepted operator: its expression is built only when asked for.
    readonly RelationQueryLink? _link;
    readonly RelationQuery<T>? _root;

    public RelationQuery(IInternalObjectDBTransaction transaction, RelationInfo relationInfo,
        RelationInfo.ItemLoaderInfo loaderInfo)
    {
        _transaction = transaction;
        _relationInfo = relationInfo;
        _loaderInfo = loaderInfo;
        _queryNode = new(relationInfo.Id, typeof(T));
        _expression = Expression.Constant(this);
    }

    RelationQuery(RelationQuery<T> source, Expression expression)
    {
        _transaction = source._transaction;
        _relationInfo = source._relationInfo;
        _loaderInfo = source._loaderInfo;
        _queryNode = source._queryNode;
        _expression = expression;
    }

    RelationQuery(RelationQuery<T> root, RelationQueryLink link)
    {
        _transaction = root._transaction;
        _relationInfo = root._relationInfo;
        _loaderInfo = root._loaderInfo;
        _queryNode = root._queryNode;
        _link = link;
        _root = root;
    }

    internal RelationInfo RelationInfo => _relationInfo;

    RelationQueryLink? IRelationQueryChain.Link => _link;

    public bool IsRoot => _link == null && _expression is ConstantExpression;

    TQueryable? IRelationQueryChain.Append<TElement, TQueryable>(RelationQueryLink link) where TQueryable : class
    {
        var root = _root ?? this;
        object result = typeof(TElement) == typeof(T)
            ? new RelationQuery<T>(root, link)
            : new RelationQueryProjected<T, TElement>(root, link);
        return result as TQueryable;
    }

    object? IRelationQueryChain.ExecuteIntercepted(RelationQueryCallSite site, object? argument1, object? argument2,
        Func<Expression> terminalExpression) =>
        RelationQueryInterception.Execute(_root ?? this, _link, site, argument1, argument2, terminalExpression,
            _link != null || IsRoot);

    public IEnumerator<T> GetEnumerator()
    {
        if (_link != null) return RelationQueryInterception.Enumerate(_root!, _link, this);
        return Enumerate(Start(_expression!), RelationQueryRequest.ForSource(_expression!));
    }

    internal IEnumerator<T> Enumerate(RelationQueryExecution execution, RelationQueryRequest request)
    {
        var source = Source(execution, request);
        if (source.HasOrdering || source.HasPaging) return CreateWindow(execution, source, null);
        return new RelationQueryEnumerator<T>(_transaction, _loaderInfo, CompilePlan(execution, source.Filter),
            execution.Values);
    }

    // Conversions and plans of an execution are memoized in the template of its expression shape, per relation.
    RelationQueryExecution Start(Expression expression) => _relationInfo.QueryCache.Start(expression);

    RelationQuerySource Source(RelationQueryExecution execution, RelationQueryRequest request)
    {
        var source = execution.Memo("source", () =>
        {
            // The Select lambda is taken from each execution's request instead, so the cache neither converts the
            // captured values of an earlier execution nor keeps them alive.
            var built = RelationQueryAstBuilder.BuildSource(request.Source, _queryNode);
            built.HasProjection = built.Projection != null;
            built.Projection = null;
            return built;
        });
        var bound = source.Bind(execution.Values);
        if (source.HasProjection) bound.Projection = request.Projection;
        return bound;
    }

    RelationQueryPlan CompilePlan(RelationQueryExecution execution, QueryNode filter, QueryNode? selector = null,
        bool orderSensitive = true, IReadOnlyList<QueryNode>? sortKeys = null,
        IReadOnlyList<QueryNode>? projection = null, bool eagerProjection = false)
    {
        return execution.Memo(new PlanKey(filter, selector, orderSensitive, sortKeys, projection, eagerProjection),
            () => new RelationQueryCompiler(_relationInfo, typeof(T)).Compile(_transaction, filter, selector,
                orderSensitive, sortKeys, projection, eagerProjection));
    }

    // Template nodes are reused between executions, so references identify what a plan was compiled from.
    sealed record PlanKey(QueryNode Filter, QueryNode? Selector, bool OrderSensitive,
        IReadOnlyList<QueryNode>? SortKeys, IReadOnlyList<QueryNode>? Projection, bool EagerProjection);

    QueryNode Predicate(RelationQueryExecution execution, LambdaExpression lambda) =>
        execution.Memo("predicate", () => RelationQueryAstBuilder.BuildPredicate(lambda, _queryNode));

    QueryNode Selector(RelationQueryExecution execution, LambdaExpression lambda) =>
        execution.Memo("selector", () => RelationQueryAstBuilder.BuildSelector(lambda, _queryNode));

    // The cached projection has no transaction; each execution gets a copy with its own.
    RelationQueryProjection<T> Projection(RelationQueryExecution execution, LambdaExpression lambda) =>
        execution.Memo("projection", () => new RelationQueryProjection<T>(lambda, _queryNode, null!, _loaderInfo))
            .ForExecution(_transaction, _loaderInfo);

    RelationQueryWindow<T> CreateWindow(RelationQueryExecution execution, RelationQuerySource source,
        QueryNode? selector, bool keepFrames = false, RelationQueryProjection<T>? projection = null)
    {
        // A projection which cannot throw is computed while scanning when every matching row is returned; Count
        // windows and sorted windows return fewer rows than they test.
        var plan = CompilePlan(execution, source.Filter, selector, true, null, projection?.Selectors,
            projection is { CanThrow: false } && source.Skip == 0);
        var order = RelationQueryOrder.Forward;
        var descending = Array.Empty<bool>();
        if (source.HasOrdering)
        {
            // Streaming in key order returns rows before all are tested, which LINQ's OrderBy does not, so it is only
            // possible when no test can throw.
            if (!plan.CanThrow && MatchesPrimaryKeyOrder(plan, source.Ordering, out var backward))
            {
                order = backward ? RelationQueryOrder.Backward : RelationQueryOrder.Forward;
            }
            else
            {
                var ordering = source.Ordering;
                var keys = execution.Memo(("sortKeys", ordering), () => ordering.ConvertAll(o => o.Key));
                plan = CompilePlan(execution, source.Filter, selector, true, keys, projection?.Selectors);
                order = RelationQueryOrder.Sorted;
                descending = source.Ordering.ConvertAll(o => o.Descending).ToArray();
            }
        }

        projection?.Bind(plan);
        return new(_transaction, _loaderInfo, plan, order, descending, source.Skip, source.Take, keepFrames,
            execution.Values);
    }

    // Rows come in primary key order, so an ordering by the key fields not fixed by equalities, in key order and one
    // direction, is already satisfied. Descending works only over all of them, because ties keep source order.
    bool MatchesPrimaryKeyOrder(RelationQueryPlan plan, List<(QueryNode Key, bool Descending)> ordering,
        out bool backward)
    {
        backward = false;
        var remaining = new List<TableFieldInfo>();
        foreach (var field in _relationInfo.ClientRelationVersionInfo.PrimaryKeyFields.Span)
            if (!plan.FixedPrimaryKeyFields.Contains(field.Name)) remaining.Add(field);
        var keys = ordering.FindAll(o => !(o.Key is PropQueryNode prop && plan.FixedPrimaryKeyFields.Contains(prop.Name)));
        if (keys.Count == 0) return true;
        if (keys.Count > remaining.Count) return false;
        for (var i = 0; i < keys.Count; i++)
        {
            if (keys[i].Key is not PropQueryNode prop || prop.Name != remaining[i].Name ||
                keys[i].Descending != keys[0].Descending ||
                !RelationQueryKeyRanges.IsOrderPreserving(remaining[i].Handler!))
                return false;
        }

        backward = keys[0].Descending;
        return !backward || keys.Count == remaining.Count;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public Type ElementType => typeof(T);
    public Expression Expression => _expression ??= _link!.Materialize();
    public IQueryProvider Provider => _root ?? this;

    public IQueryable CreateQuery(Expression expression)
    {
        ValidateQueryExpression(expression);
        return new RelationQuery<T>(this, expression);
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        ValidateQueryExpression(expression);
        if (typeof(TElement) == typeof(T)) return (IQueryable<TElement>)(object)new RelationQuery<T>(this, expression);
        return new RelationQueryProjected<T, TElement>(this, expression);
    }

    internal IEnumerator<TElement> EnumerateProjected<TElement>(Expression expression) =>
        EnumerateProjected<TElement>(Start(expression), RelationQueryRequest.ForSource(expression));

    internal IEnumerator<TElement> EnumerateProjected<TElement>(RelationQueryExecution execution,
        RelationQueryRequest request)
    {
        var source = Source(execution, request);
        if (source.Projection == null) return (IEnumerator<TElement>)(object)Enumerate(execution, request);
        var projection = Projection(execution, source.Projection);
        return new RelationQueryProjectedEnumerator<T, TElement>(
            CreateWindow(execution, source, null, false, projection), projection);
    }

    public TResult Execute<TResult>(Expression expression)
    {
        var result = Execute(expression);
        return result is null ? default! : (TResult)result;
    }

    /// Executes Queryable terminal operators on the stored rows; only returned elements are materialized.
    public object? Execute(Expression expression)
    {
        if (expression is not MethodCallExpression call || call.Method.DeclaringType != typeof(Queryable))
            throw new NotSupportedException("Relation query supports only Queryable operators.");
        return Execute(Start(call), RelationQueryRequest.ForTerminal(call));
    }

    internal object? Execute(RelationQueryExecution execution, RelationQueryRequest request)
    {
        var querySource = Source(execution, request);
        var lambda = request.Lambda;
        var name = request.Name;
        if (querySource.Projection != null) return ExecuteProjected(execution, request, querySource);
        if (querySource.HasOrdering || querySource.HasPaging)
            return ExecuteWindowed(execution, request, querySource);
        var source = querySource.Filter;
        switch (name)
        {
            case nameof(Queryable.Count):
            case nameof(Queryable.LongCount):
            {
                if (lambda != null) source = Where(execution, source, lambda);
                using var scanner = CreateScanner(execution, source, null, false);
                var count = scanner.Count();
                if (name == nameof(Queryable.Count)) return checked((int)count);
                return count;
            }
            case nameof(Queryable.Any):
            {
                if (lambda != null) source = Where(execution, source, lambda);
                using var scanner = CreateScanner(execution, source, null, false);
                return scanner.MoveNext();
            }
            case nameof(Queryable.All) when lambda != null:
            {
                // All stops at the first row failing the predicate, exactly like Any of its negation.
                var filter = source;
                source = execution.Memo(("all", filter),
                    () => new WhereQueryNode(filter, new NotQueryNode(Predicate(execution, lambda))));
                using var scanner = CreateScanner(execution, source, null, false);
                return !scanner.MoveNext();
            }
            case nameof(Queryable.First):
            case nameof(Queryable.FirstOrDefault):
            case nameof(Queryable.Single):
            case nameof(Queryable.SingleOrDefault):
            case nameof(Queryable.Last):
            case nameof(Queryable.LastOrDefault):
                return ExecuteElement(execution, request, source);
            case nameof(Queryable.Min):
            case nameof(Queryable.Max):
            case nameof(Queryable.Sum):
            case nameof(Queryable.Average):
                if (lambda == null)
                    throw new NotSupportedException($"Relation query supports Queryable.{name} only with a selector.");
                return Aggregate(execution, name, querySource, Selector(execution, lambda));
            default:
                throw new NotSupportedException($"Relation query does not support Queryable.{name}.");
        }
    }

    // Terminal operators after OrderBy, Skip or Take.
    object? ExecuteWindowed(RelationQueryExecution execution, RelationQueryRequest request,
        RelationQuerySource source)
    {
        var name = request.Name;
        var lambda = request.Lambda;
        var isAggregate = name is nameof(Queryable.Min) or nameof(Queryable.Max) or nameof(Queryable.Sum)
            or nameof(Queryable.Average);
        if (lambda != null && !isAggregate)
        {
            // LINQ tests such a predicate on rows in sorted order, which equals filtering before ordering only when it
            // cannot throw, and only without paging.
            var predicate = Predicate(execution, lambda);
            if (source.HasPaging || RelationQueryCompiler.CanThrowNode(predicate))
                throw new NotSupportedException(
                    $"Relation query supports Queryable.{name} with a predicate after OrderBy only when the predicate cannot throw, and not after Skip or Take.");
            var filter = source.Filter;
            source.Filter = execution.Memo(("windowFilter", filter), () => new WhereQueryNode(filter,
                name == nameof(Queryable.All) ? new NotQueryNode(predicate) : predicate));
        }

        switch (name)
        {
            case nameof(Queryable.Count):
            case nameof(Queryable.LongCount):
            case nameof(Queryable.Any):
            case nameof(Queryable.All):
            {
                var count = CountWindow(execution, source, name is nameof(Queryable.Any) or nameof(Queryable.All));
                return name switch
                {
                    nameof(Queryable.Count) => checked((int)count),
                    nameof(Queryable.LongCount) => count,
                    nameof(Queryable.Any) => count > 0,
                    _ => count == 0
                };
            }
            case nameof(Queryable.First):
            case nameof(Queryable.FirstOrDefault):
            case nameof(Queryable.Single):
            case nameof(Queryable.SingleOrDefault):
            case nameof(Queryable.Last):
            case nameof(Queryable.LastOrDefault):
            {
                var hasPredicate = lambda != null;
                var orDefault = name.EndsWith("OrDefault", StringComparison.Ordinal);
                var defaultValue = orDefault ? request.DefaultValue : null;
                using var window = CreateWindow(execution, source, null);
                if (!window.MoveNext()) return orDefault ? defaultValue : throw NoElements(hasPredicate);
                if (name.StartsWith("Last", StringComparison.Ordinal))
                {
                    var lastKey = window.Scanner.Cursor.GetKeySpan(Span<byte>.Empty, true).ToArray();
                    while (window.MoveNext()) lastKey = window.Scanner.Cursor.GetKeySpan(Span<byte>.Empty, true).ToArray();
                    window.Scanner.Cursor.FindExactKey(lastKey);
                    return RelationQueryRows.Load<T>(_transaction, _loaderInfo, window.Scanner.Cursor);
                }

                var result = window.Current;
                if (name.StartsWith("Single", StringComparison.Ordinal) && window.MoveNext())
                    throw new InvalidOperationException(hasPredicate
                        ? "Sequence contains more than one matching element"
                        : "Sequence contains more than one element");
                return result;
            }
            case nameof(Queryable.Min):
            case nameof(Queryable.Max):
            case nameof(Queryable.Sum):
            case nameof(Queryable.Average):
            {
                if (lambda == null)
                    throw new NotSupportedException($"Relation query supports Queryable.{name} only with a selector.");
                return Aggregate(execution, name, source, Selector(execution, lambda));
            }
            default:
                throw new NotSupportedException($"Relation query does not support Queryable.{name}.");
        }
    }

    object? Aggregate(RelationQueryExecution execution, string name, RelationQuerySource source, QueryNode selector)
    {
        if (!source.HasPaging && (!source.HasOrdering || name is nameof(Queryable.Min) or nameof(Queryable.Max)))
        {
            // Without paging, Min and Max do not depend on row order; sums and averages of floats or with overflow
            // checks follow primary key order like LINQ.
            using var scanner = CreateScanner(execution, source.Filter, selector,
                name is nameof(Queryable.Sum) or nameof(Queryable.Average));
            var aggregator = RelationQueryAggregator.Create(name, scanner.Plan.Result!);
            scanner.Aggregate(aggregator);
            return aggregator.Result();
        }

        // Sums and averages follow the window order like LINQ.
        using var window = CreateWindow(execution, source, selector, true);
        var windowAggregator = RelationQueryAggregator.Create(name, window.Scanner.Plan.Result!);
        while (window.MoveNext()) windowAggregator.Add(window.CurrentFrame);
        return windowAggregator.Result();
    }

    // Terminal operators after Select. Select values cannot throw, so counting ignores them.
    object? ExecuteProjected(RelationQueryExecution execution, RelationQueryRequest request,
        RelationQuerySource source)
    {
        var name = request.Name;
        var lambda = request.Lambda;
        if (lambda != null)
            throw new NotSupportedException($"Relation query does not support Queryable.{name} with a lambda after Select.");
        var projection = Projection(execution, source.Projection!);
        source.Projection = null;
        switch (name)
        {
            case nameof(Queryable.Count):
            case nameof(Queryable.LongCount):
            case nameof(Queryable.Any):
            {
                long count;
                if (name != nameof(Queryable.Any) && projection.CanThrow)
                {
                    // Like LINQ's Select(...).Count(), every counted row runs the selector.
                    count = 0;
                    using var window = CreateWindow(execution, source, null, false, projection);
                    while (window.MoveNext())
                    {
                        window.Scanner.ComputeProjection();
                        count++;
                    }
                }
                else if (source.HasOrdering || source.HasPaging)
                {
                    count = CountWindow(execution, source, name == nameof(Queryable.Any));
                }
                else
                {
                    using var scanner = CreateScanner(execution, source.Filter, null, false);
                    count = name == nameof(Queryable.Any) ? scanner.MoveNext() ? 1 : 0 : scanner.Count();
                }

                if (name == nameof(Queryable.Count)) return checked((int)count);
                if (name == nameof(Queryable.LongCount)) return count;
                return count > 0;
            }
            case nameof(Queryable.First):
            case nameof(Queryable.FirstOrDefault):
            case nameof(Queryable.Single):
            case nameof(Queryable.SingleOrDefault):
            case nameof(Queryable.Last):
            case nameof(Queryable.LastOrDefault):
            {
                var orDefault = name.EndsWith("OrDefault", StringComparison.Ordinal);
                // A null default of a value type becomes default(TResult) in Execute<TResult>.
                var defaultValue = orDefault ? request.DefaultValue : null;
                if (name.StartsWith("Last", StringComparison.Ordinal) && !source.HasOrdering && !source.HasPaging)
                {
                    // LINQ's Where and Select over a list search for the last element backwards, like Last.
                    var plan = CompilePlan(execution, source.Filter, null, true, null, projection.Selectors);
                    projection.Bind(plan);
                    using var scanner = new RelationQueryScanner(_transaction, plan, execution.Values);
                    if (!scanner.MoveLast()) return orDefault ? defaultValue : throw NoElements(false);
                    return projection.BuildBoxed(scanner);
                }

                using var window = CreateWindow(execution, source, null, false, projection);
                if (!window.MoveNext()) return orDefault ? defaultValue : throw NoElements(false);
                if (name.StartsWith("Last", StringComparison.Ordinal))
                {
                    // Like LINQ, only the last row runs the selector.
                    var lastKey = window.Scanner.Cursor.GetKeySpan(Span<byte>.Empty, true).ToArray();
                    while (window.MoveNext()) lastKey = window.Scanner.Cursor.GetKeySpan(Span<byte>.Empty, true).ToArray();
                    window.Scanner.Cursor.FindExactKey(lastKey);
                    return projection.BuildBoxed(window.Scanner);
                }

                var element = projection.BuildBoxed(window.Scanner);
                if (name.StartsWith("Single", StringComparison.Ordinal) && window.MoveNext())
                {
                    // LINQ moves to the second element, running its selector, before failing.
                    projection.BuildBoxed(window.Scanner);
                    throw new InvalidOperationException("Sequence contains more than one element");
                }

                return element;
            }
            case nameof(Queryable.Min):
            case nameof(Queryable.Max):
            case nameof(Queryable.Sum):
            case nameof(Queryable.Average):
                return Aggregate(execution, name, source, projection.SingleSelector ??
                    throw new NotSupportedException($"Relation query supports Queryable.{name} after Select only of a single value."));
            default:
                throw new NotSupportedException($"Relation query does not support Queryable.{name} after Select.");
        }
    }

    // Rows in the window, stopping at the first one when only presence matters. Like LINQ, an ordered source tests
    // every row, while an unordered one stops at the end of the window.
    long CountWindow(RelationQueryExecution execution, RelationQuerySource source, bool presenceOnly)
    {
        // Take(0) is empty without testing any row, like LINQ.
        if (source.Take == 0) return 0;
        if (source.HasOrdering)
        {
            using var scanner = CreateScanner(execution, source.Filter, null, false);
            var total = Math.Max(0, scanner.Count() - source.Skip);
            return source.Take is { } take ? Math.Min(total, take) : total;
        }

        using var window = CreateWindow(execution, source, null);
        var count = 0L;
        while (window.MoveNext())
        {
            count++;
            if (presenceOnly) break;
        }

        return count;
    }

    object? ExecuteElement(RelationQueryExecution execution, RelationQueryRequest request, QueryNode source)
    {
        var name = request.Name;
        var lambda = request.Lambda;
        var hasPredicate = lambda != null;
        var sourceHasWhere = source is WhereQueryNode;
        if (hasPredicate) source = Where(execution, source, lambda!);
        var orDefault = name.EndsWith("OrDefault", StringComparison.Ordinal);
        // FirstOrDefault(source, defaultValue) and FirstOrDefault(source, predicate, defaultValue).
        var defaultValue = orDefault ? request.DefaultValue : null;
        using var scanner = CreateScanner(execution, source, null);
        if (name.StartsWith("Last", StringComparison.Ordinal))
        {
            // LINQ-to-Objects over a list searches backwards, except Last(predicate) after Where, which tests every row
            // forwards; the choice decides which rows can throw.
            if (hasPredicate && sourceHasWhere)
            {
                var lastIndex = -1L;
                while (scanner.MoveNext()) lastIndex = scanner.Cursor.GetKeyIndex();
                if (lastIndex < 0) return orDefault ? defaultValue : throw NoElements(true);
                scanner.Cursor.FindKeyIndex(lastIndex);
            }
            else if (!scanner.MoveLast())
            {
                return orDefault ? defaultValue : throw NoElements(hasPredicate);
            }

            return RelationQueryRows.Load<T>(_transaction, _loaderInfo, scanner.Cursor);
        }

        if (!scanner.MoveNext()) return orDefault ? defaultValue : throw NoElements(hasPredicate);
        var result = RelationQueryRows.Load<T>(_transaction, _loaderInfo, scanner.Cursor);
        if (name.StartsWith("Single", StringComparison.Ordinal) && scanner.MoveNext())
            throw new InvalidOperationException(hasPredicate
                ? "Sequence contains more than one matching element"
                : "Sequence contains more than one element");
        return result;
    }

    static InvalidOperationException NoElements(bool hasPredicate)
    {
        return new(hasPredicate ? "Sequence contains no matching element" : "Sequence contains no elements");
    }

    WhereQueryNode Where(RelationQueryExecution execution, QueryNode source, LambdaExpression predicate)
    {
        return execution.Memo(("where", source), () => new WhereQueryNode(source, Predicate(execution, predicate)));
    }

    RelationQueryScanner CreateScanner(RelationQueryExecution execution, QueryNode source, QueryNode? selector,
        bool orderSensitive = true)
    {
        return new(_transaction, CompilePlan(execution, source, selector, orderSensitive), execution.Values);
    }

    static void ValidateQueryExpression(Expression expression)
    {
        if (expression is MethodCallExpression methodCall &&
            methodCall.Method.DeclaringType == typeof(Queryable) &&
            methodCall.Method.Name is nameof(Queryable.Where) or nameof(Queryable.OrderBy)
                or nameof(Queryable.OrderByDescending) or nameof(Queryable.ThenBy)
                or nameof(Queryable.ThenByDescending) or nameof(Queryable.Skip) or nameof(Queryable.Take)
                or nameof(Queryable.Select))
            return;
        if (expression is ConstantExpression)
            return;
        throw new NotSupportedException(
            "Relation query supports only Queryable.Where, OrderBy, ThenBy, Select, Skip and Take.");
    }
}

/// A relation query source as built from Queryable calls: a filter, then an optional ordering, then paging.
sealed class RelationQuerySource
{
    public RelationQuerySource(QueryNode filter)
    {
        Filter = filter;
    }

    /// QueryByIdQueryNode or a chain of WhereQueryNode.
    public QueryNode Filter { get; set; }

    /// Sort keys, most significant first.
    public List<(QueryNode Key, bool Descending)> Ordering { get; private init; } = [];

    // Where ThenBy keys of the latest OrderBy are inserted; keys after it come from earlier OrderBy calls, whose order
    // a stable sort keeps among ties.
    internal int OrderingGroupEnd;

    /// Skip and Take calls in order, with a literal count or the index of the parameter holding it; Skip and Take
    /// are computed from them for each execution by Bind.
    public List<(bool IsSkip, int Parameter, int Count)> Paging { get; private init; } = [];

    public long Skip { get; private set; }
    public long? Take { get; private set; }

    /// Select lambda over the row; null when rows themselves are returned.
    public LambdaExpression? Projection { get; set; }

    /// Whether the query has a Select, whose lambda cached sources do not keep.
    public bool HasProjection { get; set; }

    public bool HasOrdering => Ordering.Count > 0;
    public bool HasPaging => Skip > 0 || Take != null;

    /// A copy with Skip and Take of these parameter values; the filter, ordering and projection are shared.
    public RelationQuerySource Bind(object?[]? values)
    {
        var bound = new RelationQuerySource(Filter)
        {
            OrderingGroupEnd = OrderingGroupEnd, Projection = Projection, HasProjection = HasProjection,
            Ordering = Ordering, Paging = Paging
        };
        foreach (var (isSkip, parameter, literal) in Paging)
        {
            var count = parameter < 0 ? literal : (int)values![parameter]!;
            if (isSkip)
            {
                if (count <= 0) continue;
                if (bound.Take != null) bound.Take = Math.Max(0, bound.Take.Value - count);
                bound.Skip += count;
            }
            else
            {
                count = Math.Max(0, count);
                bound.Take = bound.Take == null ? count : Math.Min(bound.Take.Value, count);
            }
        }

        return bound;
    }
}

/// What executing a query needs from its expression, which an intercepted call site provides without building it: the
/// source, its Select lambda, and the name, lambda and default value of a terminal operator.
sealed class RelationQueryRequest
{
    readonly Func<Expression>? _sourceFactory;
    Expression? _source;

    RelationQueryRequest(Expression? source, Func<Expression>? sourceFactory)
    {
        _source = source;
        _sourceFactory = sourceFactory;
    }

    /// The source expression, built for intercepted call sites only when a conversion is not cached.
    public Expression Source => _source ??= _sourceFactory!();

    public LambdaExpression? Projection { get; private init; }
    public string Name { get; private init; } = "";
    public LambdaExpression? Lambda { get; private init; }
    public object? DefaultValue { get; private init; }

    public static RelationQueryRequest ForSource(Expression source) =>
        new(source, null) { Projection = RelationQueryAstBuilder.FindProjection(source) };

    public static RelationQueryRequest ForTerminal(MethodCallExpression call)
    {
        var arguments = call.Arguments;
        var lambda = arguments.Count > 1 ? RelationQueryAstBuilder.TryGetLambda(arguments[1]) : null;
        // FirstOrDefault(source, defaultValue) and FirstOrDefault(source, predicate, defaultValue).
        var hasDefault = arguments.Count > (lambda != null ? 2 : 1);
        return new(arguments[0], null)
        {
            Projection = RelationQueryAstBuilder.FindProjection(arguments[0]), Name = call.Method.Name,
            Lambda = lambda,
            DefaultValue = hasDefault ? RelationQueryAstBuilder.EvaluateConstant(arguments[^1]) : null
        };
    }

    public static RelationQueryRequest ForLinks(Func<Expression> source, LambdaExpression? projection) =>
        new(null, source) { Projection = projection };

    public static RelationQueryRequest ForLinks(Func<Expression> source, LambdaExpression? projection, string name,
        LambdaExpression? lambda, object? defaultValue) =>
        new(null, source) { Projection = projection, Name = name, Lambda = lambda, DefaultValue = defaultValue };
}

static class RelationQueryAstBuilder
{
    /// Builds a source made only of Where calls.
    public static QueryNode Build(Expression expression, QueryByIdQueryNode query)
    {
        var source = BuildSource(expression, query);
        if (source.HasOrdering || source.Paging.Count > 0)
            throw new NotSupportedException("Relation query supports only Queryable.Where here.");
        return source.Filter;
    }

    public static RelationQuerySource BuildSource(Expression expression, QueryByIdQueryNode query)
    {
        if (expression is ConstantExpression) return new(query);
        if (expression is not MethodCallExpression { Method.DeclaringType: var declaringType } call ||
            declaringType != typeof(Queryable))
            throw new NotSupportedException("Relation query supports only Queryable operators.");
        var source = BuildSource(call.Arguments[0], query);
        var name = call.Method.Name;
        switch (name)
        {
            case nameof(Queryable.Select):
            {
                if (source.Projection != null || StripQuote(call.Arguments[1]) is not LambdaExpression { Parameters.Count: 1 } lambda)
                    throw new NotSupportedException("Relation query supports only one Select with a single parameter.");
                // Select(p => p) returns rows unchanged.
                if (lambda.Body != lambda.Parameters[0]) source.Projection = lambda;
                return source;
            }
            case nameof(Queryable.Where):
                if (StripQuote(call.Arguments[1]) is not LambdaExpression { Parameters.Count: 1 })
                    throw new NotSupportedException("Relation query does not support Where with an element index.");
                if (source.HasOrdering || source.Paging.Count > 0 || source.Projection != null)
                    throw new NotSupportedException("Relation query supports Where only before OrderBy, Select, Skip and Take.");
                source.Filter = new WhereQueryNode(source.Filter,
                    BuildPredicate((LambdaExpression)StripQuote(call.Arguments[1]), query));
                return source;
            case nameof(Queryable.OrderBy):
            case nameof(Queryable.OrderByDescending):
            case nameof(Queryable.ThenBy):
            case nameof(Queryable.ThenByDescending):
            {
                if (source.Paging.Count > 0 || source.Projection != null || call.Arguments.Count != 2)
                    throw new NotSupportedException($"Relation query does not support {name} here.");
                var key = BuildSelector((LambdaExpression)StripQuote(call.Arguments[1]), query);
                if (key.Type == typeof(string) ? key is not PropQueryNode : !RelationQueryTypes.TryGetStorage(key.Type, out _, out _, out _))
                    throw new NotSupportedException($"Relation query does not support ordering by {key.Type.ToSimpleName()}.");
                var descending = name.EndsWith("Descending", StringComparison.Ordinal);
                if (name.StartsWith("OrderBy", StringComparison.Ordinal))
                {
                    source.Ordering.Insert(0, (key, descending));
                    source.OrderingGroupEnd = 1;
                }
                else
                {
                    source.Ordering.Insert(source.OrderingGroupEnd++, (key, descending));
                }

                return source;
            }
            case nameof(Queryable.Skip) when call.Arguments[1].Type == typeof(int):
            case nameof(Queryable.Take) when call.Arguments[1].Type == typeof(int):
            {
                // Counts from parameters are bound for each execution, so paging does not bake them.
                var isSkip = name == nameof(Queryable.Skip);
                var parameter = RelationQueryParameterScope.TryGetParameter(call.Arguments[1])?.Index ?? -1;
                var count = parameter < 0 ? (int)EvaluateConstant(call.Arguments[1])! : 0;
                if (parameter < 0 && isSkip && count <= 0) return source;
                source.Paging.Add((isSkip, parameter, count));
                return source;
            }
            default:
                throw new NotSupportedException($"Relation query does not support Queryable.{name}.");
        }
    }

    public static LambdaExpression? TryGetLambda(Expression expression) => StripQuote(expression) as LambdaExpression;

    /// The Select lambda of a source BuildSource accepted, or null for Select(p => p).
    public static LambdaExpression? FindProjection(Expression expression)
    {
        while (expression is MethodCallExpression call)
        {
            if (call.Method.Name == nameof(Queryable.Select))
            {
                var lambda = (LambdaExpression)StripQuote(call.Arguments[1]);
                return lambda.Body == lambda.Parameters[0] ? null : lambda;
            }

            expression = call.Arguments[0];
        }

        return null;
    }

    public static QueryNode BuildPredicate(LambdaExpression lambda, QueryByIdQueryNode query)
    {
        return new ExpressionConverter(lambda.Parameters[0], query).ConvertPredicate(lambda.Body);
    }

    public static QueryNode BuildSelector(LambdaExpression lambda, QueryByIdQueryNode query)
    {
        return new ExpressionConverter(lambda.Parameters[0], query).ConvertValue(lambda.Body);
    }

    public static QueryNode BuildValue(Expression expression, ParameterExpression parameter, QueryByIdQueryNode query)
    {
        return new ExpressionConverter(parameter, query).ConvertValue(expression);
    }

    static Expression StripQuote(Expression expression)
    {
        // Queryable operators receive Expression<Func<...>> as a quoted lambda argument.
        while (expression.NodeType == ExpressionType.Quote)
            expression = ((UnaryExpression)expression).Operand;
        return expression;
    }

    sealed class ExpressionConverter
    {
        readonly ParameterExpression _parameter;
        readonly QueryByIdQueryNode _query;

        public ExpressionConverter(ParameterExpression parameter, QueryByIdQueryNode query)
        {
            _parameter = parameter;
            _query = query;
        }

        public QueryNode ConvertPredicate(Expression expression) => AsPredicate(Convert(expression), expression);

        public QueryNode ConvertValue(Expression expression) => Convert(expression);

        QueryNode AsPredicate(QueryNode node, Expression expression)
        {
            if (node is BoolAndQueryNode or BoolOrQueryNode or NotQueryNode or CompareQueryNode or HasValueQueryNode
                or StringMethodQueryNode or StringIsNullOrEmptyQueryNode or InSetQueryNode or ThrowQueryNode
                or ConstantQueryNode { Value: bool })
                return node;
            if (node.Type == typeof(bool))
                return new CompareQueryNode(node, new ConstantQueryNode(true, typeof(bool)), QueryCompare.Equal);
            throw Unsupported(expression);
        }

        QueryNode Convert(Expression expression)
        {
            if (!DependsOnParameter(expression))
                return RelationQueryParameterScope.TryGetParameter(expression) ??
                       new ConstantQueryNode(EvaluateConstant(expression), expression.Type);
            switch (expression.NodeType)
            {
                case ExpressionType.AndAlso:
                {
                    var binary = (BinaryExpression)expression;
                    return new BoolAndQueryNode(ConvertPredicate(binary.Left), ConvertPredicate(binary.Right));
                }
                case ExpressionType.OrElse:
                {
                    var binary = (BinaryExpression)expression;
                    return new BoolOrQueryNode(ConvertPredicate(binary.Left), ConvertPredicate(binary.Right));
                }
                case ExpressionType.Not when expression.Type == typeof(bool):
                    return new NotQueryNode(ConvertPredicate(((UnaryExpression)expression).Operand));
                case ExpressionType.Equal:
                    return ConvertCompare((BinaryExpression)expression, QueryCompare.Equal);
                case ExpressionType.NotEqual:
                    return ConvertCompare((BinaryExpression)expression, QueryCompare.NotEqual);
                case ExpressionType.LessThan:
                    return ConvertCompare((BinaryExpression)expression, QueryCompare.Less);
                case ExpressionType.LessThanOrEqual:
                    return ConvertCompare((BinaryExpression)expression, QueryCompare.LessOrEqual);
                case ExpressionType.GreaterThan:
                    return ConvertCompare((BinaryExpression)expression, QueryCompare.Greater);
                case ExpressionType.GreaterThanOrEqual:
                    return ConvertCompare((BinaryExpression)expression, QueryCompare.GreaterOrEqual);
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    return ConvertConversion((UnaryExpression)expression);
                case ExpressionType.Add:
                    return ConvertArithmetic((BinaryExpression)expression, QueryArithmetic.Add);
                case ExpressionType.AddChecked:
                    return ConvertArithmetic((BinaryExpression)expression, QueryArithmetic.AddChecked);
                case ExpressionType.Subtract:
                    return ConvertArithmetic((BinaryExpression)expression, QueryArithmetic.Subtract);
                case ExpressionType.SubtractChecked:
                    return ConvertArithmetic((BinaryExpression)expression, QueryArithmetic.SubtractChecked);
                case ExpressionType.Multiply:
                    return ConvertArithmetic((BinaryExpression)expression, QueryArithmetic.Multiply);
                case ExpressionType.MultiplyChecked:
                    return ConvertArithmetic((BinaryExpression)expression, QueryArithmetic.MultiplyChecked);
                case ExpressionType.Divide:
                    return ConvertArithmetic((BinaryExpression)expression, QueryArithmetic.Divide);
                case ExpressionType.Modulo:
                    return ConvertArithmetic((BinaryExpression)expression, QueryArithmetic.Modulo);
                case ExpressionType.Negate:
                    return ConvertNegate((UnaryExpression)expression, QueryArithmetic.Negate);
                case ExpressionType.NegateChecked:
                    return ConvertNegate((UnaryExpression)expression, QueryArithmetic.NegateChecked);
                case ExpressionType.UnaryPlus:
                    return Convert(((UnaryExpression)expression).Operand);
                case ExpressionType.MemberAccess:
                    return ConvertMember((MemberExpression)expression);
                case ExpressionType.Call:
                    return ConvertCall((MethodCallExpression)expression);
                default:
                    throw Unsupported(expression);
            }
        }

        QueryNode ConvertCompare(BinaryExpression expression, QueryCompare compare)
        {
            // A comparison lifted to null returns bool? and cannot be a predicate.
            if (expression.Type != typeof(bool)) throw Unsupported(expression);
            var left = Convert(expression.Left);
            var right = Convert(expression.Right);
            EnsureComparable(left, expression);
            EnsureComparable(right, expression);
            return new CompareQueryNode(left, right, compare);
        }

        void EnsureComparable(QueryNode node, Expression expression)
        {
            if (node is StringCompareOrdinalQueryNode || node.Type == typeof(string) ||
                RelationQueryTypes.TryGetStorage(node.Type, out _, out _, out _))
                return;
            throw Unsupported(expression);
        }

        QueryNode ConvertConversion(UnaryExpression expression)
        {
            var operand = Convert(expression.Operand);
            if (operand.Type == expression.Type) return operand;
            if (!RelationQueryTypes.TryGetStorage(operand.Type, out var fromStorage, out _, out _) ||
                !RelationQueryTypes.TryGetStorage(expression.Type, out var toStorage, out _, out _) ||
                fromStorage != toStorage && !(RelationQueryTypes.IsNumeric(fromStorage) &&
                                              RelationQueryTypes.IsNumeric(toStorage)))
                throw Unsupported(expression);
            return new ConvertQueryNode(operand, expression.Type, expression.NodeType == ExpressionType.ConvertChecked);
        }

        QueryNode ConvertArithmetic(BinaryExpression expression, QueryArithmetic operation)
        {
            if (!RelationQueryTypes.TryGetStorage(expression.Type, out var storage, out _, out _) ||
                !RelationQueryTypes.IsArithmetic(storage) ||
                expression.Method != null && storage != typeof(decimal))
                throw Unsupported(expression);
            return new ArithmeticQueryNode(Convert(expression.Left), Convert(expression.Right), operation,
                expression.Type);
        }

        QueryNode ConvertNegate(UnaryExpression expression, QueryArithmetic operation)
        {
            if (!RelationQueryTypes.TryGetStorage(expression.Type, out var storage, out _, out _) ||
                !RelationQueryTypes.IsArithmetic(storage) ||
                expression.Method != null && storage != typeof(decimal))
                throw Unsupported(expression);
            return new ArithmeticQueryNode(Convert(expression.Operand), null, operation, expression.Type);
        }

        QueryNode ConvertMember(MemberExpression expression)
        {
            if (expression.Expression == _parameter)
            {
                if (expression.Member is not PropertyInfo propertyInfo)
                    throw new NotSupportedException(
                        $"Member '{expression.Member.Name}' is not supported in relation query.");
                return new PropQueryNode(_query, RelationQueryPropertyName.GetPersistentName(propertyInfo),
                    propertyInfo.PropertyType);
            }

            if (expression.Expression != null && Nullable.GetUnderlyingType(expression.Expression.Type) != null)
            {
                if (expression.Member.Name == nameof(Nullable<int>.HasValue))
                    return new HasValueQueryNode(Convert(expression.Expression));
                if (expression.Member.Name == nameof(Nullable<int>.Value))
                    return new NullableValueQueryNode(Convert(expression.Expression));
            }

            if (expression.Expression != null && expression.Member.DeclaringType == typeof(string) &&
                expression.Member.Name == nameof(string.Length))
                return new StringLengthQueryNode(Convert(expression.Expression));
            throw Unsupported(expression);
        }

        QueryNode ConvertCall(MethodCallExpression expression)
        {
            var method = expression.Method;
            var arguments = expression.Arguments;
            if (method.DeclaringType == typeof(string))
            {
                if (expression.Object != null)
                    return ConvertStringInstanceCall(expression);
                return ConvertStringStaticCall(expression);
            }

            if (method.Name == nameof(Enumerable.Contains))
            {
                // An optional equality comparer argument must be null, meaning the default equality.
                if (expression.Object == null && arguments.Count is 2 or 3 && !DependsOnParameter(arguments[0]) &&
                    (arguments.Count == 2 || !DependsOnParameter(arguments[2]) && EvaluateConstant(arguments[2]) == null))
                {
                    // Enumerable.Contains(source, value) or MemoryExtensions.Contains(span, value); C# 14 binds
                    // array.Contains(x) in expression trees to the span overload.
                    var source = method.DeclaringType == typeof(MemoryExtensions)
                        ? EvaluateConstant(UnwrapSpan(arguments[0]))
                        : EvaluateConstant(arguments[0]);
                    if (method.DeclaringType == typeof(Enumerable) || method.DeclaringType == typeof(MemoryExtensions))
                        return CreateInSet(source, Convert(arguments[1]), expression);
                }

                if (expression.Object != null && arguments.Count == 1 && !DependsOnParameter(expression.Object))
                    return CreateInSet(EvaluateConstant(expression.Object), Convert(arguments[0]), expression);
            }

            throw Unsupported(expression);
        }

        static Expression UnwrapSpan(Expression expression)
        {
            return expression switch
            {
                MethodCallExpression { Method.Name: "op_Implicit", Arguments.Count: 1 } call => call.Arguments[0],
                UnaryExpression { NodeType: ExpressionType.Convert } convert => convert.Operand,
                NewExpression { Arguments.Count: 1 } newExpression => newExpression.Arguments[0],
                _ => throw Unsupported(expression)
            };
        }

        QueryNode ConvertStringInstanceCall(MethodCallExpression expression)
        {
            var arguments = expression.Arguments;
            foreach (var argument in arguments)
                if (DependsOnParameter(argument)) throw Unsupported(expression);
            var operand = Convert(expression.Object!);
            var operation = expression.Method.Name switch
            {
                nameof(string.StartsWith) => QueryStringOp.StartsWith,
                nameof(string.EndsWith) => QueryStringOp.EndsWith,
                nameof(string.Contains) => QueryStringOp.Contains,
                nameof(string.Equals) => QueryStringOp.InstanceEquals,
                _ => throw Unsupported(expression)
            };
            var parameters = expression.Method.GetParameters();
            if (parameters.Length == 0 || parameters.Length > 2) throw Unsupported(expression);
            var argumentType = parameters[0].ParameterType;
            if (argumentType != typeof(string) && !(argumentType == typeof(char) && operation != QueryStringOp.InstanceEquals))
                throw Unsupported(expression);
            StringComparison comparison;
            if (parameters.Length == 2)
            {
                if (parameters[1].ParameterType != typeof(StringComparison)) throw Unsupported(expression);
                comparison = (StringComparison)EvaluateConstant(arguments[1])!;
            }
            else
            {
                // string.StartsWith(string) and EndsWith(string) compare with the current culture, the rest ordinally.
                comparison = argumentType == typeof(string) && operation is QueryStringOp.StartsWith or QueryStringOp.EndsWith
                    ? StringComparison.CurrentCulture
                    : StringComparison.Ordinal;
            }

            return new StringMethodQueryNode(operation, comparison, operand, EvaluateConstant(arguments[0]));
        }

        QueryNode ConvertStringStaticCall(MethodCallExpression expression)
        {
            var arguments = expression.Arguments;
            var parameters = expression.Method.GetParameters();
            switch (expression.Method.Name)
            {
                case nameof(string.IsNullOrEmpty) when arguments.Count == 1:
                    return new StringIsNullOrEmptyQueryNode(Convert(arguments[0]));
                case nameof(string.Equals) when arguments.Count >= 2 && parameters[0].ParameterType == typeof(string) &&
                                                parameters[1].ParameterType == typeof(string):
                {
                    var comparison = StringComparison.Ordinal;
                    if (arguments.Count == 3)
                    {
                        if (DependsOnParameter(arguments[2])) throw Unsupported(expression);
                        comparison = (StringComparison)EvaluateConstant(arguments[2])!;
                    }

                    var (operand, constant, _) = SplitStringArguments(arguments[0], arguments[1], expression);
                    return new StringMethodQueryNode(QueryStringOp.Equals, comparison, operand, constant);
                }
                case nameof(string.CompareOrdinal) when arguments.Count == 2:
                case nameof(string.Compare) when arguments.Count == 3 && parameters[2].ParameterType == typeof(StringComparison) &&
                                                 !DependsOnParameter(arguments[2]) &&
                                                 (StringComparison)EvaluateConstant(arguments[2])! == StringComparison.Ordinal:
                {
                    var (operand, constant, reversed) = SplitStringArguments(arguments[0], arguments[1], expression);
                    return new StringCompareOrdinalQueryNode(operand, (string?)constant, reversed);
                }
                default:
                    throw Unsupported(expression);
            }
        }

        (QueryNode Operand, object? Constant, bool Reversed) SplitStringArguments(Expression first, Expression second,
            Expression expression)
        {
            var firstDepends = DependsOnParameter(first);
            if (firstDepends == DependsOnParameter(second)) throw Unsupported(expression);
            return firstDepends
                ? (Convert(first), EvaluateConstant(second), false)
                : (Convert(second), EvaluateConstant(first), true);
        }

        static QueryNode CreateInSet(object? source, QueryNode operand, Expression expression)
        {
            if (source is not IEnumerable items) throw Unsupported(expression);
            if (operand.Type == typeof(string))
            {
                if (source is HashSet<string> stringSet && !ReferenceEquals(stringSet.Comparer, StringComparer.Ordinal) &&
                    !ReferenceEquals(stringSet.Comparer, EqualityComparer<string>.Default))
                    throw Unsupported(expression);
                var values = new List<string?>();
                foreach (var item in items) values.Add((string?)item);
                return new InSetQueryNode(operand, new QueryStringSet(values));
            }

            if (!RelationQueryTypes.TryGetStorage(operand.Type, out var storage, out _, out var nullable) || nullable)
                throw Unsupported(expression);
            // Only the default equality of the element type is supported, which a HashSet may override.
            if (source.GetType().IsGenericType && source.GetType().GetGenericTypeDefinition() == typeof(HashSet<>) &&
                !RelationQueryTypes.HasDefaultComparer(source, storage))
                throw Unsupported(expression);
            return new InSetQueryNode(operand, RelationQueryTypes.CreateSet(storage, items));
        }

        bool DependsOnParameter(Expression expression)
        {
            var visitor = new ParameterFinder(_parameter);
            visitor.Visit(expression);
            return visitor.Found;
        }

        static NotSupportedException Unsupported(Expression expression)
        {
            return new($"Expression '{expression}' is not supported in relation query.");
        }
    }

    internal static object? EvaluateConstant(Expression expression)
    {
        RelationQueryParameterScope.BakeIfParameter(expression);
        return expression switch
        {
            ConstantExpression constant => constant.Value,
            MemberExpression member => EvaluateMember(member),
            UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked, Method: null } convert =>
                ConvertConstant(EvaluateConstant(convert.Operand), convert.Type),
            BinaryExpression { Method: null } binary when IsPrimitiveArithmetic(binary) => EvaluateArithmetic(binary),
            // Anything else independent of the row is evaluated once, by the expression interpreter.
            _ => Expression.Lambda<Func<object?>>(Expression.Convert(expression, typeof(object)))
                .Compile(preferInterpretation: true)()
        };
    }

    // Arithmetic on two values of the same primitive type, as captured values like id + 1 often are, without starting
    // the expression interpreter.
    static bool IsPrimitiveArithmetic(BinaryExpression binary)
    {
        var type = binary.Type;
        return binary.NodeType is ExpressionType.Add or ExpressionType.AddChecked or ExpressionType.Subtract
                   or ExpressionType.SubtractChecked or ExpressionType.Multiply or ExpressionType.MultiplyChecked
                   or ExpressionType.Divide or ExpressionType.Modulo &&
               binary.Left.Type == type && binary.Right.Type == type &&
               (type == typeof(int) || type == typeof(long) || type == typeof(uint) || type == typeof(ulong) ||
                type == typeof(double) || type == typeof(float));
    }

    static object EvaluateArithmetic(BinaryExpression binary)
    {
        var left = EvaluateConstant(binary.Left)!;
        var right = EvaluateConstant(binary.Right)!;
        var operation = binary.NodeType;
        // Each result is boxed as its own type; a switch expression would convert them all to double.
        return left switch
        {
            int l => (object)Compute(l, (int)right, operation),
            long l => Compute(l, (long)right, operation),
            uint l => Compute(l, (uint)right, operation),
            ulong l => Compute(l, (ulong)right, operation),
            double l => Compute(l, (double)right, operation),
            _ => Compute((float)left, (float)right, operation)
        };
    }

    static T Compute<T>(T left, T right, ExpressionType operation) where T : INumber<T> => operation switch
    {
        ExpressionType.Add => left + right,
        ExpressionType.AddChecked => checked(left + right),
        ExpressionType.Subtract => left - right,
        ExpressionType.SubtractChecked => checked(left - right),
        ExpressionType.Multiply => left * right,
        ExpressionType.MultiplyChecked => checked(left * right),
        ExpressionType.Divide => left / right,
        _ => left % right
    };

    static object? EvaluateMember(MemberExpression expression)
    {
        var instance = expression.Expression == null ? null : EvaluateConstant(expression.Expression);
        return expression.Member switch
        {
            FieldInfo fieldInfo => fieldInfo.GetValue(instance),
            PropertyInfo propertyInfo => propertyInfo.GetValue(instance),
            _ => throw new NotSupportedException($"Member '{expression.Member.Name}' is not supported in relation query.")
        };
    }

    internal static object? ConvertConstant(object? value, Type type)
    {
        if (value == null) return null;
        var targetType = Nullable.GetUnderlyingType(type) ?? type;
        if (targetType.IsInstanceOfType(value)) return value;
        if (targetType.IsEnum) return Enum.ToObject(targetType, value);
        return System.Convert.ChangeType(value, targetType);
    }

    class ParameterFinder : ExpressionVisitor
    {
        readonly ParameterExpression _parameter;

        public ParameterFinder(ParameterExpression parameter)
        {
            _parameter = parameter;
        }

        public bool Found { get; private set; }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _parameter) Found = true;
            return node;
        }
    }
}

/// Supported value types of relation query fields and their storage in the interpreter frame.
static class RelationQueryTypes
{
    /// Storage is the non-nullable type with enums replaced by their underlying type.
    public static bool TryGetStorage(Type type, out Type storage, out QueryValueType valueType, out bool nullable)
    {
        nullable = false;
        var t = type;
        if (Nullable.GetUnderlyingType(t) is { } underlying)
        {
            nullable = true;
            t = underlying;
        }

        if (t.IsEnum) t = Enum.GetUnderlyingType(t);
        storage = t;
        valueType = default;
        if (t == typeof(bool) || t == typeof(byte)) valueType = QueryValueType.UInt8;
        else if (t == typeof(sbyte)) valueType = QueryValueType.Int8;
        else if (t == typeof(ushort)) valueType = QueryValueType.UInt16;
        else if (t == typeof(short)) valueType = QueryValueType.Int16;
        else if (t == typeof(uint)) valueType = QueryValueType.UInt32;
        else if (t == typeof(int)) valueType = QueryValueType.Int32;
        else if (t == typeof(ulong)) valueType = QueryValueType.UInt64;
        else if (t == typeof(long)) valueType = QueryValueType.Int64;
        else if (t == typeof(Half)) valueType = QueryValueType.Half;
        else if (t == typeof(float)) valueType = QueryValueType.Single;
        else if (t == typeof(double)) valueType = QueryValueType.Double;
        else if (t == typeof(DateTime)) valueType = QueryValueType.DateTime;
        else if (t == typeof(Guid)) valueType = QueryValueType.Guid;
        else if (t == typeof(decimal)) valueType = QueryValueType.Decimal;
        else if (t == typeof(TimeSpan)) valueType = QueryValueType.TimeSpan;
        else if (t == typeof(DateTimeOffset)) valueType = QueryValueType.DateTimeOffset;
        else return false;
        return true;
    }

    public static bool IsNumeric(Type storage)
    {
        return IsInteger(storage) || storage == typeof(Half) || storage == typeof(float) ||
               storage == typeof(double) || storage == typeof(decimal);
    }

    public static bool IsInteger(Type storage)
    {
        return storage == typeof(byte) || storage == typeof(sbyte) || storage == typeof(ushort) ||
               storage == typeof(short) || storage == typeof(uint) || storage == typeof(int) ||
               storage == typeof(ulong) || storage == typeof(long);
    }

    /// Types C# does arithmetic in after promotion.
    public static bool IsArithmetic(Type storage)
    {
        return storage == typeof(int) || storage == typeof(uint) || storage == typeof(long) ||
               storage == typeof(ulong) || storage == typeof(float) || storage == typeof(double) ||
               storage == typeof(decimal);
    }

    /// Size in bytes of a value of the storage type.
    public static int ValueSize(Type storage)
    {
        if (storage == typeof(bool) || storage == typeof(byte) || storage == typeof(sbyte)) return 1;
        if (storage == typeof(ushort) || storage == typeof(short) || storage == typeof(Half)) return 2;
        if (storage == typeof(uint) || storage == typeof(int) || storage == typeof(float)) return 4;
        if (storage == typeof(Guid) || storage == typeof(decimal) || storage == typeof(DateTimeOffset)) return 16;
        return 8;
    }

    public static int SlotSize(Type storage)
    {
        return storage == typeof(Guid) || storage == typeof(decimal) || storage == typeof(DateTimeOffset) ? 16 : 8;
    }

    public static object? DefaultValue(Type type)
    {
        if (!type.IsValueType || Nullable.GetUnderlyingType(type) != null) return null;
        if (type.IsEnum) return Enum.ToObject(type, 0);
        if (type == typeof(bool)) return false;
        if (type == typeof(byte)) return (byte)0;
        if (type == typeof(sbyte)) return (sbyte)0;
        if (type == typeof(ushort)) return (ushort)0;
        if (type == typeof(short)) return (short)0;
        if (type == typeof(uint)) return 0u;
        if (type == typeof(int)) return 0;
        if (type == typeof(ulong)) return 0ul;
        if (type == typeof(long)) return 0L;
        if (type == typeof(Half)) return Half.Zero;
        if (type == typeof(float)) return 0f;
        if (type == typeof(double)) return 0d;
        if (type == typeof(DateTime)) return default(DateTime);
        if (type == typeof(Guid)) return Guid.Empty;
        if (type == typeof(decimal)) return 0m;
        if (type == typeof(TimeSpan)) return TimeSpan.Zero;
        if (type == typeof(DateTimeOffset)) return default(DateTimeOffset);
        throw new NotSupportedException($"Relation query does not support fields of type {type.ToSimpleName()} yet.");
    }

    /// Converts a constant (possibly a boxed enum) to its storage type.
    public static object ToStorage(object value, Type storage)
    {
        return value.GetType() == storage ? value : System.Convert.ChangeType(value, storage);
    }

    /// Writes a value as its storage type into frame bytes, like programs keep values.
    public static void WriteStorage(Span<byte> target, Type storage, object value)
    {
        value = ToStorage(value, storage);
        if (storage == typeof(bool)) MemoryMarshal.Write(target, (bool)value);
        else if (storage == typeof(byte)) MemoryMarshal.Write(target, (byte)value);
        else if (storage == typeof(sbyte)) MemoryMarshal.Write(target, (sbyte)value);
        else if (storage == typeof(ushort)) MemoryMarshal.Write(target, (ushort)value);
        else if (storage == typeof(short)) MemoryMarshal.Write(target, (short)value);
        else if (storage == typeof(uint)) MemoryMarshal.Write(target, (uint)value);
        else if (storage == typeof(int)) MemoryMarshal.Write(target, (int)value);
        else if (storage == typeof(ulong)) MemoryMarshal.Write(target, (ulong)value);
        else if (storage == typeof(long)) MemoryMarshal.Write(target, (long)value);
        else if (storage == typeof(Half)) MemoryMarshal.Write(target, (Half)value);
        else if (storage == typeof(float)) MemoryMarshal.Write(target, (float)value);
        else if (storage == typeof(double)) MemoryMarshal.Write(target, (double)value);
        else if (storage == typeof(DateTime)) MemoryMarshal.Write(target, (DateTime)value);
        else if (storage == typeof(Guid)) MemoryMarshal.Write(target, (Guid)value);
        else if (storage == typeof(decimal)) MemoryMarshal.Write(target, (decimal)value);
        else if (storage == typeof(TimeSpan)) MemoryMarshal.Write(target, (TimeSpan)value);
        else if (storage == typeof(DateTimeOffset)) MemoryMarshal.Write(target, (DateTimeOffset)value);
        else throw new NotSupportedException($"Relation query cannot store {storage.ToSimpleName()} values.");
    }

    public static object CreateSet(Type storage, IEnumerable items)
    {
        if (storage == typeof(bool)) return Create<bool>(items);
        if (storage == typeof(byte)) return Create<byte>(items);
        if (storage == typeof(sbyte)) return Create<sbyte>(items);
        if (storage == typeof(ushort)) return Create<ushort>(items);
        if (storage == typeof(short)) return Create<short>(items);
        if (storage == typeof(uint)) return Create<uint>(items);
        if (storage == typeof(int)) return Create<int>(items);
        if (storage == typeof(ulong)) return Create<ulong>(items);
        if (storage == typeof(long)) return Create<long>(items);
        if (storage == typeof(Half)) return Create<Half>(items);
        if (storage == typeof(float)) return Create<float>(items);
        if (storage == typeof(double)) return Create<double>(items);
        if (storage == typeof(DateTime)) return Create<DateTime>(items);
        if (storage == typeof(Guid)) return Create<Guid>(items);
        if (storage == typeof(decimal)) return Create<decimal>(items);
        if (storage == typeof(TimeSpan)) return Create<TimeSpan>(items);
        if (storage == typeof(DateTimeOffset)) return Create<DateTimeOffset>(items);
        throw new NotSupportedException($"Relation query does not support sets of {storage.ToSimpleName()} yet.");

        static object Create<T>(IEnumerable items) where T : struct
        {
            var values = new List<T>();
            foreach (var item in items) values.Add((T)ToStorage(item, typeof(T)));
            return values.ToFrozenSet();
        }
    }

    public static bool HasDefaultComparer(object set, Type storage)
    {
        return set switch
        {
            HashSet<bool> s => ReferenceEquals(s.Comparer, EqualityComparer<bool>.Default),
            HashSet<byte> s => ReferenceEquals(s.Comparer, EqualityComparer<byte>.Default),
            HashSet<sbyte> s => ReferenceEquals(s.Comparer, EqualityComparer<sbyte>.Default),
            HashSet<ushort> s => ReferenceEquals(s.Comparer, EqualityComparer<ushort>.Default),
            HashSet<short> s => ReferenceEquals(s.Comparer, EqualityComparer<short>.Default),
            HashSet<uint> s => ReferenceEquals(s.Comparer, EqualityComparer<uint>.Default),
            HashSet<int> s => ReferenceEquals(s.Comparer, EqualityComparer<int>.Default),
            HashSet<ulong> s => ReferenceEquals(s.Comparer, EqualityComparer<ulong>.Default),
            HashSet<long> s => ReferenceEquals(s.Comparer, EqualityComparer<long>.Default),
            HashSet<double> s => ReferenceEquals(s.Comparer, EqualityComparer<double>.Default),
            HashSet<float> s => ReferenceEquals(s.Comparer, EqualityComparer<float>.Default),
            HashSet<DateTime> s => ReferenceEquals(s.Comparer, EqualityComparer<DateTime>.Default),
            HashSet<Guid> s => ReferenceEquals(s.Comparer, EqualityComparer<Guid>.Default),
            HashSet<decimal> s => ReferenceEquals(s.Comparer, EqualityComparer<decimal>.Default),
            HashSet<TimeSpan> s => ReferenceEquals(s.Comparer, EqualityComparer<TimeSpan>.Default),
            HashSet<DateTimeOffset> s => ReferenceEquals(s.Comparer, EqualityComparer<DateTimeOffset>.Default),
            // A HashSet of an enum cannot be inspected without reflection, so its comparer is not trusted.
            _ => false
        };
    }
}

/// Compiled relation query: key ranges to scan plus interpreter programs evaluated on serialized key and value bytes.
sealed class RelationQueryPlan
{
    public RelationQueryPlan(RelationQueryRange[] ranges, RelationQueryProgram? keyProgram,
        RelationQueryProgram?[]? valuePrograms, int frameSize, int objectCount, RelationQueryResult? result = null,
        RelationQuerySecondaryKey? secondaryKey = null)
    {
        SortKeys = [];
        FixedPrimaryKeyFields = [];
        Ranges = ranges;
        KeyProgram = keyProgram;
        ValuePrograms = valuePrograms;
        FrameSize = frameSize;
        ObjectCount = objectCount;
        Result = result;
        SecondaryKey = secondaryKey;
    }

    /// Ascending, disjoint ranges of primary keys, or of secondary index keys when SecondaryKey is set. No ranges
    /// means no row can match. When RangeBuilder is set, these are the ranges of the values the plan was compiled with.
    public RelationQueryRange[] Ranges { get; }

    /// Builds Ranges from the parameter values of each execution; null when they do not depend on parameters.
    public RelationQueryRangeBuilder? RangeBuilder { get; internal set; }

    /// Frame slots the scanner fills with parameter values before scanning.
    public (int Index, ushort Slot, Type Storage)[] Parameters { get; internal set; } = [];

    /// False when the plan depends on the transaction it was compiled in, so it cannot be reused.
    public bool Cacheable { get; internal set; } = true;

    /// Tests conjuncts using only primary key fields before the value is read. Null when there are none.
    public RelationQueryProgram? KeyProgram { get; }

    /// Indexed by persisted value version, tests the remaining conjuncts. Null when there are none. A null item is an
    /// unknown version whose rows never match.
    public RelationQueryProgram?[]? ValuePrograms { get; }

    /// Size in bytes of the frame shared by all programs; a multiple of 8.
    public int FrameSize { get; internal set; }

    public int ObjectCount { get; internal set; }

    /// Frame byte the scanner sets to 1, the HasValue of results which are non-nullable fields; -1 when unused.
    public int OneSlot { get; internal set; } = -1;

    /// Where the last program of a matching row stores the selector value; null without a selector.
    public RelationQueryResult? Result { get; }

    public RelationQuerySecondaryKey? SecondaryKey { get; }

    /// Whether primary rows are tested by programs.
    public bool HasPrograms => KeyProgram != null || ValuePrograms != null;

    /// Sort key values of a matching row, computed by its last program.
    public RelationQueryResult[] SortKeys { get; internal set; }

    /// Values of Select arguments of a row, computed by the projection programs.
    public RelationQueryResult[] Projection { get; internal set; } = [];

    /// Computes Projection from the primary key alone. Null when value programs are needed.
    public RelationQueryProgram? ProjectionKeyProgram { get; internal set; }

    /// Computes Projection, indexed by value version.
    public RelationQueryProgram?[]? ProjectionValuePrograms { get; internal set; }

    public bool ProjectionCanThrow { get; internal set; }

    /// Projection is computed by the scanning programs of each matching row.
    public bool ProjectionIsEager { get; internal set; }

    /// Primary key fields every matching row has the same value of.
    public HashSet<string> FixedPrimaryKeyFields { get; internal set; }

    /// Whether testing a row can throw, so evaluating fewer rows than LINQ would be observable.
    public bool CanThrow { get; internal set; }
}

/// Scanning a secondary index: index keys in the plan ranges are tested by IndexProgram, then their primary rows by
/// the plan programs, in primary key order when SortCandidates is set.
sealed class RelationQuerySecondaryKey
{
    public RelationQuerySecondaryKey(RelationQueryProgram? indexProgram, bool sortCandidates, bool covering,
        RelationInfo.SecondaryKeyValueToPKLoader toPrimaryKey, byte[] primaryKeyPrefix)
    {
        IndexProgram = indexProgram;
        SortCandidates = sortCandidates;
        Covering = covering;
        ToPrimaryKey = toPrimaryKey;
        PrimaryKeyPrefix = primaryKeyPrefix;
    }

    public RelationQueryProgram? IndexProgram { get; }
    public bool SortCandidates { get; }

    /// The index key holds everything needed, so primary rows are never read.
    public bool Covering { get; }

    public RelationInfo.SecondaryKeyValueToPKLoader ToPrimaryKey { get; }
    public byte[] PrimaryKeyPrefix { get; }
}

/// Frame location of a selector value computed for each matching row.
sealed class RelationQueryResult
{
    public RelationQueryResult(Type type, Type storageType, QueryValueType valueType, ushort slot, ushort hasValueSlot,
        bool isField = false)
    {
        Type = type;
        StorageType = storageType;
        ValueType = valueType;
        Slot = slot;
        HasValueSlot = hasValueSlot;
        IsField = isField;
    }

    /// The slots are those of the selected property, which programs read without copying.
    public bool IsField { get; }

    /// A string property, which its program stores into an object slot.
    public RelationQueryResult(ushort objectSlot)
    {
        Type = typeof(string);
        StorageType = typeof(string);
        IsString = true;
        Slot = objectSlot;
    }

    public bool IsString { get; }

    /// Reads the value of the current row; enums are returned as their underlying type.
    public object? Read(ReadOnlySpan<byte> frame, object?[] objects)
    {
        if (IsString) return objects[Slot];

        if (frame[HasValueSlot] == 0) return null;
        var value = frame[Slot..];
        if (StorageType == typeof(bool)) return MemoryMarshal.Read<bool>(value);
        if (StorageType == typeof(byte)) return MemoryMarshal.Read<byte>(value);
        if (StorageType == typeof(sbyte)) return MemoryMarshal.Read<sbyte>(value);
        if (StorageType == typeof(ushort)) return MemoryMarshal.Read<ushort>(value);
        if (StorageType == typeof(short)) return MemoryMarshal.Read<short>(value);
        if (StorageType == typeof(uint)) return MemoryMarshal.Read<uint>(value);
        if (StorageType == typeof(int)) return MemoryMarshal.Read<int>(value);
        if (StorageType == typeof(ulong)) return MemoryMarshal.Read<ulong>(value);
        if (StorageType == typeof(long)) return MemoryMarshal.Read<long>(value);
        if (StorageType == typeof(Half)) return MemoryMarshal.Read<Half>(value);
        if (StorageType == typeof(float)) return MemoryMarshal.Read<float>(value);
        if (StorageType == typeof(double)) return MemoryMarshal.Read<double>(value);
        if (StorageType == typeof(DateTime)) return MemoryMarshal.Read<DateTime>(value);
        if (StorageType == typeof(Guid)) return MemoryMarshal.Read<Guid>(value);
        if (StorageType == typeof(decimal)) return MemoryMarshal.Read<decimal>(value);
        if (StorageType == typeof(TimeSpan)) return MemoryMarshal.Read<TimeSpan>(value);
        if (StorageType == typeof(DateTimeOffset)) return MemoryMarshal.Read<DateTimeOffset>(value);
        throw new NotSupportedException($"Relation query cannot read {StorageType.ToSimpleName()} results.");
    }

    /// Selector type, possibly nullable or an enum.
    public Type Type { get; }

    public Type StorageType { get; }
    public QueryValueType ValueType { get; }
    public ushort Slot { get; }
    public ushort HasValueSlot { get; }
}

sealed class RelationQueryProgram
{
    public RelationQueryProgram(InterpreterProgram? program, int keyStart, bool constantResult)
    {
        Program = program;
        KeyStart = keyStart;
        ConstantResult = constantResult;
    }

    /// Null when the conjuncts folded to a constant; ConstantResult is the result then.
    public InterpreterProgram? Program { get; }

    /// Byte offset in the key where the program starts reading primary key fields.
    public int KeyStart { get; }

    /// The program starts after the prefix common to all ranges, whose length differs between executions.
    public bool KeyStartFromRanges { get; init; }

    public bool ConstantResult { get; }
}

sealed class RelationQueryField
{
    public RelationQueryField(string name, Type type)
    {
        Name = name;
        Type = type;
        IsString = type == typeof(string);
        if (!IsString) RelationQueryTypes.TryGetStorage(type, out StorageType, out ValueType, out IsNullable);
        else StorageType = typeof(string);
    }

    public readonly string Name;

    /// Property type.
    public readonly Type Type;

    public readonly bool IsString;
    public readonly bool IsNullable;
    public readonly Type StorageType;
    public readonly QueryValueType ValueType;

    /// Frame offset of the value, or of the captured encoding (pointer and length) for strings.
    public ushort Slot;

    /// Frame offset of the HasValue byte of nullable values.
    public ushort HasValueSlot;

    /// Object slot with the reused char[] of decoded strings.
    public ushort CharsObject;

    /// Frame offset of the decoded string length (int, -1 for null).
    public ushort LengthSlot;
}

enum QueryHasValue
{
    Always,
    Never,
    Frame
}

readonly struct QueryOperand
{
    public QueryOperand(QueryOperandKind kind, uint offset, QueryValueType valueType, Type storageType,
        QueryHasValue hasValue, ushort hasValueSlot)
    {
        Kind = kind;
        Offset = offset;
        ValueType = valueType;
        StorageType = storageType;
        HasValue = hasValue;
        HasValueSlot = hasValueSlot;
    }

    public QueryOperandKind Kind { get; }
    public uint Offset { get; }
    public QueryValueType ValueType { get; }
    public Type StorageType { get; }
    public QueryHasValue HasValue { get; }
    public ushort HasValueSlot { get; }

    public QueryOperand WithHasValue(QueryHasValue hasValue) =>
        new(Kind, Offset, ValueType, StorageType, hasValue, HasValueSlot);

    public QueryOperand WithType(QueryValueType valueType, Type storageType) =>
        new(Kind, Offset, valueType, storageType, HasValue, HasValueSlot);
}

sealed class RelationQueryCompiler
{
    readonly RelationInfo _relationInfo;
    readonly Type _itemType;
    readonly ReadOnlyMemory<TableFieldInfo> _primaryKeyFields;
    readonly Dictionary<string, RelationQueryField> _fields = new();
    int _frameSize;
    int _objectCount;
    ushort _frameTemporary;
    ushort _objectTemporary;

    readonly List<(QueryNode Node, RelationQueryResult Result)> _selectors = [];

    // State of the program being built.
    InterpreterBuilder _builder = null!;
    readonly Dictionary<string, QueryStringEncoding> _encodings = new();

    public RelationQueryCompiler(RelationInfo relationInfo, Type itemType)
    {
        _relationInfo = relationInfo;
        _itemType = itemType;
        _primaryKeyFields = relationInfo.ClientRelationVersionInfo.PrimaryKeyFields;
    }

    readonly record struct Conjunct(QueryNode Node, int Index, bool Throws);

    /// The optional selector is computed for every matching row into RelationQueryPlan.Result. When the result does
    /// not depend on row order (Count, Any, All, Min, Max), a secondary index may be scanned in its own order.
    /// Sort keys are computed for every matching row into RelationQueryPlan.SortKeys.
    public RelationQueryPlan Compile(IInternalObjectDBTransaction transaction, QueryNode ast,
        QueryNode? selector = null, bool orderSensitive = true, IReadOnlyList<QueryNode>? sortKeys = null,
        IReadOnlyList<QueryNode>? projection = null, bool eagerProjection = false)
    {
        var predicate = ExtractPredicate(ast);
        if (predicate != null) CollectFields(predicate);
        RelationQueryResult? result = null;
        if (selector != null)
        {
            result = AddSelector(selector);
        }

        var sortKeyResults = new RelationQueryResult[sortKeys?.Count ?? 0];
        for (var i = 0; i < sortKeyResults.Length; i++)
        {
            if (CanThrow(sortKeys![i]))
                throw new NotSupportedException("Relation query does not support ordering keys which can throw.");
            sortKeyResults[i] = AddSelector(sortKeys[i]);
        }

        // An eager projection is computed by the scanning programs of every matching row, which saves reading returned
        // rows again; it is only used when it cannot throw and every matching row is returned.
        RelationQueryResult[]? eagerResults = null;
        if (eagerProjection && projection is { Count: > 0 })
        {
            eagerResults = new RelationQueryResult[projection.Count];
            for (var i = 0; i < eagerResults.Length; i++) eagerResults[i] = AddSelector(projection[i]);
        }

        var plan = CompilePlan(transaction, predicate, result, orderSensitive);
        plan.SortKeys = sortKeyResults;
        if (eagerResults != null)
        {
            plan.Projection = eagerResults;
            plan.ProjectionIsEager = true;
        }
        else if (projection is { Count: > 0 })
        {
            CompileProjection(plan, projection);
        }
        plan.FrameSize = _frameSize;
        plan.ObjectCount = _objectCount;
        plan.OneSlot = _oneSlot ?? -1;
        plan.Parameters = _parameterSlots.ToArray();
        plan.Cacheable = !_uncacheable;
        return plan;
    }

    // Select values get their own programs, run only for rows actually returned, like LINQ calls a selector.
    void CompileProjection(RelationQueryPlan plan, IReadOnlyList<QueryNode> projection)
    {
        _selectors.Clear();
        var results = new RelationQueryResult[projection.Count];
        for (var i = 0; i < results.Length; i++) results[i] = AddSelector(projection[i]);
        var primaryKeyFields = _primaryKeyFields.ToArray();
        plan.Projection = results;
        plan.ProjectionCanThrow = _selectors.Exists(s => CanThrow(s.Node));
        if (_selectors.TrueForAll(s => UsesOnlyFields(s.Node, primaryKeyFields)))
        {
            plan.ProjectionKeyProgram = BuildProgram([], null, _relationInfo.Prefix.Length, 0, _selectors,
                primaryKeyFields, _relationInfo.Prefix.Length);
            return;
        }

        var maxVersion = 0u;
        foreach (var (version, _) in _relationInfo.RelationVersions) maxVersion = Math.Max(maxVersion, version);
        var programs = new RelationQueryProgram?[maxVersion + 1];
        foreach (var (version, info) in _relationInfo.RelationVersions)
            programs[version] = BuildProgram([], info.Fields, _relationInfo.Prefix.Length, 0, _selectors,
                primaryKeyFields, _relationInfo.Prefix.Length);
        plan.ProjectionValuePrograms = programs;
    }

    RelationQueryResult AddSelector(QueryNode selector)
    {
        CollectFields(selector);
        RelationQueryResult result;
        if (selector is PropQueryNode { Type: var type } && type == typeof(string))
        {
            result = new(AllocObjectSlot());
        }
        else if (selector is PropQueryNode prop)
        {
            // A property is read into its field slot, which then holds the result; no program copies it.
            var field = _fields[prop.Name];
            result = new(selector.Type, field.StorageType, field.ValueType, field.Slot,
                field.IsNullable ? field.HasValueSlot : _oneSlot ??= AllocFrame(8), true);
        }
        else
        {
            if (!RelationQueryTypes.TryGetStorage(selector.Type, out var storage, out var valueType, out _))
                throw new NotSupportedException(
                    $"Relation query does not support selecting {selector.Type.ToSimpleName()} values yet.");
            var size = RelationQueryTypes.SlotSize(storage);
            var slot = AllocFrame(size + 8);
            result = new(selector.Type, storage, valueType, slot, (ushort)(slot + size));
        }

        _selectors.Add((selector, result));
        return result;
    }

    RelationQueryPlan CompilePlan(IInternalObjectDBTransaction transaction, QueryNode? predicate,
        RelationQueryResult? result, bool orderSensitive)
    {
        _frameTemporary = AllocFrame(16);
        _objectTemporary = AllocObjectSlot();

        var nodes = new List<QueryNode>();
        if (predicate != null) FlattenConjuncts(Simplify(predicate), nodes);
        var conjuncts = new List<Conjunct>();
        var seenThrowing = false;
        foreach (var node in nodes)
        {
            // A false conjunct makes the query empty unless an earlier conjunct could throw first.
            if (IsFalse(node) && !seenThrowing) return new([], null, null, _frameSize, _objectCount, result);
            if (IsTrue(node)) continue;
            var throws = CanThrow(node);
            conjuncts.Add(new(node, conjuncts.Count, throws));
            seenThrowing |= throws;
        }

        // Equalities, IN lists and bounds on leading key fields become key ranges and are removed; every key in the
        // ranges satisfies them. The primary key wins ties with secondary indexes.
        var path = AnalyzeAccessPath(transaction, conjuncts, _primaryKeyFields.ToArray(), _relationInfo.Prefix, -1);
        if (!_relationInfo.HasInKeyValue)
        {
            foreach (var (index, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var prefix = new byte[_relationInfo.PrefixSecondary.Length + 1];
                _relationInfo.PrefixSecondary.CopyTo(prefix, 0);
                prefix[^1] = (byte)index;
                var candidate = AnalyzeAccessPath(transaction, conjuncts,
                    _relationInfo.ClientRelationVersionInfo.GetSecondaryKeyFields(index).ToArray(), prefix, (int)index);
                if (candidate.Score > path.Score) path = candidate;
            }
        }

        var canThrow = seenThrowing || _selectors.Exists(s => CanThrow(s.Node));
        var fixedFields = new HashSet<string>(path.FixedFields);
        conjuncts.RemoveAll(c => path.Used.Contains(c.Index));
        var ranges = path.Ranges;
        var rangeBuilder = path.Builder.HasParameters ? path.Builder : null;
        // Programs start reading keys after the common prefix, whose length then comes from each execution's ranges.
        var dynamicPrefix = rangeBuilder != null && path.CommonFieldCount > 0;
        if (path.SecondaryKeyIndex < 0)
        {
            var (keyProgram, valuePrograms) = BuildPrimaryPrograms(conjuncts, path.CommonPrefixLength,
                path.CommonFieldCount, dynamicPrefix);
            return new(ranges, keyProgram, valuePrograms, _frameSize, _objectCount, result)
                { FixedPrimaryKeyFields = fixedFields, CanThrow = canThrow, RangeBuilder = rangeBuilder };
        }

        // Index conjuncts are tested on index keys; they must not throw, because index order is not row order.
        var indexConjuncts = new List<Conjunct>();
        var primaryConjuncts = new List<Conjunct>();
        var barrier = false;
        foreach (var conjunct in conjuncts)
        {
            barrier |= conjunct.Throws;
            if (!barrier && UsesOnlyFields(conjunct.Node, path.KeyFields)) indexConjuncts.Add(conjunct);
            else primaryConjuncts.Add(conjunct);
        }

        var anyThrows = barrier || _selectors.Exists(s => CanThrow(s.Node));
        var covering = !orderSensitive && primaryConjuncts.Count == 0 && !anyThrows &&
                       _selectors.TrueForAll(s => UsesOnlyFields(s.Node, path.KeyFields));
        var indexProgram = indexConjuncts.Count > 0 || covering && _selectors.Count > 0
            ? BuildProgram(indexConjuncts, null, path.CommonPrefixLength, path.CommonFieldCount,
                covering ? _selectors : [], path.KeyFields, path.Prefix.Length, dynamicPrefix)
            : null;
        RelationQueryProgram? primaryKeyProgram = null;
        RelationQueryProgram?[]? primaryValuePrograms = null;
        if (!covering)
            (primaryKeyProgram, primaryValuePrograms) = BuildPrimaryPrograms(primaryConjuncts,
                _relationInfo.Prefix.Length, 0);
        var secondaryKey = new RelationQuerySecondaryKey(indexProgram, !covering && (orderSensitive || anyThrows),
            covering, _relationInfo.GetSKKeyValueToPKMerger((uint)path.SecondaryKeyIndex), _relationInfo.Prefix);
        return new(ranges, primaryKeyProgram, primaryValuePrograms, _frameSize, _objectCount, result, secondaryKey)
            { FixedPrimaryKeyFields = fixedFields, CanThrow = canThrow, RangeBuilder = rangeBuilder };
    }

    (RelationQueryProgram? KeyProgram, RelationQueryProgram?[]? ValuePrograms) BuildPrimaryPrograms(
        List<Conjunct> conjuncts, int prefixLength, int prefixFieldCount, bool dynamicPrefix = false)
    {
        // Conjuncts are evaluated in a different order than written, so a throwing conjunct must still see every
        // earlier conjunct evaluated first, and nothing after it may move before it.
        var primaryKeyFields = _primaryKeyFields.ToArray();
        var keyConjuncts = new List<Conjunct>();
        var valueConjuncts = new List<Conjunct>();
        var barrier = false;
        var allPreviousInKey = true;
        foreach (var conjunct in conjuncts)
        {
            if (UsesOnlyFields(conjunct.Node, primaryKeyFields) && !barrier && (!conjunct.Throws || allPreviousInKey))
            {
                keyConjuncts.Add(conjunct);
            }
            else
            {
                valueConjuncts.Add(conjunct);
                allPreviousInKey = false;
            }

            barrier |= conjunct.Throws;
        }

        // Selectors run in the last program of a row: the value programs when they exist or a selector needs value
        // fields, otherwise the key program.
        var hasValueStage = valueConjuncts.Count > 0 ||
                            _selectors.Exists(s => !UsesOnlyFields(s.Node, primaryKeyFields));
        var keyProgram = keyConjuncts.Count == 0 && (_selectors.Count == 0 || hasValueStage)
            ? null
            : BuildProgram(keyConjuncts, null, prefixLength, prefixFieldCount, hasValueStage ? [] : _selectors,
                primaryKeyFields, _relationInfo.Prefix.Length, dynamicPrefix);
        RelationQueryProgram?[]? valuePrograms = null;
        if (hasValueStage)
        {
            var maxVersion = 0u;
            foreach (var (version, _) in _relationInfo.RelationVersions) maxVersion = Math.Max(maxVersion, version);
            valuePrograms = new RelationQueryProgram?[maxVersion + 1];
            foreach (var (version, info) in _relationInfo.RelationVersions)
                valuePrograms[version] = BuildProgram(valueConjuncts, info.Fields, prefixLength, prefixFieldCount,
                    _selectors, primaryKeyFields, _relationInfo.Prefix.Length, dynamicPrefix);
        }

        return (keyProgram, valuePrograms);
    }

    sealed class AccessPath
    {
        public int SecondaryKeyIndex = -1;
        public TableFieldInfo[] KeyFields = [];
        public byte[] Prefix = [];
        public RelationQueryRange[] Ranges = [];

        /// Builds Ranges, again for each execution when they depend on parameters.
        public RelationQueryRangeBuilder Builder = null!;

        /// Bytes and fields shared by every range: Prefix and the leading single-valued equalities. The length differs
        /// between executions when those equalities have parameters.
        public int CommonPrefixLength;

        public int CommonFieldCount;
        public readonly HashSet<int> Used = [];
        public int Score;

        /// Primary key fields set to a single value by the used equalities.
        public readonly HashSet<string> FixedFields = [];
    }

    // At most this many ranges come from IN lists, so huge lists stay residual conjuncts.
    const int MaxRanges = 1024;

    AccessPath AnalyzeAccessPath(IInternalObjectDBTransaction transaction, List<Conjunct> conjuncts,
        TableFieldInfo[] keyFields, byte[] prefix, int secondaryKeyIndex)
    {
        var builder = new RelationQueryRangeBuilder(prefix);
        var path = new AccessPath
        {
            SecondaryKeyIndex = secondaryKeyIndex, KeyFields = keyFields, Prefix = prefix, Builder = builder
        };
        var parameterValues = RelationQueryParameterScope.CurrentValues;
        var rangeCount = 1;
        var allSingle = true;
        var fieldIndex = 0;
        for (; fieldIndex < keyFields.Length; fieldIndex++)
        {
            var field = keyFields[fieldIndex];
            if (field.Computed) break;
            var values = FindEqualityValues(conjuncts, field.Name, out var conjunctIndex, out var propertyType);
            if (values == null || !CanSaveConstant(propertyType) || !HasValueEqualityOfKeyBytes(propertyType) ||
                rangeCount * values.Count > MaxRanges) break;
            if (field.Handler!.NeedsCtx())
            {
                builder.WriterCtx ??= new DBWriterCtx(transaction);
                _uncacheable = true;
            }

            var save = field.Handler.Save(propertyType, _relationInfo.TypeConverterFactory);
            // With parameters, how many values there are cannot depend on equal values merging.
            var hasParameters = values.Exists(v => v is RelationQueryParameterRef);
            var count = hasParameters
                ? values.Count
                : RelationQueryRangeBuilder.Distinct(save, builder.WriterCtx, propertyType, values, null).Count;
            builder.Fields.Add((save, propertyType, values, count == 1));
            builder.HasParameters |= hasParameters;
            rangeCount *= count;
            path.Used.Add(conjunctIndex);
            path.Score += 2;
            if (count == 1 && IsPrimaryKeyField(field.Name)) path.FixedFields.Add(field.Name);
            if (allSingle && count == 1) path.CommonFieldCount++;
            else allSingle = false;
        }

        if (fieldIndex < keyFields.Length && !keyFields[fieldIndex].Computed)
            AnalyzeRangeField(transaction, conjuncts, keyFields[fieldIndex], path);
        path.Ranges = builder.Build(parameterValues, out path.CommonPrefixLength);
        return path;
    }

    // A range on the field after the equalities: ordinal StartsWith on ordered strings, or bounds on fields whose
    // encoding is order preserving.
    void AnalyzeRangeField(IInternalObjectDBTransaction transaction, List<Conjunct> conjuncts, TableFieldInfo field,
        AccessPath path)
    {
        var handler = field.Handler!;
        var builder = path.Builder;
        object? lower = null, upper = null;
        var lowerInclusive = false;
        var upperInclusive = false;
        Type? type = null;
        foreach (var conjunct in conjuncts)
        {
            if (conjunct.Node is StringMethodQueryNode { Operand: PropQueryNode prop } startsWith &&
                prop.Name == field.Name && handler.Name == "StringOrderable" && UsesEncodedStartsWith(startsWith))
            {
                // StartsWith throws for a null key, so the conjunct stays and the null keys are scanned too.
                var writer = new MemWriter();
                writer.WriteStringOrderedPrefix((string)startsWith.Argument!);
                builder.StartsWith = writer.GetSpan().ToArray();
                writer = new();
                writer.WriteStringOrdered(null);
                builder.NullKey = writer.GetSpan().ToArray();
                path.Score++;
                return;
            }

            if (conjunct.Throws) break;

            if (conjunct.Node is not CompareQueryNode compare || !TrySplitPropertyConstant(compare, out var property,
                    out var constant) || property.Name != field.Name || !CanSaveConstant(property.Type) ||
                !RelationQueryKeyRanges.IsOrderPreserving(handler) ||
                !TryGetExactValue(property, constant, out var value) || value == null)
                continue;
            var op = compare.Left is ConstantQueryNode ? Mirror(compare.Compare) : compare.Compare;
            if (op is QueryCompare.Greater or QueryCompare.GreaterOrEqual && lower == null)
            {
                lower = value;
                lowerInclusive = op == QueryCompare.GreaterOrEqual;
            }
            else if (op is QueryCompare.Less or QueryCompare.LessOrEqual && upper == null)
            {
                upper = value;
                upperInclusive = op == QueryCompare.LessOrEqual;
            }
            else
            {
                continue;
            }

            type = property.Type;
            path.Used.Add(conjunct.Index);
        }

        if (type == null) return;
        path.Score++;
        if (handler.NeedsCtx())
        {
            builder.WriterCtx ??= new DBWriterCtx(transaction);
            _uncacheable = true;
        }

        builder.BoundSave = handler.Save(type, _relationInfo.TypeConverterFactory);
        builder.BoundType = type;
        builder.Lower = lower;
        builder.LowerInclusive = lowerInclusive;
        builder.Upper = upper;
        builder.UpperInclusive = upperInclusive;
        builder.HasParameters |= lower is RelationQueryParameterRef || upper is RelationQueryParameterRef;
    }

    // The value of an exact equality or bound: the constant's value, or a reference to a non-null parameter of the
    // property's type, encoded for each execution.
    static bool TryGetExactValue(PropQueryNode property, ConstantQueryNode constant, out object? value)
    {
        if (constant is ParameterQueryNode parameter && parameter.Type == property.Type && parameter.PeekValue != null)
        {
            value = new RelationQueryParameterRef(parameter.Index);
            return true;
        }

        value = constant.Value;
        return value == null ? property.Type == typeof(string) : value.GetType() == property.Type;
    }

    internal static byte[] Encode(FieldHandlerSave save, IWriterCtx? writerCtx, Type type, object? value)
    {
        var writer = new MemWriter();
        SaveConstant(save, ref writer, writerCtx, type, value);
        return writer.GetSpan().ToArray();
    }

    // Values of the first equality, IN list or OR of equalities on the field written before any throwing conjunct.
    static List<object?>? FindEqualityValues(List<Conjunct> conjuncts, string name, out int conjunctIndex,
        out Type propertyType)
    {
        foreach (var conjunct in conjuncts)
        {
            if (conjunct.Throws) break;
            conjunctIndex = conjunct.Index;
            switch (conjunct.Node)
            {
                case CompareQueryNode { Compare: QueryCompare.Equal } equals
                    when TrySplitPropertyConstant(equals, out var property, out var constant) &&
                         property.Name == name && TryGetExactValue(property, constant, out var value):
                    propertyType = property.Type;
                    return [value];
                case InSetQueryNode { Operand: PropQueryNode property } inSet when property.Name == name:
                {
                    propertyType = property.Type;
                    var values = new List<object?>();
                    if (inSet.Set is QueryStringSet stringSet)
                    {
                        foreach (var value in stringSet.Lookup.Set) values.Add(value);
                        if (stringSet.ContainsNull) values.Add(null);
                        return values;
                    }

                    if (property.Type.IsEnum) continue;
                    foreach (var value in (IEnumerable)inSet.Set) values.Add(value);
                    return values;
                }
                case BoolOrQueryNode or:
                {
                    var values = new List<object?>();
                    Type? type = null;
                    if (CollectOrEqualities(or, name, values, ref type))
                    {
                        propertyType = type!;
                        return values;
                    }

                    break;
                }
            }
        }

        conjunctIndex = -1;
        propertyType = typeof(void);
        return null;
    }

    static bool CollectOrEqualities(QueryNode node, string name, List<object?> values, ref Type? type)
    {
        if (node is BoolOrQueryNode or)
            return CollectOrEqualities(or.Left, name, values, ref type) &&
                   CollectOrEqualities(or.Right, name, values, ref type);
        if (node is not CompareQueryNode { Compare: QueryCompare.Equal } equals ||
            !TrySplitPropertyConstant(equals, out var property, out var constant) || property.Name != name ||
            type != null && type != property.Type || !TryGetExactValue(property, constant, out var value))
            return false;
        type = property.Type;
        values.Add(value);
        return true;
    }

    void CollectFields(QueryNode node)
    {
        if (node is PropQueryNode prop && !_fields.ContainsKey(prop.Name))
        {
            var tableField = _relationInfo.ClientRelationVersionInfo[prop.Name] ??
                             throw new BTDBException($"Property {_itemType.ToSimpleName()}.{prop.Name} was not found.");
            if (tableField.Computed)
                throw new NotSupportedException(
                    $"Computed property {_itemType.ToSimpleName()}.{prop.Name} is not supported in relation query.");
            if (prop.Type != typeof(string) && !RelationQueryTypes.TryGetStorage(prop.Type, out _, out _, out _))
                throw new NotSupportedException(
                    $"Relation query does not support fields of type {prop.Type.ToSimpleName()} yet.");
            var field = new RelationQueryField(prop.Name, prop.Type);
            if (field.IsString)
            {
                field.Slot = AllocFrame(16);
                field.LengthSlot = AllocFrame(8);
                field.CharsObject = AllocObjectSlot();
            }
            else
            {
                var size = RelationQueryTypes.SlotSize(field.StorageType);
                // Every value slot has at least 8 bytes, so a narrower property can be read by truncating a wider
                // stored integer.
                field.Slot = AllocFrame(size + (field.IsNullable ? 8 : 0));
                field.HasValueSlot = (ushort)(field.Slot + size);
            }

            _fields.Add(prop.Name, field);
        }

        ForEachChild(node, CollectFields);
    }

    ushort? _oneSlot;

    // Parameters compared, computed or selected by programs, in frame slots filled for each execution.
    readonly List<(int Index, ushort Slot, Type Storage)> _parameterSlots = [];

    // Set when something compiled depends on the transaction.
    bool _uncacheable;

    ushort AllocFrame(int size)
    {
        if (_frameSize + size > ushort.MaxValue)
            throw new NotSupportedException("Relation query references too many fields.");
        var offset = (ushort)_frameSize;
        _frameSize += size;
        return offset;
    }

    ushort AllocObjectSlot()
    {
        if (_objectCount >= ushort.MaxValue)
            throw new NotSupportedException("Relation query references too many fields.");
        return (ushort)_objectCount++;
    }

    /// Key fields are the primary key fields, or the fields of a secondary index key whose prefix has basePrefixLength
    /// bytes; prefixLength and prefixFieldCount describe the leading fields every scanned key shares.
    RelationQueryProgram BuildProgram(List<Conjunct> conjuncts, ReadOnlyMemory<TableFieldInfo>? valueFields,
        int prefixLength, int prefixFieldCount,
        IReadOnlyList<(QueryNode Node, RelationQueryResult Result)> selectors, TableFieldInfo[] keyFieldArray,
        int basePrefixLength, bool dynamicPrefix = false)
    {
        var keyFields = keyFieldArray.AsSpan();
        var specialized = new List<Conjunct>(conjuncts.Count);
        var seenThrowing = false;
        foreach (var conjunct in conjuncts)
        {
            // Properties missing in this value version read as their default value.
            var node = valueFields == null
                ? conjunct.Node
                : Simplify(SubstituteMissing(conjunct.Node, valueFields.Value.Span));
            if (IsFalse(node) && !seenThrowing) return new(null, 0, false);
            if (IsTrue(node)) continue;
            var throws = CanThrow(node);
            specialized.Add(new(node, conjunct.Index, throws));
            seenThrowing |= throws;
        }

        if (valueFields != null)
        {
            var specializedSelectors = new List<(QueryNode Node, RelationQueryResult Result)>(selectors.Count);
            foreach (var (node, result) in selectors)
                specializedSelectors.Add((Simplify(SubstituteMissing(node, valueFields.Value.Span)), result));
            selectors = specializedSelectors;
        }

        if (specialized.Count == 0 && selectors.Count == 0) return new(null, 0, true);

        var names = new HashSet<string>();
        foreach (var conjunct in specialized) CollectPropertyNames(conjunct.Node, names);
        foreach (var (node, _) in selectors) CollectPropertyNames(node, names);
        var firstKeyField = int.MaxValue;
        var lastKeyField = -1;
        for (var i = 0; i < keyFields.Length; i++)
        {
            if (!names.Contains(keyFields[i].Name)) continue;
            firstKeyField = Math.Min(firstKeyField, i);
            lastKeyField = i;
        }

        // Fields encoded in the cursor prefix are skipped unless a remaining conjunct still needs them.
        var startField = firstKeyField >= prefixFieldCount ? prefixFieldCount : 0;
        var keyStart = startField == prefixFieldCount ? prefixLength : basePrefixLength;

        _encodings.Clear();
        for (var i = startField; i <= lastKeyField; i++) RememberEncoding(keyFields[i]);
        var lastValueField = -1;
        var fields = valueFields.HasValue ? valueFields.Value.Span : default;
        for (var i = 0; i < fields.Length; i++)
        {
            if (!names.Contains(fields[i].Name) || fields[i].Computed) continue;
            RememberEncoding(fields[i]);
            lastValueField = i;
        }

        var needsChars = new HashSet<string>();
        foreach (var conjunct in specialized) CollectNeedsChars(conjunct.Node, needsChars);
        foreach (var (node, result) in selectors)
            if (!result.IsString) CollectNeedsChars(node, needsChars);

        var order = OrderConjuncts(specialized, valueFields == null ? keyFields : fields);

        _builder = new();
        var falseLabel = _builder.DeclareLabel("false");
        var emitted = 0;
        var next = 0;
        // Conjuncts needing no field come first in the key program; in a value program, conjuncts using only key
        // fields follow the key reads.
        if (valueFields == null) emitted += EmitReady(order, ref next, -1, falseLabel);
        var skip = new SkipRun(OpCode.SkipKey);
        for (var i = startField; i <= lastKeyField; i++)
        {
            var field = keyFields[i];
            if (field.Computed) continue;
            if (!names.Contains(field.Name))
            {
                EmitSkip(ref skip, field.Handler!, true);
                continue;
            }

            skip.Flush(_builder);
            EmitRead(true, field, _fields[field.Name], needsChars.Contains(field.Name));
            if (valueFields == null) emitted += EmitReady(order, ref next, i, falseLabel);
        }

        if (valueFields != null)
        {
            emitted += EmitReady(order, ref next, -1, falseLabel);
            skip = new(OpCode.SkipValue);
            for (var i = 0; i <= lastValueField; i++)
            {
                var field = fields[i];
                if (field.Computed) continue;
                if (!names.Contains(field.Name))
                {
                    EmitSkip(ref skip, field.Handler!, false);
                    continue;
                }

                skip.Flush(_builder);
                EmitRead(false, field, _fields[field.Name], needsChars.Contains(field.Name));
                emitted += EmitReady(order, ref next, i, falseLabel);
            }
        }

        if (emitted != specialized.Count)
            throw new InvalidOperationException("Relation query compiler did not emit every conjunct.");
        foreach (var (node, result) in selectors)
        {
            if (!result.IsString)
            {
                EmitSelector(node, result);
                continue;
            }

            // A string selector creates the string from the captured field; a missing field is a constant null.
            if (node is PropQueryNode stringProp)
            {
                _builder.AddInstruction(OpCode.StringFromCaptured);
                _builder.AddUInt8((byte)_encodings[stringProp.Name]);
                _builder.AddUInt16(_fields[stringProp.Name].Slot);
            }
            else
            {
                _builder.AddInstruction(OpCode.SetObjectNull);
            }

            _builder.AddUInt16(result.Slot);
        }
        _builder.AddInstruction(OpCode.SetBoolResultTrue);
        _builder.AddInstruction(OpCode.Stop);
        _builder.MarkLabel(falseLabel);
        _builder.AddInstruction(OpCode.SetBoolResultFalse);
        _builder.AddInstruction(OpCode.Stop);
        var program = _builder.Materialize();
        _builder = null!;
        return new(program, keyStart, false) { KeyStartFromRanges = dynamicPrefix && keyStart == prefixLength };
    }

    // Stores the selector value and its HasValue into the result slot.
    void EmitSelector(QueryNode selector, RelationQueryResult result)
    {
        // The property was read into the result slots; a missing property is still stored as its constant.
        if (result.IsField && selector is PropQueryNode) return;
        var operand = CompileValue(selector);
        if (operand.StorageType != result.StorageType)
            throw new NotSupportedException("Relation query selector type does not match its result.");
        if (operand.HasValue != QueryHasValue.Never)
        {
            _builder.AddInstruction(OpCode.CopyToFrame);
            _builder.AddUInt8((byte)RelationQueryTypes.ValueSize(result.StorageType));
            _builder.AddUInt16(result.Slot);
            _builder.AddUInt8((byte)operand.Kind);
            _builder.AddUInt32(operand.Offset);
        }

        if (operand.HasValue == QueryHasValue.Frame)
        {
            _builder.AddInstruction(OpCode.CopyToFrame);
            _builder.AddUInt8(1);
            _builder.AddUInt16(result.HasValueSlot);
            _builder.AddUInt8((byte)QueryOperandKind.Frame);
            _builder.AddUInt32(operand.HasValueSlot);
            return;
        }

        _builder.AddInstruction(OpCode.SetFrameByte);
        _builder.AddUInt16(result.HasValueSlot);
        _builder.AddUInt8(operand.HasValue == QueryHasValue.Always ? (byte)1 : (byte)0);
    }

    void RememberEncoding(TableFieldInfo field)
    {
        if (!_fields.TryGetValue(field.Name, out var queryField) || !queryField.IsString) return;
        _encodings[field.Name] = field.Handler!.Name switch
        {
            "String" => QueryStringEncoding.String,
            "StringOrderable" => QueryStringEncoding.StringOrdered,
            _ => throw new NotSupportedException(
                $"Field handler {field.Handler.Name} cannot load a relation query string field yet.")
        };
    }

    readonly record struct OrderedConjunct(QueryNode Node, int Ready, int Segment, bool Throws, int Cost, int Index);

    // Each conjunct is tested right after the last field it needs. A throwing conjunct waits for every earlier
    // conjunct and later conjuncts wait for it; between such barriers cheaper conjuncts go first.
    static List<OrderedConjunct> OrderConjuncts(List<Conjunct> conjuncts, ReadOnlySpan<TableFieldInfo> fields)
    {
        var result = new List<OrderedConjunct>(conjuncts.Count);
        var names = new HashSet<string>();
        var maxReady = -1;
        var barrierReady = -1;
        var segment = 0;
        foreach (var conjunct in conjuncts)
        {
            names.Clear();
            CollectPropertyNames(conjunct.Node, names);
            var ready = -1;
            for (var i = 0; i < fields.Length; i++)
                if (names.Contains(fields[i].Name)) ready = i;
            ready = conjunct.Throws ? Math.Max(ready, maxReady) : Math.Max(ready, barrierReady);
            result.Add(new(conjunct.Node, ready, segment, conjunct.Throws, Cost(conjunct.Node), conjunct.Index));
            maxReady = Math.Max(maxReady, ready);
            if (!conjunct.Throws) continue;
            barrierReady = ready;
            segment++;
        }

        result.Sort(static (a, b) =>
        {
            var c = a.Ready.CompareTo(b.Ready);
            if (c != 0) return c;
            c = a.Segment.CompareTo(b.Segment);
            if (c != 0) return c;
            c = a.Throws.CompareTo(b.Throws);
            if (c != 0) return c;
            c = a.Cost.CompareTo(b.Cost);
            return c != 0 ? c : a.Index.CompareTo(b.Index);
        });
        return result;
    }

    static int Cost(QueryNode node)
    {
        var cost = node switch
        {
            StringMethodQueryNode or StringCompareOrdinalQueryNode => 8,
            InSetQueryNode => 3,
            ArithmeticQueryNode or ConvertQueryNode => 2,
            _ => 1
        };
        ForEachChild(node, child => cost += Cost(child));
        return cost;
    }

    int EmitReady(List<OrderedConjunct> order, ref int next, int fieldIndex, uint falseLabel)
    {
        var emitted = 0;
        while (next < order.Count && order[next].Ready <= fieldIndex)
        {
            EmitBranch(order[next].Node, falseLabel, false);
            next++;
            emitted++;
        }

        return emitted;
    }

    struct SkipRun
    {
        readonly OpCode _opCode;
        CodeOp _codeOp;
        ushort _count;

        public SkipRun(OpCode opCode)
        {
            _opCode = opCode;
        }

        public void Add(InterpreterBuilder builder, CodeOp codeOp)
        {
            if (_count != 0 && (_codeOp != codeOp || _count == ushort.MaxValue)) Flush(builder);
            _codeOp = codeOp;
            _count++;
        }

        public void Flush(InterpreterBuilder builder)
        {
            if (_count == 0) return;
            builder.AddInstruction(_opCode);
            builder.AddUInt8((byte)_codeOp);
            builder.AddUInt16(_count);
            _count = 0;
        }
    }

    void EmitSkip(ref SkipRun skip, IFieldHandler handler, bool fromKey)
    {
        if (TryGetSkipCodeOp(handler, out var codeOp))
        {
            skip.Add(_builder, codeOp);
            return;
        }

        if (handler.NeedsCtx())
            throw new NotSupportedException($"Field handler {handler.Name} cannot skip a relation query field yet.");
        skip.Flush(_builder);
        _builder.AddInstruction(fromKey ? OpCode.SkipKeyByHandler : OpCode.SkipValueByHandler);
        _builder.AddUInt32((uint)_builder.AddObjectConstant(handler));
    }

    void EmitRead(bool fromKey, TableFieldInfo field, RelationQueryField queryField, bool needsChars)
    {
        var handler = field.Handler!;
        if (queryField.IsString)
        {
            var encoding = _encodings[field.Name];
            _builder.AddInstruction(fromKey ? OpCode.CaptureKey : OpCode.CaptureValue);
            _builder.AddUInt8((byte)(encoding == QueryStringEncoding.String ? CodeOp.SkipString : CodeOp.SkipStringOrdered));
            _builder.AddUInt16(queryField.Slot);
            if (!needsChars) return;
            _builder.AddInstruction(OpCode.DecodeString);
            _builder.AddUInt8((byte)encoding);
            _builder.AddUInt16(queryField.Slot);
            _builder.AddUInt16(queryField.CharsObject);
            _builder.AddUInt16(queryField.LengthSlot);
            return;
        }

        if (handler is NullableFieldHandler nullableHandler)
        {
            if (!queryField.IsNullable)
                throw new NotSupportedException(
                    $"Relation query cannot read nullable stored field {field.Name} as {queryField.Type.ToSimpleName()}.");
            EmitReadInto(fromKey, CodeOp.ReadBool, false, queryField.HasValueSlot);
            var nullLabel = _builder.DeclareLabel("null");
            EmitJmpIfFrameConst(false, QueryValueType.UInt8, QueryCompare.Equal, queryField.HasValueSlot,
                AddFrameConstant(typeof(bool), true), nullLabel);
            EmitReadPlain(fromKey, nullableHandler.ItemHandler, queryField);
            _builder.MarkLabel(nullLabel);
            return;
        }

        EmitReadPlain(fromKey, handler, queryField);
        if (!queryField.IsNullable) return;
        _builder.AddInstruction(OpCode.SetFrameByte);
        _builder.AddUInt16(queryField.HasValueSlot);
        _builder.AddUInt8(1);
    }

    void EmitReadPlain(bool fromKey, IFieldHandler handler, RelationQueryField queryField)
    {
        CodeOp codeOp;
        Type sourceType;
        if (handler is EnumFieldHandler)
        {
            // EnumFieldHandler.HandledType() builds a type with Reflection.Emit, so only its signedness is used.
            var signed = new EnumFieldHandler.EnumConfiguration(handler.Configuration!).Signed;
            codeOp = signed ? CodeOp.ReadVInt64 : CodeOp.ReadVUInt64;
            sourceType = signed ? typeof(long) : typeof(ulong);
        }
        else
        {
            codeOp = GetReadCodeOp(handler);
            sourceType = handler.HandledType()!;
        }

        var target = queryField.StorageType;
        if (sourceType == target || CanTruncate(sourceType, target))
        {
            EmitReadInto(fromKey, codeOp, false, queryField.Slot);
            return;
        }

        var sourceIsObject = !sourceType.IsValueType;
        var temporary = sourceIsObject ? _objectTemporary : _frameTemporary;
        EmitReadInto(fromKey, codeOp, sourceIsObject, temporary);
        _builder.AddInstruction(OpCode.ConvertSlot);
        _builder.AddUInt8((byte)(sourceIsObject ? QuerySlotKind.Object : QuerySlotKind.Frame));
        _builder.AddUInt16(temporary);
        _builder.AddUInt8((byte)QuerySlotKind.Frame);
        _builder.AddUInt16(queryField.Slot);
        _builder.AddConverterIndex(_relationInfo.TypeConverterFactory, sourceType, target);
    }

    // The default converter narrows integers by unchecked casts, which on little endian is the same as reading the low
    // bytes of the wider value from the 8 byte frame slot.
    bool CanTruncate(Type sourceType, Type targetType)
    {
        return BitConverter.IsLittleEndian &&
               _relationInfo.TypeConverterFactory.GetType() == typeof(DefaultTypeConverterFactory) &&
               RelationQueryTypes.IsInteger(sourceType) && RelationQueryTypes.IsInteger(targetType) &&
               SizeOf(sourceType) >= SizeOf(targetType);
    }

    static int SizeOf(Type type)
    {
        if (type == typeof(byte) || type == typeof(sbyte)) return 1;
        if (type == typeof(ushort) || type == typeof(short)) return 2;
        if (type == typeof(uint) || type == typeof(int)) return 4;
        return 8;
    }

    void EmitReadInto(bool fromKey, CodeOp codeOp, bool isObject, ushort index)
    {
        _builder.AddInstruction(fromKey
            ? isObject ? OpCode.ReadKeyToObject : OpCode.ReadKeyToFrame
            : isObject ? OpCode.ReadValueToObject : OpCode.ReadValueToFrame);
        _builder.AddUInt8((byte)codeOp);
        _builder.AddUInt16(index);
    }

    static CodeOp GetReadCodeOp(IFieldHandler handler)
    {
        return handler.Name switch
        {
            "String" => CodeOp.ReadString,
            "StringOrderable" => CodeOp.ReadStringOrdered,
            "UInt8" => CodeOp.ReadUInt8,
            "Int8" => CodeOp.ReadInt8,
            "Int8Orderable" => CodeOp.ReadInt8Ordered,
            "Unsigned" => CodeOp.ReadVUInt64,
            "Signed" => CodeOp.ReadVInt64,
            "Bool" => CodeOp.ReadBool,
            "Single" => CodeOp.ReadSingle,
            "Double" => CodeOp.ReadDouble,
            "DateTime" => CodeOp.ReadDateTime,
            "Guid" => CodeOp.ReadGuid,
            "Decimal" => CodeOp.ReadDecimal,
            "TimeSpan" => CodeOp.ReadTimeSpan,
            "DateTimeOffset" => CodeOp.ReadDateTimeOffset,
            _ => throw new NotSupportedException(
                $"Field handler {handler.Name} cannot load a relation query field yet.")
        };
    }

    static bool TryGetSkipCodeOp(IFieldHandler handler, out CodeOp codeOp)
    {
        if (handler is EnumFieldHandler)
        {
            codeOp = new EnumFieldHandler.EnumConfiguration(handler.Configuration!).Signed
                ? CodeOp.SkipVInt64
                : CodeOp.SkipVUInt64;
            return true;
        }

        codeOp = handler.Name switch
        {
            "String" => CodeOp.SkipString,
            "StringOrderable" => CodeOp.SkipStringOrdered,
            "UInt8" or "Int8" or "Int8Orderable" or "Bool" => CodeOp.Skip1Byte,
            "Unsigned" => CodeOp.SkipVUInt64,
            "Signed" => CodeOp.SkipVInt64,
            "Single" => CodeOp.Skip4Bytes,
            "Double" or "DateTime" => CodeOp.Skip8Bytes,
            "Guid" => CodeOp.SkipGuid,
            "Decimal" => CodeOp.SkipDecimal,
            "TimeSpan" => CodeOp.SkipVInt64,
            "DateTimeOffset" => CodeOp.SkipDateTimeOffset,
            "IPAddress" => CodeOp.SkipIPAddress,
            "Version" => CodeOp.SkipVersion,
            "StringValues" => CodeOp.SkipStringValues,
            _ => CodeOp.Invalid
        };
        return codeOp != CodeOp.Invalid;
    }

    // Emits code jumping to label when node evaluates to jumpIfTrue and falling through otherwise.
    void EmitBranch(QueryNode node, uint label, bool jumpIfTrue)
    {
        switch (node)
        {
            case ConstantQueryNode { Value: bool value }:
                EmitConstantBranch(value, label, jumpIfTrue);
                return;
            case ThrowQueryNode throwNode:
                EmitThrow(throwNode.Exception);
                return;
            case BoolAndQueryNode and:
                if (jumpIfTrue)
                {
                    var skipLabel = _builder.DeclareLabel("andSkip");
                    EmitBranch(and.Left, skipLabel, false);
                    EmitBranch(and.Right, label, true);
                    _builder.MarkLabel(skipLabel);
                }
                else
                {
                    EmitBranch(and.Left, label, false);
                    EmitBranch(and.Right, label, false);
                }

                return;
            case BoolOrQueryNode or:
                if (jumpIfTrue)
                {
                    EmitBranch(or.Left, label, true);
                    EmitBranch(or.Right, label, true);
                }
                else
                {
                    var skipLabel = _builder.DeclareLabel("orSkip");
                    EmitBranch(or.Left, skipLabel, true);
                    EmitBranch(or.Right, label, false);
                    _builder.MarkLabel(skipLabel);
                }

                return;
            case NotQueryNode not:
                EmitBranch(not.Operand, label, !jumpIfTrue);
                return;
            case HasValueQueryNode hasValue:
                EmitHasValueBranch(CompileValue(hasValue.Operand), true, label, jumpIfTrue);
                return;
            case CompareQueryNode compare:
                EmitCompare(compare, label, jumpIfTrue);
                return;
            case StringMethodQueryNode stringMethod:
                EmitStringMethod(stringMethod, label, jumpIfTrue);
                return;
            case StringIsNullOrEmptyQueryNode isNullOrEmpty:
                EmitIsNullOrEmpty(isNullOrEmpty, label, jumpIfTrue);
                return;
            case InSetQueryNode inSet:
                EmitInSet(inSet, label, jumpIfTrue);
                return;
            default:
                throw new NotSupportedException("Relation query predicate is not supported.");
        }
    }

    void EmitConstantBranch(bool value, uint label, bool jumpIfTrue)
    {
        if (value != jumpIfTrue) return;
        _builder.AddInstruction(OpCode.Jmp);
        _builder.AddLabelParameter(label);
    }

    void EmitJmp(uint label)
    {
        _builder.AddInstruction(OpCode.Jmp);
        _builder.AddLabelParameter(label);
    }

    void EmitThrow(QueryException exception)
    {
        _builder.AddInstruction(OpCode.ThrowQueryException);
        _builder.AddUInt8((byte)exception);
    }

    void EmitJmpIfFrameConst(bool jumpIfTrue, QueryValueType valueType, QueryCompare compare, uint frameOffset,
        uint constantOffset, uint label)
    {
        _builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfFrameConst : OpCode.JmpIfNotFrameConst);
        _builder.AddUInt8((byte)valueType);
        _builder.AddUInt8((byte)compare);
        _builder.AddUInt16((ushort)frameOffset);
        _builder.AddUInt32(constantOffset);
        _builder.AddLabelTarget(label);
    }

    // Jumps to label when (operand has a value) == wantHasValue equals jumpIfTrue.
    void EmitHasValueBranch(QueryOperand operand, bool wantHasValue, uint label, bool jumpIfTrue)
    {
        switch (operand.HasValue)
        {
            case QueryHasValue.Always:
                EmitConstantBranch(wantHasValue, label, jumpIfTrue);
                return;
            case QueryHasValue.Never:
                EmitConstantBranch(!wantHasValue, label, jumpIfTrue);
                return;
            default:
                EmitJmpIfFrameConst(jumpIfTrue, QueryValueType.UInt8, QueryCompare.Equal, operand.HasValueSlot,
                    AddFrameConstant(typeof(bool), wantHasValue), label);
                return;
        }
    }

    void EnsureHasValue(QueryOperand operand)
    {
        if (operand.HasValue == QueryHasValue.Always) return;
        if (operand.HasValue == QueryHasValue.Never)
        {
            EmitThrow(QueryException.NullableHasNoValue);
            return;
        }

        var hasValueLabel = _builder.DeclareLabel("hasValue");
        EmitHasValueBranch(operand, true, hasValueLabel, true);
        EmitThrow(QueryException.NullableHasNoValue);
        _builder.MarkLabel(hasValueLabel);
    }

    QueryOperand CompileValue(QueryNode node)
    {
        switch (node)
        {
            case PropQueryNode prop:
            {
                var field = _fields[prop.Name];
                if (field.IsString)
                    throw new NotSupportedException("Relation query cannot use a string property as a value here.");
                return new(QueryOperandKind.Frame, field.Slot, field.ValueType, field.StorageType,
                    field.IsNullable ? QueryHasValue.Frame : QueryHasValue.Always, field.HasValueSlot);
            }
            case ParameterQueryNode parameter when parameter.PeekValue != null &&
                                                   RelationQueryTypes.TryGetStorage(parameter.Type,
                                                       out var parameterStorage, out var parameterValueType, out _):
            {
                // Whether the value is null is part of the cache key, so a non-null value always has a value.
                var index = _parameterSlots.FindIndex(p => p.Index == parameter.Index);
                if (index < 0)
                {
                    _parameterSlots.Add((parameter.Index, AllocFrame(RelationQueryTypes.SlotSize(parameterStorage)),
                        parameterStorage));
                    index = _parameterSlots.Count - 1;
                }

                return new(QueryOperandKind.Frame, _parameterSlots[index].Slot, parameterValueType, parameterStorage,
                    QueryHasValue.Always, 0);
            }
            case ConstantQueryNode constant:
            {
                if (!RelationQueryTypes.TryGetStorage(constant.Type, out var storage, out var valueType, out _))
                    throw new NotSupportedException(
                        $"Relation query constants of type {constant.Type.ToSimpleName()} are not supported yet.");
                if (constant.Value == null)
                    return new(QueryOperandKind.Constant, 0, valueType, storage, QueryHasValue.Never, 0);
                return new(QueryOperandKind.Constant, AddFrameConstant(storage, constant.Value), valueType, storage,
                    QueryHasValue.Always, 0);
            }
            case ThrowQueryNode throwNode:
            {
                EmitThrow(throwNode.Exception);
                RelationQueryTypes.TryGetStorage(throwNode.Type, out var storage, out var valueType, out _);
                return new(QueryOperandKind.Constant, 0, valueType, storage, QueryHasValue.Never, 0);
            }
            case ConvertQueryNode convert:
                return CompileConvert(convert);
            case ArithmeticQueryNode arithmetic:
                return CompileArithmetic(arithmetic);
            case NullableValueQueryNode nullableValue:
            {
                var operand = CompileValue(nullableValue.Operand);
                EnsureHasValue(operand);
                return operand.WithHasValue(QueryHasValue.Always);
            }
            case StringLengthQueryNode stringLength:
                return CompileStringLength(stringLength);
            default:
                throw new NotSupportedException("Relation query expression is not supported.");
        }
    }

    QueryOperand CompileConvert(ConvertQueryNode convert)
    {
        var source = CompileValue(convert.Operand);
        RelationQueryTypes.TryGetStorage(convert.Type, out var storage, out var valueType, out var nullable);
        if (!nullable && source.HasValue != QueryHasValue.Always)
        {
            EnsureHasValue(source);
            source = source.WithHasValue(QueryHasValue.Always);
        }

        if (source.HasValue == QueryHasValue.Never) return source.WithType(valueType, storage);
        if (source.StorageType == storage) return source;
        if (!RelationQueryTypes.IsNumeric(source.StorageType) || !RelationQueryTypes.IsNumeric(storage))
            throw new NotSupportedException(
                $"Relation query cannot convert {source.StorageType.ToSimpleName()} to {storage.ToSimpleName()}.");
        var lifted = source.HasValue == QueryHasValue.Frame;
        var size = RelationQueryTypes.SlotSize(storage);
        var destination = AllocFrame(size + (lifted ? 8 : 0));
        var hasValueSlot = (ushort)(destination + size);
        var nullLabel = 0u;
        if (lifted)
        {
            nullLabel = _builder.DeclareLabel("convertNull");
            EmitHasValueBranch(source, false, nullLabel, true);
        }

        _builder.AddInstruction(OpCode.ConvertNumber);
        _builder.AddUInt8((byte)source.ValueType);
        _builder.AddUInt8((byte)valueType);
        _builder.AddUInt8(convert.IsChecked ? (byte)1 : (byte)0);
        _builder.AddUInt16(destination);
        _builder.AddUInt8((byte)source.Kind);
        _builder.AddUInt32(source.Offset);
        if (lifted) EmitLiftedEnd(hasValueSlot, nullLabel);
        return new(QueryOperandKind.Frame, destination, valueType, storage,
            lifted ? QueryHasValue.Frame : QueryHasValue.Always, hasValueSlot);
    }

    // Sets HasValue of a lifted result: 1 when the computation ran, 0 when it jumped to nullLabel.
    void EmitLiftedEnd(ushort hasValueSlot, uint nullLabel)
    {
        var endLabel = _builder.DeclareLabel("liftedEnd");
        _builder.AddInstruction(OpCode.SetFrameByte);
        _builder.AddUInt16(hasValueSlot);
        _builder.AddUInt8(1);
        EmitJmp(endLabel);
        _builder.MarkLabel(nullLabel);
        _builder.AddInstruction(OpCode.SetFrameByte);
        _builder.AddUInt16(hasValueSlot);
        _builder.AddUInt8(0);
        _builder.MarkLabel(endLabel);
    }

    QueryOperand CompileArithmetic(ArithmeticQueryNode arithmetic)
    {
        RelationQueryTypes.TryGetStorage(arithmetic.Type, out var storage, out var valueType, out _);
        var left = CompileValue(arithmetic.Left);
        var right = arithmetic.Right == null ? left : CompileValue(arithmetic.Right);
        if (left.StorageType != storage || right.StorageType != storage)
            throw new NotSupportedException("Relation query arithmetic operands must have the same type.");
        if (left.HasValue == QueryHasValue.Never || right.HasValue == QueryHasValue.Never)
            return new(QueryOperandKind.Constant, 0, valueType, storage, QueryHasValue.Never, 0);
        var lifted = left.HasValue == QueryHasValue.Frame || right.HasValue == QueryHasValue.Frame;
        var size = RelationQueryTypes.SlotSize(storage);
        var destination = AllocFrame(size + (lifted ? 8 : 0));
        var hasValueSlot = (ushort)(destination + size);
        var nullLabel = 0u;
        if (lifted)
        {
            nullLabel = _builder.DeclareLabel("arithmeticNull");
            if (left.HasValue == QueryHasValue.Frame) EmitHasValueBranch(left, false, nullLabel, true);
            if (right.HasValue == QueryHasValue.Frame) EmitHasValueBranch(right, false, nullLabel, true);
        }

        _builder.AddInstruction(OpCode.Arithmetic);
        _builder.AddUInt8((byte)valueType);
        _builder.AddUInt8((byte)arithmetic.Operation);
        _builder.AddUInt16(destination);
        _builder.AddUInt8((byte)left.Kind);
        _builder.AddUInt32(left.Offset);
        _builder.AddUInt8((byte)right.Kind);
        _builder.AddUInt32(right.Offset);
        if (lifted) EmitLiftedEnd(hasValueSlot, nullLabel);
        return new(QueryOperandKind.Frame, destination, valueType, storage,
            lifted ? QueryHasValue.Frame : QueryHasValue.Always, hasValueSlot);
    }

    QueryOperand CompileStringLength(StringLengthQueryNode stringLength)
    {
        var field = StringField(stringLength.Operand);
        ushort length;
        if (_encodings[field.Name] == QueryStringEncoding.String)
        {
            length = AllocFrame(8);
            _builder.AddInstruction(OpCode.StringLength);
            _builder.AddUInt16(field.Slot);
            _builder.AddUInt16(length);
        }
        else
        {
            length = field.LengthSlot;
        }

        var notNullLabel = _builder.DeclareLabel("notNull");
        EmitJmpIfFrameConst(false, QueryValueType.Int32, QueryCompare.Equal, length, AddFrameConstant(typeof(int), -1),
            notNullLabel);
        EmitThrow(QueryException.NullReference);
        _builder.MarkLabel(notNullLabel);
        return new(QueryOperandKind.Frame, length, QueryValueType.Int32, typeof(int), QueryHasValue.Always, 0);
    }

    RelationQueryField StringField(QueryNode node)
    {
        if (node is PropQueryNode prop && _fields[prop.Name].IsString) return _fields[prop.Name];
        throw new NotSupportedException("Relation query supports string operations only on string properties.");
    }

    void EmitCompare(CompareQueryNode compareNode, uint label, bool jumpIfTrue)
    {
        if (compareNode.Left is StringCompareOrdinalQueryNode || compareNode.Right is StringCompareOrdinalQueryNode)
        {
            EmitStringCompareOrdinal(compareNode, label, jumpIfTrue);
            return;
        }

        if (compareNode.Left.Type == typeof(string) || compareNode.Right.Type == typeof(string))
        {
            EmitStringEquality(compareNode, label, jumpIfTrue);
            return;
        }

        var compare = compareNode.Compare;
        var left = CompileValue(compareNode.Left);
        var right = CompileValue(compareNode.Right);
        if (left.Kind == QueryOperandKind.Constant && right.Kind == QueryOperandKind.Frame)
        {
            (left, right) = (right, left);
            compare = Mirror(compare);
        }

        var equality = compare is QueryCompare.Equal or QueryCompare.NotEqual;
        if (left.HasValue == QueryHasValue.Never || right.HasValue == QueryHasValue.Never)
        {
            if (!equality)
            {
                EmitConstantBranch(false, label, jumpIfTrue);
                return;
            }

            // Comparing with null tests HasValue of the other operand.
            var other = left.HasValue == QueryHasValue.Never ? right : left;
            if (other.HasValue == QueryHasValue.Never)
                EmitConstantBranch(compare == QueryCompare.Equal, label, jumpIfTrue);
            else
                EmitHasValueBranch(other, compare == QueryCompare.NotEqual, label, jumpIfTrue);
            return;
        }

        if (left.Kind == QueryOperandKind.Constant)
            throw new NotSupportedException("Relation query cannot compare two constants.");
        if (left.ValueType != right.ValueType || left.StorageType != right.StorageType)
            throw new NotSupportedException("Relation query comparison operands must have the same type.");

        if (left.HasValue == QueryHasValue.Always && right.HasValue == QueryHasValue.Always)
        {
            EmitPlainCompare(left, right, compare, label, jumpIfTrue);
            return;
        }

        // Lifted comparison: null equals only null, and ordering with null is false.
        var endLabel = _builder.DeclareLabel("compareEnd");
        var leftNullLabel = _builder.DeclareLabel("leftNull");
        var rightNullLabel = _builder.DeclareLabel("rightNull");
        if (left.HasValue == QueryHasValue.Frame) EmitHasValueBranch(left, false, leftNullLabel, true);
        if (right.HasValue == QueryHasValue.Frame) EmitHasValueBranch(right, false, rightNullLabel, true);
        EmitPlainCompare(left, right, compare, label, jumpIfTrue);
        EmitJmp(endLabel);
        if (left.HasValue == QueryHasValue.Frame)
        {
            _builder.MarkLabel(leftNullLabel);
            if (right.HasValue == QueryHasValue.Frame)
            {
                var bothNullLabel = _builder.DeclareLabel("bothNull");
                EmitHasValueBranch(right, false, bothNullLabel, true);
                EmitResult(compare == QueryCompare.NotEqual, label, jumpIfTrue, endLabel);
                _builder.MarkLabel(bothNullLabel);
                EmitResult(compare == QueryCompare.Equal, label, jumpIfTrue, endLabel);
            }
            else
            {
                EmitResult(compare == QueryCompare.NotEqual, label, jumpIfTrue, endLabel);
            }
        }

        if (right.HasValue == QueryHasValue.Frame)
        {
            _builder.MarkLabel(rightNullLabel);
            EmitResult(compare == QueryCompare.NotEqual, label, jumpIfTrue, endLabel);
        }

        // Unused labels still need a position.
        if (left.HasValue != QueryHasValue.Frame) _builder.MarkLabel(leftNullLabel);
        if (right.HasValue != QueryHasValue.Frame) _builder.MarkLabel(rightNullLabel);
        _builder.MarkLabel(endLabel);
    }

    void EmitResult(bool result, uint label, bool jumpIfTrue, uint endLabel)
    {
        EmitJmp(result == jumpIfTrue ? label : endLabel);
    }

    void EmitPlainCompare(QueryOperand left, QueryOperand right, QueryCompare compare, uint label, bool jumpIfTrue)
    {
        if (right.Kind == QueryOperandKind.Constant)
        {
            EmitJmpIfFrameConst(jumpIfTrue, left.ValueType, compare, left.Offset, right.Offset, label);
            return;
        }

        _builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfFrameFrame : OpCode.JmpIfNotFrameFrame);
        _builder.AddUInt8((byte)left.ValueType);
        _builder.AddUInt8((byte)compare);
        _builder.AddUInt16((ushort)left.Offset);
        _builder.AddUInt16((ushort)right.Offset);
        _builder.AddLabelTarget(label);
    }

    static QueryCompare Mirror(QueryCompare compare)
    {
        return compare switch
        {
            QueryCompare.Less => QueryCompare.Greater,
            QueryCompare.LessOrEqual => QueryCompare.GreaterOrEqual,
            QueryCompare.Greater => QueryCompare.Less,
            QueryCompare.GreaterOrEqual => QueryCompare.LessOrEqual,
            _ => compare
        };
    }

    void EmitStringEquality(CompareQueryNode compareNode, uint label, bool jumpIfTrue)
    {
        if (compareNode.Compare is not (QueryCompare.Equal or QueryCompare.NotEqual))
            throw new NotSupportedException("Relation query supports only equality of strings.");
        var (prop, constant) = compareNode.Left is ConstantQueryNode leftConstant
            ? (compareNode.Right, leftConstant)
            : (compareNode.Left, compareNode.Right as ConstantQueryNode ??
                                 throw new NotSupportedException(
                                     "Relation query string equality supports a property and a constant only."));
        if (constant.Value != null && constant.Value is not string)
            throw new NotSupportedException("Relation query constant type does not match property type.");
        EmitEncodedEquality(StringField(prop), (string?)constant.Value,
            jumpIfTrue ^ (compareNode.Compare == QueryCompare.NotEqual), label);
    }

    void EmitEncodedEquality(RelationQueryField field, string? value, bool jumpIfEqual, uint label)
    {
        var writer = new MemWriter();
        if (_encodings[field.Name] == QueryStringEncoding.String) writer.WriteString(value);
        else writer.WriteStringOrdered(value);
        var bytes = writer.GetSpan();
        _builder.AddInstruction(jumpIfEqual ? OpCode.JmpIfEncodedEqual : OpCode.JmpIfNotEncodedEqual);
        _builder.AddUInt16(field.Slot);
        _builder.AddUInt32(_builder.AddConstantBytes(bytes));
        _builder.AddUInt32((uint)bytes.Length);
        _builder.AddLabelTarget(label);
    }

    void EmitStringCompareOrdinal(CompareQueryNode compareNode, uint label, bool jumpIfTrue)
    {
        var compare = compareNode.Compare;
        StringCompareOrdinalQueryNode node;
        ConstantQueryNode? zero;
        if (compareNode.Left is StringCompareOrdinalQueryNode leftNode)
        {
            node = leftNode;
            zero = compareNode.Right as ConstantQueryNode;
        }
        else
        {
            node = (StringCompareOrdinalQueryNode)compareNode.Right;
            zero = compareNode.Left as ConstantQueryNode;
            compare = Mirror(compare);
        }

        // CompareOrdinal returns any negative or positive number, so only its sign can be compared.
        if (zero?.Value is not 0)
            throw new NotSupportedException("Relation query supports comparing string.CompareOrdinal only to 0.");
        if (node.Reversed) compare = Mirror(compare);
        EmitStringOp(StringField(node.Operand), QueryStringOp.CompareOrdinal, (byte)compare, node.Argument, label,
            jumpIfTrue);
    }

    void EmitStringOp(RelationQueryField field, QueryStringOp operation, byte argument, object? constant, uint label,
        bool jumpIfTrue)
    {
        _builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfStringOp : OpCode.JmpIfNotStringOp);
        _builder.AddUInt8((byte)operation);
        _builder.AddUInt8(argument);
        _builder.AddUInt16(field.CharsObject);
        _builder.AddUInt16(field.LengthSlot);
        _builder.AddUInt32((uint)_builder.AddObjectConstant(constant));
        _builder.AddLabelTarget(label);
    }

    void EmitStringMethod(StringMethodQueryNode node, uint label, bool jumpIfTrue)
    {
        var field = StringField(node.Operand);
        var encoding = _encodings[field.Name];
        if (UsesEncodedStartsWith(node))
        {
            var prefix = (string)node.Argument!;
            var writer = new MemWriter();
            int skip;
            if (encoding == QueryStringEncoding.String)
            {
                writer.WriteString(prefix);
                skip = (int)PackUnpack.LengthVUInt((uint)prefix.Length + 1u);
            }
            else
            {
                writer.WriteStringOrderedPrefix(prefix);
                skip = 0;
            }

            var bytes = writer.GetSpan()[skip..];
            _builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfEncodedStartsWith : OpCode.JmpIfNotEncodedStartsWith);
            _builder.AddUInt8((byte)encoding);
            _builder.AddUInt16(field.Slot);
            _builder.AddUInt32(_builder.AddConstantBytes(bytes));
            _builder.AddUInt32((uint)bytes.Length);
            _builder.AddLabelTarget(label);
            return;
        }

        if (UsesEncodedEquals(node))
        {
            EmitEncodedEquality(field, (string?)node.Argument, jumpIfTrue, label);
            return;
        }

        EmitStringOp(field, node.Operation, (byte)node.Comparison, node.Argument, label, jumpIfTrue);
    }

    // Encodings of both string formats keep a prefix property, except that a trailing high surrogate of the prefix
    // could be combined with the next char into one code point.
    static bool UsesEncodedStartsWith(StringMethodQueryNode node)
    {
        return node is { Operation: QueryStringOp.StartsWith, Comparison: StringComparison.Ordinal, Argument: string s } &&
               (s.Length == 0 || !char.IsHighSurrogate(s[^1]));
    }

    static bool UsesEncodedEquals(StringMethodQueryNode node)
    {
        return node is { Operation: QueryStringOp.Equals, Comparison: StringComparison.Ordinal } &&
               node.Argument is null or string;
    }

    void EmitIsNullOrEmpty(StringIsNullOrEmptyQueryNode node, uint label, bool jumpIfTrue)
    {
        var field = StringField(node.Operand);
        var length = field.LengthSlot;
        if (_encodings[field.Name] == QueryStringEncoding.String)
        {
            length = AllocFrame(8);
            _builder.AddInstruction(OpCode.StringLength);
            _builder.AddUInt16(field.Slot);
            _builder.AddUInt16(length);
        }

        EmitJmpIfFrameConst(jumpIfTrue, QueryValueType.Int32, QueryCompare.LessOrEqual, length,
            AddFrameConstant(typeof(int), 0), label);
    }

    void EmitInSet(InSetQueryNode node, uint label, bool jumpIfTrue)
    {
        if (node.Set is QueryStringSet)
        {
            EmitStringOp(StringField(node.Operand), QueryStringOp.InSet, 0, node.Set, label, jumpIfTrue);
            return;
        }

        var operand = CompileValue(node.Operand);
        if (operand.Kind != QueryOperandKind.Frame || operand.HasValue != QueryHasValue.Always)
            throw new NotSupportedException("Relation query supports Contains only on non-nullable values.");
        _builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfFrameInSet : OpCode.JmpIfNotFrameInSet);
        _builder.AddUInt8((byte)operand.ValueType);
        _builder.AddUInt16((ushort)operand.Offset);
        _builder.AddUInt32((uint)_builder.AddObjectConstant(node.Set));
        _builder.AddLabelTarget(label);
    }

    void CollectNeedsChars(QueryNode node, HashSet<string> names)
    {
        switch (node)
        {
            case StringMethodQueryNode stringMethod when !UsesEncodedStartsWith(stringMethod) &&
                                                         !UsesEncodedEquals(stringMethod):
            case StringCompareOrdinalQueryNode:
            case InSetQueryNode { Set: QueryStringSet }:
                AddNeedsChars(node, names);
                break;
            case StringIsNullOrEmptyQueryNode or StringLengthQueryNode:
            {
                var operand = node is StringIsNullOrEmptyQueryNode n ? n.Operand : ((StringLengthQueryNode)node).Operand;
                if (operand is PropQueryNode prop && _encodings.TryGetValue(prop.Name, out var encoding) &&
                    encoding == QueryStringEncoding.StringOrdered)
                    names.Add(prop.Name);
                break;
            }
        }

        ForEachChild(node, child => CollectNeedsChars(child, names));
    }

    static void AddNeedsChars(QueryNode node, HashSet<string> names)
    {
        var operand = node switch
        {
            StringMethodQueryNode n => n.Operand,
            StringCompareOrdinalQueryNode n => n.Operand,
            InSetQueryNode n => n.Operand,
            _ => null
        };
        if (operand is PropQueryNode prop) names.Add(prop.Name);
    }

    uint AddFrameConstant(Type storage, object value)
    {
        value = RelationQueryTypes.ToStorage(value, storage);
        if (storage == typeof(bool)) return Add((bool)value);
        if (storage == typeof(byte)) return Add((byte)value);
        if (storage == typeof(sbyte)) return Add((sbyte)value);
        if (storage == typeof(ushort)) return Add((ushort)value);
        if (storage == typeof(short)) return Add((short)value);
        if (storage == typeof(uint)) return Add((uint)value);
        if (storage == typeof(int)) return Add((int)value);
        if (storage == typeof(ulong)) return Add((ulong)value);
        if (storage == typeof(long)) return Add((long)value);
        if (storage == typeof(Half)) return Add((Half)value);
        if (storage == typeof(float)) return Add((float)value);
        if (storage == typeof(double)) return Add((double)value);
        if (storage == typeof(DateTime)) return Add((DateTime)value);
        if (storage == typeof(Guid)) return Add((Guid)value);
        if (storage == typeof(decimal)) return Add((decimal)value);
        if (storage == typeof(TimeSpan)) return Add((TimeSpan)value);
        if (storage == typeof(DateTimeOffset)) return Add((DateTimeOffset)value);
        throw new NotSupportedException($"Relation query constants of type {storage.ToSimpleName()} are not supported yet.");

        uint Add<TValue>(TValue constant) where TValue : unmanaged
        {
            var offset = _builder.AllocAlignedConst((uint)Unsafe.SizeOf<TValue>());
            _builder.ConstSpan<TValue>(offset) = constant;
            return offset;
        }
    }

    static bool TrySplitPropertyConstant(CompareQueryNode compare, out PropQueryNode prop,
        out ConstantQueryNode constant)
    {
        if (compare.Left is PropQueryNode leftProp && compare.Right is ConstantQueryNode rightConstant)
        {
            prop = leftProp;
            constant = rightConstant;
            return true;
        }

        if (compare.Right is PropQueryNode rightProp && compare.Left is ConstantQueryNode leftConstant)
        {
            prop = rightProp;
            constant = leftConstant;
            return true;
        }

        prop = null!;
        constant = null!;
        return false;
    }

    // Equal values must have equal key bytes to be found by a key seek. C# equality of decimal ignores scale, of
    // DateTimeOffset the offset, of DateTime the Kind, and of floating point numbers treats 0 and -0 as equal and NaN as
    // unequal, while their stored keys keep those differences, so such conditions are tested on every row instead.
    static bool HasValueEqualityOfKeyBytes(Type type)
    {
        return type != typeof(decimal) && type != typeof(DateTimeOffset) && type != typeof(DateTime) &&
               type != typeof(double) && type != typeof(float) && type != typeof(Half);
    }

    static bool CanSaveConstant(Type type)
    {
        return type == typeof(string) || type == typeof(bool) || type == typeof(byte) || type == typeof(sbyte) ||
               type == typeof(ushort) || type == typeof(short) || type == typeof(uint) || type == typeof(int) ||
               type == typeof(ulong) || type == typeof(long) || type == typeof(Half) || type == typeof(float) ||
               type == typeof(double) || type == typeof(DateTime) || type == typeof(Guid) ||
               type == typeof(decimal) || type == typeof(TimeSpan) || type == typeof(DateTimeOffset);
    }

    internal static void SaveConstant(FieldHandlerSave saver, ref MemWriter writer, IWriterCtx? writerCtx, Type type,
        object? value)
    {
        if (type == typeof(string))
        {
            var stringValue = (string?)value;
            saver(ref writer, writerCtx, ref Unsafe.As<string?, byte>(ref stringValue));
            return;
        }

        Int128 storage = 0;
        if (type == typeof(bool)) Unsafe.As<Int128, bool>(ref storage) = (bool)value!;
        else if (type == typeof(byte)) Unsafe.As<Int128, byte>(ref storage) = (byte)value!;
        else if (type == typeof(sbyte)) Unsafe.As<Int128, sbyte>(ref storage) = (sbyte)value!;
        else if (type == typeof(ushort)) Unsafe.As<Int128, ushort>(ref storage) = (ushort)value!;
        else if (type == typeof(short)) Unsafe.As<Int128, short>(ref storage) = (short)value!;
        else if (type == typeof(uint)) Unsafe.As<Int128, uint>(ref storage) = (uint)value!;
        else if (type == typeof(int)) Unsafe.As<Int128, int>(ref storage) = (int)value!;
        else if (type == typeof(ulong)) Unsafe.As<Int128, ulong>(ref storage) = (ulong)value!;
        else if (type == typeof(long)) Unsafe.As<Int128, long>(ref storage) = (long)value!;
        else if (type == typeof(Half)) Unsafe.As<Int128, Half>(ref storage) = (Half)value!;
        else if (type == typeof(float)) Unsafe.As<Int128, float>(ref storage) = (float)value!;
        else if (type == typeof(double)) Unsafe.As<Int128, double>(ref storage) = (double)value!;
        else if (type == typeof(DateTime)) Unsafe.As<Int128, DateTime>(ref storage) = (DateTime)value!;
        else if (type == typeof(Guid)) Unsafe.As<Int128, Guid>(ref storage) = (Guid)value!;
        else if (type == typeof(decimal)) Unsafe.As<Int128, decimal>(ref storage) = (decimal)value!;
        else if (type == typeof(TimeSpan)) Unsafe.As<Int128, TimeSpan>(ref storage) = (TimeSpan)value!;
        else if (type == typeof(DateTimeOffset)) Unsafe.As<Int128, DateTimeOffset>(ref storage) = (DateTimeOffset)value!;
        else throw new NotSupportedException(
            $"Relation query cursor prefix does not support {type.ToSimpleName()} constants yet.");
        saver(ref writer, writerCtx, ref Unsafe.As<Int128, byte>(ref storage));
    }

    static QueryNode? ExtractPredicate(QueryNode node)
    {
        return node switch
        {
            QueryByIdQueryNode => null,
            WhereQueryNode where => ExtractPredicate(where.Source) is { } source
                ? new BoolAndQueryNode(source, where.Predicate)
                : where.Predicate,
            _ => throw new NotSupportedException("Relation query source is not supported.")
        };
    }

    // Splits top-level conjunctions, also out of !(a || b) by De Morgan, which keeps evaluation order.
    static void FlattenConjuncts(QueryNode node, List<QueryNode> conjuncts)
    {
        switch (node)
        {
            case BoolAndQueryNode and:
                FlattenConjuncts(and.Left, conjuncts);
                FlattenConjuncts(and.Right, conjuncts);
                return;
            case NotQueryNode { Operand: BoolOrQueryNode or }:
                FlattenConjuncts(Simplify(new NotQueryNode(or.Left)), conjuncts);
                FlattenConjuncts(Simplify(new NotQueryNode(or.Right)), conjuncts);
                return;
            case NotQueryNode { Operand: NotQueryNode inner }:
                FlattenConjuncts(inner.Operand, conjuncts);
                return;
            default:
                conjuncts.Add(node);
                return;
        }
    }

    static bool IsTrue(QueryNode node) => node is ConstantQueryNode { Value: true };
    static bool IsFalse(QueryNode node) => node is ConstantQueryNode { Value: false };

    static ConstantQueryNode Constant(bool value) => new(value, typeof(bool));

    static bool IsIntegerOrDecimal(Type type)
    {
        RelationQueryTypes.TryGetStorage(type, out var storage, out _, out _);
        return RelationQueryTypes.IsInteger(storage) || storage == typeof(decimal);
    }

    public static bool CanThrowNode(QueryNode node) => CanThrow(node);

    /// Whether evaluating the node can throw, which limits reordering it with other conjuncts.
    static bool CanThrow(QueryNode node)
    {
        var throws = node switch
        {
            ThrowQueryNode => true,
            StringMethodQueryNode n => n.Operation is not QueryStringOp.Equals,
            StringLengthQueryNode or NullableValueQueryNode => true,
            ConvertQueryNode n => n.IsChecked || Nullable.GetUnderlyingType(n.Type) == null &&
                Nullable.GetUnderlyingType(n.Operand.Type) != null ||
                n.Type == typeof(decimal) || n.Operand.Type == typeof(decimal) ||
                Nullable.GetUnderlyingType(n.Type) == typeof(decimal) ||
                Nullable.GetUnderlyingType(n.Operand.Type) == typeof(decimal),
            ArithmeticQueryNode n => n.Operation is QueryArithmetic.AddChecked or QueryArithmetic.SubtractChecked
                                         or QueryArithmetic.MultiplyChecked or QueryArithmetic.NegateChecked ||
                                     n.Operation is QueryArithmetic.Divide or QueryArithmetic.Modulo &&
                                     IsIntegerOrDecimal(n.Type) ||
                                     IsDecimal(n.Type),
            _ => false
        };
        if (throws) return true;
        ForEachChild(node, child => throws |= CanThrow(child));
        return throws;
    }

    static bool IsDecimal(Type type) => (Nullable.GetUnderlyingType(type) ?? type) == typeof(decimal);

    static QueryNode Simplify(QueryNode node)
    {
        switch (node)
        {
            case BoolAndQueryNode and:
            {
                var left = Simplify(and.Left);
                var right = Simplify(and.Right);
                if (IsFalse(left)) return left;
                if (IsTrue(left)) return right;
                if (IsTrue(right)) return left;
                if (IsFalse(right) && !CanThrow(left)) return right;
                return new BoolAndQueryNode(left, right);
            }
            case BoolOrQueryNode or:
            {
                var left = Simplify(or.Left);
                var right = Simplify(or.Right);
                if (IsTrue(left)) return left;
                if (IsFalse(left)) return right;
                if (IsFalse(right)) return left;
                if (IsTrue(right) && !CanThrow(left)) return right;
                return new BoolOrQueryNode(left, right);
            }
            case NotQueryNode not:
            {
                var operand = Simplify(not.Operand);
                if (operand is ConstantQueryNode { Value: bool value }) return Constant(!value);
                if (operand is NotQueryNode inner) return inner.Operand;
                return new NotQueryNode(operand);
            }
            case CompareQueryNode compare:
            {
                var left = Simplify(compare.Left);
                var right = Simplify(compare.Right);
                if (left is ConstantQueryNode leftConstant && right is ConstantQueryNode rightConstant)
                    return Constant(CompareConstants(leftConstant.Value, rightConstant.Value, compare.Compare));
                return new CompareQueryNode(left, right, compare.Compare);
            }
            case ConvertQueryNode convert:
            {
                var operand = Simplify(convert.Operand);
                if (operand is not ConstantQueryNode constant) return new ConvertQueryNode(operand, convert.Type, convert.IsChecked);
                if (constant.Value == null && Nullable.GetUnderlyingType(convert.Type) == null)
                    return new ThrowQueryNode(QueryException.NullableHasNoValue, convert.Type);
                return new ConstantQueryNode(RelationQueryAstBuilder.ConvertConstant(constant.Value, convert.Type),
                    convert.Type);
            }
            case ArithmeticQueryNode arithmetic:
                return new ArithmeticQueryNode(Simplify(arithmetic.Left),
                    arithmetic.Right == null ? null : Simplify(arithmetic.Right), arithmetic.Operation, arithmetic.Type);
            case HasValueQueryNode hasValue:
            {
                var operand = Simplify(hasValue.Operand);
                return operand is ConstantQueryNode constant
                    ? Constant(constant.Value != null)
                    : new HasValueQueryNode(operand);
            }
            case NullableValueQueryNode nullableValue:
            {
                var operand = Simplify(nullableValue.Operand);
                if (operand is not ConstantQueryNode constant) return new NullableValueQueryNode(operand);
                return constant.Value == null
                    ? new ThrowQueryNode(QueryException.NullableHasNoValue, nullableValue.Type)
                    : new ConstantQueryNode(constant.Value, nullableValue.Type);
            }
            case StringLengthQueryNode stringLength:
            {
                var operand = Simplify(stringLength.Operand);
                if (operand is not ConstantQueryNode constant) return new StringLengthQueryNode(operand);
                return constant.Value is string s
                    ? new ConstantQueryNode(s.Length, typeof(int))
                    : new ThrowQueryNode(QueryException.NullReference, typeof(int));
            }
            case StringMethodQueryNode stringMethod:
            {
                var operand = Simplify(stringMethod.Operand);
                if (operand is not ConstantQueryNode constant)
                    return new StringMethodQueryNode(stringMethod.Operation, stringMethod.Comparison, operand,
                        stringMethod.Argument);
                var value = (string?)constant.Value;
                if (value == null && stringMethod.Operation != QueryStringOp.Equals)
                    return new ThrowQueryNode(QueryException.NullReference, typeof(bool));
                return Constant(EvaluateStringMethod(stringMethod, value));
            }
            case StringCompareOrdinalQueryNode compareOrdinal:
            {
                var operand = Simplify(compareOrdinal.Operand);
                if (operand is not ConstantQueryNode constant)
                    return new StringCompareOrdinalQueryNode(operand, compareOrdinal.Argument, compareOrdinal.Reversed);
                var order = Math.Sign(string.CompareOrdinal((string?)constant.Value, compareOrdinal.Argument));
                return new ConstantQueryNode(compareOrdinal.Reversed ? -order : order, typeof(int));
            }
            case StringIsNullOrEmptyQueryNode isNullOrEmpty:
            {
                var operand = Simplify(isNullOrEmpty.Operand);
                return operand is ConstantQueryNode constant
                    ? Constant(string.IsNullOrEmpty((string?)constant.Value))
                    : new StringIsNullOrEmptyQueryNode(operand);
            }
            case InSetQueryNode inSet:
            {
                var operand = Simplify(inSet.Operand);
                if (operand is not ConstantQueryNode constant) return new InSetQueryNode(operand, inSet.Set);
                if (inSet.Set is QueryStringSet stringSet)
                    return Constant(constant.Value is string s ? stringSet.Lookup.Contains(s) : stringSet.ContainsNull);
                RelationQueryTypes.TryGetStorage(operand.Type, out var storage, out _, out _);
                var item = RelationQueryTypes.ToStorage(constant.Value!, storage);
                foreach (var setItem in (IEnumerable)inSet.Set)
                    if (Equals(setItem, item)) return Constant(true);
                return Constant(false);
            }
            default:
                return node;
        }
    }

    static bool EvaluateStringMethod(StringMethodQueryNode node, string? value)
    {
        var argument = node.Argument;
        return node.Operation switch
        {
            QueryStringOp.Equals => string.Equals(value, (string?)argument, node.Comparison),
            QueryStringOp.InstanceEquals => value!.Equals((string?)argument, node.Comparison),
            QueryStringOp.StartsWith => argument is char c ? value!.StartsWith(c) : value!.StartsWith((string)argument!, node.Comparison),
            QueryStringOp.EndsWith => argument is char c ? value!.EndsWith(c) : value!.EndsWith((string)argument!, node.Comparison),
            QueryStringOp.Contains => argument is char c ? value!.Contains(c) : value!.Contains((string)argument!, node.Comparison),
            _ => throw new NotSupportedException("Relation query string operation is not supported.")
        };
    }

    // C# semantics: null equals only null, ordering with null is false, and NaN is unordered.
    static bool CompareConstants(object? left, object? right, QueryCompare compare)
    {
        if (left == null || right == null)
        {
            return compare switch
            {
                QueryCompare.Equal => left == null && right == null,
                QueryCompare.NotEqual => !(left == null && right == null),
                _ => false
            };
        }

        if (left is double or float or Half && (IsNaN(left) || IsNaN(right)))
            return compare == QueryCompare.NotEqual;
        if (compare is QueryCompare.Equal or QueryCompare.NotEqual)
            return Equals(left, right) == (compare == QueryCompare.Equal);
        var order = ((IComparable)left).CompareTo(right);
        return compare switch
        {
            QueryCompare.Less => order < 0,
            QueryCompare.LessOrEqual => order <= 0,
            QueryCompare.Greater => order > 0,
            _ => order >= 0
        };
    }

    static bool IsNaN(object value)
    {
        return value switch
        {
            double d => double.IsNaN(d),
            float f => float.IsNaN(f),
            Half h => Half.IsNaN(h),
            _ => false
        };
    }

    QueryNode SubstituteMissing(QueryNode node, ReadOnlySpan<TableFieldInfo> valueFields)
    {
        if (node is PropQueryNode prop)
        {
            if (IsPrimaryKeyField(prop.Name)) return prop;
            foreach (var field in valueFields)
                if (field.Name == prop.Name && !field.Computed) return prop;
            return new ConstantQueryNode(RelationQueryTypes.DefaultValue(prop.Type), prop.Type);
        }

        var fields = valueFields.ToArray();
        return MapChildren(node, child => SubstituteMissing(child, fields));
    }

    static QueryNode MapChildren(QueryNode node, Func<QueryNode, QueryNode> map)
    {
        return node switch
        {
            BoolAndQueryNode n => new BoolAndQueryNode(map(n.Left), map(n.Right)),
            BoolOrQueryNode n => new BoolOrQueryNode(map(n.Left), map(n.Right)),
            NotQueryNode n => new NotQueryNode(map(n.Operand)),
            CompareQueryNode n => new CompareQueryNode(map(n.Left), map(n.Right), n.Compare),
            ConvertQueryNode n => new ConvertQueryNode(map(n.Operand), n.Type, n.IsChecked),
            ArithmeticQueryNode n => new ArithmeticQueryNode(map(n.Left), n.Right == null ? null : map(n.Right),
                n.Operation, n.Type),
            HasValueQueryNode n => new HasValueQueryNode(map(n.Operand)),
            NullableValueQueryNode n => new NullableValueQueryNode(map(n.Operand)),
            StringLengthQueryNode n => new StringLengthQueryNode(map(n.Operand)),
            StringMethodQueryNode n => new StringMethodQueryNode(n.Operation, n.Comparison, map(n.Operand), n.Argument),
            StringCompareOrdinalQueryNode n => new StringCompareOrdinalQueryNode(map(n.Operand), n.Argument, n.Reversed),
            StringIsNullOrEmptyQueryNode n => new StringIsNullOrEmptyQueryNode(map(n.Operand)),
            InSetQueryNode n => new InSetQueryNode(map(n.Operand), n.Set),
            _ => node
        };
    }

    static void ForEachChild(QueryNode node, Action<QueryNode> action)
    {
        MapChildren(node, child =>
        {
            action(child);
            return child;
        });
    }

    bool IsPrimaryKeyField(string name)
    {
        foreach (var field in _primaryKeyFields.Span)
            if (field.Name == name) return true;
        return false;
    }

    static bool UsesOnlyFields(QueryNode node, TableFieldInfo[] fields)
    {
        var names = new HashSet<string>();
        CollectPropertyNames(node, names);
        foreach (var name in names)
            if (Array.FindIndex(fields, f => f.Name == name) < 0) return false;
        return true;
    }

    static void CollectPropertyNames(QueryNode node, HashSet<string> names)
    {
        if (node is PropQueryNode prop) names.Add(prop.Name);
        ForEachChild(node, child => CollectPropertyNames(child, names));
    }
}

/// Moves a cursor over rows matching a compiled plan, testing rows on their stored bytes. Rows come from the plan key
/// ranges, or from secondary index keys whose primary rows it looks up.
sealed class RelationQueryScanner : IDisposable
{
    const int ValueBufferSize = 4096;

    readonly IInternalObjectDBTransaction _transaction;
    readonly CursorIterateCallback _stopAtMatch;
    readonly CursorIterateCallback _aggregateMatches;
    readonly CursorIterateCallback _countKeys;
    readonly CursorIterateCallback _collectCandidates;
    readonly CursorIterateCallback _aggregateIndex;
    readonly long[] _frame;
    readonly object?[] _objects;
    // Rented from the shared pool and returned on Dispose, so short queries do not allocate it.
    byte[]? _valueBuffer;
    IKeyValueDBCursor? _cursor;
    IKeyValueDBCursor? _indexCursor;
    RelationQueryAggregator? _aggregator;
    RelationQueryRange? _range;
    int _rangeIndex;
    bool _inRange;
    bool _first = true;
    bool _afterEnd;
    bool _matched;
    bool _skipCurrentRow;
    bool _stepping;
    int _examined;

    // Ranges of this execution's parameter values, and the length of the key prefix they all share.
    readonly RelationQueryRange[] _ranges;
    readonly int _commonPrefixLength;

    int KeyStart(RelationQueryProgram program) =>
        program.KeyStartFromRanges ? _commonPrefixLength : program.KeyStart;
    List<byte[]>? _candidates;

    // A match within this many tested rows switches to stepping; this many rejected rows in a row switch back.
    const int StepAfterExamined = 2;
    const int StepRejectLimit = 8;
    int _candidateIndex;

    public RelationQueryScanner(IInternalObjectDBTransaction transaction, RelationQueryPlan plan,
        object?[]? parameterValues = null)
    {
        _transaction = transaction;
        Plan = plan;
        _stopAtMatch = StopAtMatch;
        _aggregateMatches = AggregateMatches;
        _countKeys = CountKeys;
        _collectCandidates = CollectCandidate;
        _aggregateIndex = AggregateIndex;
        _frame = new long[plan.FrameSize / sizeof(long)];
        _objects = new object?[plan.ObjectCount];
        if (plan.OneSlot >= 0) FrameBytes[plan.OneSlot] = 1;
        foreach (var (index, slot, storage) in plan.Parameters)
            RelationQueryTypes.WriteStorage(FrameBytes[slot..], storage, parameterValues![index]!);
        _ranges = plan.RangeBuilder?.Build(parameterValues, out _commonPrefixLength) ?? plan.Ranges;
        if (plan.ValuePrograms != null || plan.ProjectionValuePrograms != null)
            _valueBuffer = ArrayPool<byte>.Shared.Rent(ValueBufferSize);
        _cursor = transaction.KeyValueDBTransaction.CreateCursor();
    }

    /// Runs the projection programs on the row the cursor is positioned on.
    [SkipLocalsInit]
    public unsafe void ComputeProjection()
    {
        Span<byte> keyBuffer = stackalloc byte[512];
        var key = _cursor!.GetKeySpan(ref keyBuffer);
        ref var frame = ref Unsafe.As<long, byte>(ref MemoryMarshal.GetArrayDataReference(_frame));
        ref var objects = ref MemoryMarshal.GetArrayDataReference(_objects);
        var program = Plan.ProjectionKeyProgram;
        var valueReader = new MemReader();
        fixed (byte* keyPointer = key)
        {
            if (program == null)
            {
                Span<byte> valueBuffer = _valueBuffer;
                var value = _cursor.GetValueSpan(ref valueBuffer);
                fixed (byte* valuePointer = value)
                {
                    valueReader = new(valuePointer, value.Length);
                    var version = valueReader.ReadVUInt32();
                    program = version < Plan.ProjectionValuePrograms!.Length ? Plan.ProjectionValuePrograms[version] : null;
                    if (program == null) throw new BTDBException($"Unknown relation value version {version}.");
                    Run(program, keyPointer, key.Length, ref valueReader, ref frame, ref objects);
                }

                return;
            }

            Run(program, keyPointer, key.Length, ref valueReader, ref frame, ref objects);
        }

        static void Run(RelationQueryProgram program, byte* keyPointer, int keyLength, ref MemReader valueReader,
            ref byte frame, ref object? objects)
        {
            if (program.Program == null) return;
            // Projection programs read keys from the relation prefix, which never depends on parameters.
            var keyReader = new MemReader(keyPointer + program.KeyStart, keyLength - program.KeyStart);
            var ctx = new InterpreterCtx(program.Program, ref frame, ref objects, ref keyReader, ref valueReader);
            BTDB.Interpreter.Interpreter.Run(ref ctx);
        }
    }

    public RelationQueryPlan Plan { get; }

    /// Positioned on the matching primary row after MoveNext or MoveLast returned true.
    public IKeyValueDBCursor Cursor
    {
        get
        {
            ObjectDisposedException.ThrowIf(_cursor == null, this);
            return _cursor;
        }
    }

    IKeyValueDBCursor IndexCursor => _indexCursor ??= _transaction.KeyValueDBTransaction.CreateCursor();

    /// Frame of the current row, with its selector value when the plan has one.
    public Span<byte> FrameBytes => MemoryMarshal.AsBytes(_frame.AsSpan());

    public object? ReadResult(RelationQueryResult result) => result.Read(FrameBytes, _objects);

    void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_cursor == null, this);
        _transaction.ThrowIfDisposed();
    }

    public bool MoveNext()
    {
        ThrowIfDisposed();
        if (_afterEnd) return false;
        if (Plan.SecondaryKey == null) return MoveNextInRanges() || AfterEnd();
        _candidates ??= CollectCandidates();
        while (_candidateIndex < _candidates.Count)
        {
            var primaryKey = _candidates[_candidateIndex++];
            if (_cursor!.FindExactKey(primaryKey) && Evaluate(primaryKey)) return true;
        }

        return AfterEnd();
    }

    [SkipLocalsInit]
    bool MoveNextInRanges()
    {
        var cursor = _cursor!;
        Span<byte> keyBuffer = stackalloc byte[512];
        if (_first)
        {
            _first = false;
            _rangeIndex = 0;
            _inRange = false;
        }

        while (_rangeIndex < _ranges.Length)
        {
            var range = _ranges[_rangeIndex];
            if (!_inRange)
            {
                if (!Seek(cursor, range))
                {
                    _rangeIndex++;
                    continue;
                }

                _inRange = true;
                _skipCurrentRow = false;
            }
            else if (Plan.HasPrograms && !_stepping && cursor.IsValid())
            {
                // FastIterate continues from the previously matched row, which is cheaper than moving the cursor;
                // that row itself is skipped.
                _skipCurrentRow = true;
            }
            else
            {
                // Without programs, when stepping, or when the previously matched row was removed, FindNextKey moves
                // on.
                if (!cursor.FindNextKey(range.Prefix))
                {
                    NextRange();
                    continue;
                }

                _skipCurrentRow = false;
            }

            if (!Plan.HasPrograms)
            {
                // The cursor stays within the prefix, so only bounds need checking.
                if (range.IsWholePrefix || range.Contains(cursor.GetKeySpan(ref keyBuffer))) return true;
                NextRange();
                continue;
            }

            if (_stepping)
            {
                // When most rows match, restarting FastIterate for every match costs more than stepping the cursor;
                // a run of rejected rows switches back.
                var rejected = 0;
                var leftRange = false;
                while (true)
                {
                    var key = cursor.GetKeySpan(ref keyBuffer);
                    if (!range.IsWholePrefix && !range.Contains(key))
                    {
                        leftRange = true;
                        break;
                    }

                    if (Evaluate(key)) return true;
                    if (++rejected == StepRejectLimit) break;
                    if (cursor.FindNextKey(range.Prefix)) continue;
                    leftRange = true;
                    break;
                }

                if (leftRange)
                {
                    NextRange();
                    continue;
                }

                _stepping = false;
                _skipCurrentRow = true;
            }

            // Tests rows starting at the cursor position without moving the cursor per row; iteration stops on the
            // first matching row or the first key outside the range.
            _matched = false;
            _examined = 0;
            _range = range;
            if (Plan.ValuePrograms == null)
                cursor.FastIterateNoCursor(ref keyBuffer, _stopAtMatch);
            else
                cursor.FastIterate(ref keyBuffer, _stopAtMatch);
            if (_matched)
            {
                _stepping = _examined <= StepAfterExamined;
                return true;
            }

            NextRange();
        }

        return false;
    }

    void NextRange()
    {
        _rangeIndex++;
        _inRange = false;
    }

    /// Positions the cursor on the first key of the range.
    static bool Seek(IKeyValueDBCursor cursor, RelationQueryRange range)
    {
        if (range.Start.Length == range.Prefix.Length) return cursor.FindFirstKey(range.Prefix);
        switch (cursor.Find(range.Start, (uint)range.Prefix.Length))
        {
            case FindResult.Exact:
            case FindResult.Next:
                break;
            case FindResult.Previous:
                if (!cursor.FindNextKey(range.Prefix)) return false;
                break;
            default:
                return false;
        }

        if (range.ExcludeStart && cursor.KeyHasPrefix(range.Start))
        {
            cursor.FindLastKey(range.Start);
            if (!cursor.FindNextKey(range.Prefix)) return false;
        }

        return true;
    }

    /// Positions the cursor on the last key of the range.
    static bool SeekLast(IKeyValueDBCursor cursor, RelationQueryRange range)
    {
        if (range.Stop == null) return cursor.FindLastKey(range.Prefix);
        byte[] stop = [..range.Prefix, ..range.Stop];
        if (range.StopInclusive && cursor.FindLastKey(stop)) return true;
        return cursor.Find(stop, (uint)range.Prefix.Length) switch
        {
            FindResult.Previous => true,
            FindResult.Exact or FindResult.Next => cursor.FindPreviousKey(range.Prefix),
            _ => false
        };
    }

    bool StopAtMatch(long keyIndex, ReadOnlySpan<byte> key)
    {
        if (_skipCurrentRow)
        {
            _skipCurrentRow = false;
            return false;
        }

        if (!_range!.Contains(key)) return true;
        _examined++;
        if (!Evaluate(key)) return false;
        _matched = true;
        return true;
    }

    /// Moves to the previous matching row in descending key order, testing rows backwards from the end.
    [SkipLocalsInit]
    public bool MovePrevious()
    {
        ThrowIfDisposed();
        if (_afterEnd) return false;
        if (Plan.SecondaryKey != null)
        {
            _candidates ??= CollectCandidates();
            if (_first)
            {
                _first = false;
                _candidateIndex = _candidates.Count;
            }

            while (_candidateIndex > 0)
            {
                var primaryKey = _candidates[--_candidateIndex];
                if (_cursor!.FindExactKey(primaryKey) && Evaluate(primaryKey)) return true;
            }

            return AfterEnd();
        }

        var cursor = _cursor!;
        Span<byte> keyBuffer = stackalloc byte[512];
        if (_first)
        {
            _first = false;
            _rangeIndex = _ranges.Length - 1;
            _inRange = false;
        }

        while (_rangeIndex >= 0)
        {
            var range = _ranges[_rangeIndex];
            if (!(_inRange ? cursor.FindPreviousKey(range.Prefix) : SeekLast(cursor, range)))
            {
                _rangeIndex--;
                _inRange = false;
                continue;
            }

            _inRange = true;
            var key = cursor.GetKeySpan(ref keyBuffer);
            if (!range.Contains(key))
            {
                _rangeIndex--;
                _inRange = false;
                continue;
            }

            if (!Plan.HasPrograms || Evaluate(key)) return true;
        }

        return AfterEnd();
    }

    /// Skips up to count rows before the first MoveNext using key indexes, which is possible when rows are whole
    /// primary key prefix ranges without programs; returns the number of skipped rows.
    public long SkipKeys(long count)
    {
        ThrowIfDisposed();
        if (count <= 0 || !_first || Plan.SecondaryKey != null || Plan.HasPrograms ||
            !Array.TrueForAll(_ranges, r => r.IsWholePrefix))
            return 0;
        var cursor = _cursor!;
        _first = false;
        var skipped = 0L;
        for (_rangeIndex = 0; _rangeIndex < _ranges.Length; _rangeIndex++)
        {
            if (skipped == count) break;
            var range = _ranges[_rangeIndex];
            if (!cursor.FindFirstKey(range.Prefix)) continue;
            var firstIndex = cursor.GetKeyIndex();
            cursor.FindLastKey(range.Prefix);
            var size = cursor.GetKeyIndex() - firstIndex + 1;
            var remaining = count - skipped;
            if (remaining < size)
            {
                // MoveNext continues with the key after the last skipped one.
                cursor.FindKeyIndex(range.Prefix, remaining - 1);
                _inRange = true;
                return count;
            }

            skipped += size;
        }

        _inRange = false;
        return skipped;
    }

    /// Positions the cursor on the last matching row by testing rows backwards from the end, like LINQ-to-Objects
    /// does for Last over a list, so only rows after the last match are tested.
    [SkipLocalsInit]
    public bool MoveLast()
    {
        ThrowIfDisposed();
        if (Plan.SecondaryKey != null)
        {
            _candidates ??= CollectCandidates();
            for (var i = _candidates.Count - 1; i >= 0; i--)
                if (_cursor!.FindExactKey(_candidates[i]) && Evaluate(_candidates[i]))
                    return true;
            return false;
        }

        var cursor = _cursor!;
        Span<byte> keyBuffer = stackalloc byte[512];
        for (var i = _ranges.Length - 1; i >= 0; i--)
        {
            var range = _ranges[i];
            if (!SeekLast(cursor, range)) continue;
            do
            {
                var key = cursor.GetKeySpan(ref keyBuffer);
                if (!range.Contains(key)) break;
                if (!Plan.HasPrograms || Evaluate(key)) return true;
            } while (cursor.FindPreviousKey(range.Prefix));
        }

        return false;
    }

    /// Counts matching rows without materializing them; whole prefix ranges without programs are a difference of key
    /// indexes.
    public long Count()
    {
        ThrowIfDisposed();
        var secondaryKey = Plan.SecondaryKey;
        var noPrograms = !Plan.HasPrograms && (secondaryKey == null || secondaryKey.IndexProgram == null);
        if (noPrograms && Array.TrueForAll(_ranges, r => r.IsWholePrefix))
        {
            var cursor = secondaryKey == null ? _cursor! : IndexCursor;
            var count = 0L;
            foreach (var range in _ranges)
            {
                if (!cursor.FindFirstKey(range.Prefix)) continue;
                var firstIndex = cursor.GetKeyIndex();
                cursor.FindLastKey(range.Prefix);
                count += cursor.GetKeyIndex() - firstIndex + 1;
            }

            return count;
        }

        var counter = new RelationQueryCounter();
        Aggregate(counter);
        return counter.Count;
    }

    /// Passes the frame of every matching row, with the selector value when the plan has one, to the aggregator.
    [SkipLocalsInit]
    public void Aggregate(RelationQueryAggregator aggregator)
    {
        ThrowIfDisposed();
        _aggregator = aggregator;
        try
        {
            var secondaryKey = Plan.SecondaryKey;
            Span<byte> keyBuffer = stackalloc byte[512];
            if (secondaryKey == null)
            {
                var cursor = _cursor!;
                foreach (var range in _ranges)
                {
                    if (!Seek(cursor, range)) continue;
                    _range = range;
                    if (!Plan.HasPrograms) cursor.FastIterateNoCursor(ref keyBuffer, _countKeys);
                    else if (Plan.ValuePrograms == null) cursor.FastIterateNoCursor(ref keyBuffer, _aggregateMatches);
                    else cursor.FastIterate(ref keyBuffer, _aggregateMatches);
                }

                return;
            }

            if (secondaryKey.SortCandidates)
            {
                foreach (var primaryKey in _candidates ??= CollectCandidates())
                    if (_cursor!.FindExactKey(primaryKey) && Evaluate(primaryKey))
                        aggregator.Add(FrameBytes);
                return;
            }

            // Order does not matter and nothing can throw, so index keys are processed in index order.
            var indexCursor = IndexCursor;
            foreach (var range in _ranges)
            {
                if (!Seek(indexCursor, range)) continue;
                _range = range;
                indexCursor.FastIterateNoCursor(ref keyBuffer, _aggregateIndex);
            }
        }
        finally
        {
            _aggregator = null;
        }
    }

    bool AggregateMatches(long keyIndex, ReadOnlySpan<byte> key)
    {
        if (!_range!.Contains(key)) return true;
        if (Evaluate(key)) _aggregator!.Add(FrameBytes);
        return false;
    }

    bool CountKeys(long keyIndex, ReadOnlySpan<byte> key)
    {
        if (!_range!.Contains(key)) return true;
        _aggregator!.Add(FrameBytes);
        return false;
    }

    [SkipLocalsInit]
    bool AggregateIndex(long keyIndex, ReadOnlySpan<byte> key)
    {
        if (!_range!.Contains(key)) return true;
        if (!EvaluateIndex(key)) return false;
        if (Plan.SecondaryKey!.Covering)
        {
            _aggregator!.Add(FrameBytes);
            return false;
        }

        var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[512]);
        var primaryKey = ToPrimaryKey(key, ref writer);
        if (_cursor!.FindExactKey(primaryKey) && Evaluate(primaryKey)) _aggregator!.Add(FrameBytes);
        return false;
    }

    // Primary keys of index keys in the ranges passing the index program, sorted when row order matters.
    [SkipLocalsInit]
    List<byte[]> CollectCandidates()
    {
        _candidates = [];
        var indexCursor = IndexCursor;
        Span<byte> keyBuffer = stackalloc byte[512];
        foreach (var range in _ranges)
        {
            if (!Seek(indexCursor, range)) continue;
            _range = range;
            indexCursor.FastIterateNoCursor(ref keyBuffer, _collectCandidates);
        }

        if (Plan.SecondaryKey!.SortCandidates)
            _candidates.Sort(static (a, b) => a.AsSpan().SequenceCompareTo(b));
        return _candidates;
    }

    [SkipLocalsInit]
    bool CollectCandidate(long keyIndex, ReadOnlySpan<byte> key)
    {
        if (!_range!.Contains(key)) return true;
        if (!EvaluateIndex(key)) return false;
        var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[512]);
        _candidates!.Add(ToPrimaryKey(key, ref writer).ToArray());
        return false;
    }

    unsafe ReadOnlySpan<byte> ToPrimaryKey(ReadOnlySpan<byte> indexKey, ref MemWriter writer)
    {
        var secondaryKey = Plan.SecondaryKey!;
        writer.WriteBlock(secondaryKey.PrimaryKeyPrefix);
        fixed (byte* pointer = indexKey)
        {
            var reader = new MemReader(pointer, indexKey.Length);
            secondaryKey.ToPrimaryKey(ref reader, ref writer);
        }

        return writer.GetSpan();
    }

    unsafe bool EvaluateIndex(ReadOnlySpan<byte> indexKey)
    {
        var program = Plan.SecondaryKey!.IndexProgram;
        if (program == null) return true;
        if (program.Program == null) return program.ConstantResult;
        ref var frame = ref Unsafe.As<long, byte>(ref MemoryMarshal.GetArrayDataReference(_frame));
        ref var objects = ref MemoryMarshal.GetArrayDataReference(_objects);
        fixed (byte* keyPointer = indexKey)
        {
            var keyReader = new MemReader(keyPointer + KeyStart(program), indexKey.Length - KeyStart(program));
            var valueReader = new MemReader();
            var ctx = new InterpreterCtx(program.Program, ref frame, ref objects, ref keyReader, ref valueReader);
            BTDB.Interpreter.Interpreter.Run(ref ctx);
            return ctx.BoolResult;
        }
    }

    // Tests the primary row the cursor is positioned on, whose key is passed.
    unsafe bool Evaluate(ReadOnlySpan<byte> key)
    {
        ref var frame = ref Unsafe.As<long, byte>(ref MemoryMarshal.GetArrayDataReference(_frame));
        ref var objects = ref MemoryMarshal.GetArrayDataReference(_objects);
        var keyReader = new MemReader();
        var valueReader = new MemReader();
        fixed (byte* keyPointer = key)
        {
            var keyProgram = Plan.KeyProgram;
            if (keyProgram != null)
            {
                if (keyProgram.Program == null)
                {
                    if (!keyProgram.ConstantResult) return false;
                }
                else
                {
                    keyReader = new(keyPointer + KeyStart(keyProgram), key.Length - KeyStart(keyProgram));
                    var ctx = new InterpreterCtx(keyProgram.Program, ref frame, ref objects, ref keyReader,
                        ref valueReader);
                    BTDB.Interpreter.Interpreter.Run(ref ctx);
                    if (!ctx.BoolResult) return false;
                }
            }

            var valuePrograms = Plan.ValuePrograms;
            if (valuePrograms == null) return true;
            Span<byte> valueBuffer = _valueBuffer;
            var value = _cursor!.GetValueSpan(ref valueBuffer);
            fixed (byte* valuePointer = value)
            {
                valueReader = new(valuePointer, value.Length);
                var version = valueReader.ReadVUInt32();
                if (version >= (uint)valuePrograms.Length) return false;
                var program = valuePrograms[version];
                if (program == null) return false;
                if (program.Program == null) return program.ConstantResult;
                keyReader = new(keyPointer + KeyStart(program), key.Length - KeyStart(program));
                var ctx = new InterpreterCtx(program.Program, ref frame, ref objects, ref keyReader, ref valueReader);
                BTDB.Interpreter.Interpreter.Run(ref ctx);
                return ctx.BoolResult;
            }
        }
    }

    bool AfterEnd()
    {
        _afterEnd = true;
        return false;
    }

    public void Reset()
    {
        ThrowIfDisposed();
        _cursor!.Invalidate();
        _first = true;
        _afterEnd = false;
        _candidates = null;
        _candidateIndex = 0;
    }

    public void Dispose()
    {
        _cursor?.Dispose();
        _cursor = null;
        _indexCursor?.Dispose();
        _indexCursor = null;
        Array.Clear(_objects);
        if (_valueBuffer == null) return;
        ArrayPool<byte>.Shared.Return(_valueBuffer);
        _valueBuffer = null;
    }
}

sealed class RelationQueryEnumerator<T> : IEnumerator<T>, IEnumerable<T> where T : class
{
    readonly IInternalObjectDBTransaction _transaction;
    readonly RelationInfo.ItemLoaderInfo _loaderInfo;
    readonly RelationQueryScanner _scanner;
    bool _hasCurrent;
    T? _current;

    public RelationQueryEnumerator(IInternalObjectDBTransaction transaction, RelationInfo.ItemLoaderInfo loaderInfo,
        RelationQueryPlan plan, object?[]? parameterValues)
    {
        _transaction = transaction;
        _loaderInfo = loaderInfo;
        _scanner = new(transaction, plan, parameterValues);
    }

    public bool MoveNext()
    {
        _hasCurrent = false;
        _current = null;
        _hasCurrent = _scanner.MoveNext();
        return _hasCurrent;
    }

    public T Current
    {
        get
        {
            if (!_hasCurrent) throw new InvalidOperationException("Enumerator is not positioned on a row.");
            return _current ??= RelationQueryRows.Load<T>(_transaction, _loaderInfo, _scanner.Cursor);
        }
    }

    object IEnumerator.Current => Current;

    public void Reset()
    {
        _scanner.Reset();
        _hasCurrent = false;
        _current = null;
    }

    public void Dispose()
    {
        _scanner.Dispose();
    }

    public IEnumerator<T> GetEnumerator() => this;
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

enum RelationQueryOrder
{
    /// Primary key order, which is the source order and also matches orderings by leading key fields.
    Forward,

    /// Descending primary key order, for descending orderings by all remaining key fields.
    Backward,

    /// All matching rows sorted by their sort keys in memory, stable like Enumerable.OrderBy.
    Sorted
}

/// Rows of an ordered and paged relation query source; only rows in the window are materialized.
sealed class RelationQueryWindow<T> : IEnumerator<T>, IEnumerable<T> where T : class
{
    readonly IInternalObjectDBTransaction _transaction;
    readonly RelationInfo.ItemLoaderInfo _loaderInfo;
    readonly RelationQueryOrder _order;
    readonly bool[] _descending;
    readonly long _skip;
    readonly long? _take;
    readonly bool _keepFrames;
    bool _started;
    long _taken;
    List<SortEntry>? _sorted;
    bool _hasCurrent;
    T? _current;

    public RelationQueryWindow(IInternalObjectDBTransaction transaction, RelationInfo.ItemLoaderInfo loaderInfo,
        RelationQueryPlan plan, RelationQueryOrder order, bool[] descending, long skip, long? take,
        bool keepFrames = false, object?[]? parameterValues = null)
    {
        _keepFrames = keepFrames;
        _transaction = transaction;
        _loaderInfo = loaderInfo;
        _order = order;
        _descending = descending;
        _skip = skip;
        _take = take;
        Scanner = new(transaction, plan, parameterValues);
    }

    /// Positioned on the current row; its frame holds the selector value of the plan.
    public RelationQueryScanner Scanner { get; }

    /// Frame of the current row; sorted windows keep copies of frames when created to keep them.
    public ReadOnlySpan<byte> CurrentFrame =>
        _order == RelationQueryOrder.Sorted ? _sorted![(int)(_skip + _taken - 1)].Frame : Scanner.FrameBytes;

    public bool MoveNext()
    {
        _hasCurrent = false;
        _current = null;
        if (_take is { } take && _taken >= take) return false;
        if (_order == RelationQueryOrder.Sorted)
        {
            _sorted ??= Sort();
            var index = _skip + _taken;
            if (index >= _sorted.Count) return false;
            if (!Scanner.Cursor.FindExactKey(_sorted[(int)index].PrimaryKey))
                throw new InvalidOperationException("Relation row was removed during query enumeration.");
        }
        else
        {
            if (!_started)
            {
                _started = true;
                var skipped = _order == RelationQueryOrder.Forward ? Scanner.SkipKeys(_skip) : 0;
                for (; skipped < _skip; skipped++)
                    if (!Step()) return false;
            }

            if (!Step()) return false;
        }

        _taken++;
        _hasCurrent = true;
        return true;
    }

    bool Step() => _order == RelationQueryOrder.Forward ? Scanner.MoveNext() : Scanner.MovePrevious();

    [SkipLocalsInit]
    List<SortEntry> Sort()
    {
        var sortKeys = Scanner.Plan.SortKeys;
        var comparer = new SortEntryComparer(_descending);
        var limit = _take is { } take ? _skip + take : long.MaxValue;
        var entries = new List<SortEntry>();
        Span<byte> keyBuffer = stackalloc byte[512];
        var sequence = 0L;
        var probe = new SortEntry(new object?[sortKeys.Length], [], 0, null);
        // Like Enumerable.OrderBy, every matching row is tested before the first one is returned.
        while (Scanner.MoveNext())
        {
            for (var i = 0; i < sortKeys.Length; i++) probe.Keys[i] = Scanner.ReadResult(sortKeys[i]);
            probe.Sequence = sequence++;
            var position = entries.Count;
            if (limit != long.MaxValue)
            {
                // Keeps only the first limit entries; the sequence makes every entry unique and the sort stable.
                // Rejected rows allocate nothing beyond their boxed keys.
                position = ~entries.BinarySearch(probe, comparer);
                if (position >= limit) continue;
            }

            entries.Insert(position, new((object?[])probe.Keys.Clone(),
                Scanner.Cursor.GetKeySpan(ref keyBuffer).ToArray(), probe.Sequence,
                _keepFrames ? Scanner.FrameBytes.ToArray() : null));
            if (entries.Count > limit) entries.RemoveAt(entries.Count - 1);
        }

        if (limit == long.MaxValue) entries.Sort(comparer);
        return entries;
    }

    sealed class SortEntry(object?[] keys, byte[] primaryKey, long sequence, byte[]? frame)
    {
        public object?[] Keys { get; } = keys;
        public byte[] PrimaryKey { get; } = primaryKey;
        public long Sequence { get; set; } = sequence;
        public byte[]? Frame { get; } = frame;
    }

    sealed class SortEntryComparer(bool[] descending) : IComparer<SortEntry>
    {
        // Comparer<T>.Default of Enumerable.OrderBy: culture sensitive for strings, nulls first.
        readonly Comparer _comparer = new(CultureInfo.CurrentCulture);

        public int Compare(SortEntry? x, SortEntry? y)
        {
            for (var i = 0; i < descending.Length; i++)
            {
                var order = _comparer.Compare(x!.Keys[i], y!.Keys[i]);
                if (order != 0) return descending[i] ? -order : order;
            }

            return x!.Sequence.CompareTo(y!.Sequence);
        }
    }

    public T Current
    {
        get
        {
            if (!_hasCurrent) throw new InvalidOperationException("Enumerator is not positioned on a row.");
            return _current ??= RelationQueryRows.Load<T>(_transaction, _loaderInfo, Scanner.Cursor);
        }
    }

    object IEnumerator.Current => Current;

    public void Reset()
    {
        throw new NotSupportedException();
    }

    public void Dispose()
    {
        Scanner.Dispose();
    }

    public IEnumerator<T> GetEnumerator() => this;
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// Queryable after Select: element type differs from the row type, the root query provides everything.
sealed class RelationQueryProjected<TRow, TElement> : IQueryable<TElement>, IRelationQueryChain where TRow : class
{
    readonly RelationQuery<TRow> _root;
    Expression? _expression;
    readonly RelationQueryLink? _link;

    public RelationQueryProjected(RelationQuery<TRow> root, Expression expression)
    {
        _root = root;
        _expression = expression;
    }

    public RelationQueryProjected(RelationQuery<TRow> root, RelationQueryLink link)
    {
        _root = root;
        _link = link;
    }

    public Type ElementType => typeof(TElement);
    public Expression Expression => _expression ??= _link!.Materialize();
    public IQueryProvider Provider => _root;

    public IEnumerator<TElement> GetEnumerator() => _link != null
        ? RelationQueryInterception.EnumerateProjected<TRow, TElement>(_root, _link, this)
        : _root.EnumerateProjected<TElement>(Expression);

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    RelationQueryLink? IRelationQueryChain.Link => _link;

    public bool IsRoot => false;

    // After Select only paging keeps the element type; anything else is left to Queryable.
    TQueryable? IRelationQueryChain.Append<TElementOut, TQueryable>(RelationQueryLink link) where TQueryable : class =>
        typeof(TElementOut) == typeof(TElement) ? new RelationQueryProjected<TRow, TElement>(_root, link) as TQueryable : null;

    object? IRelationQueryChain.ExecuteIntercepted(RelationQueryCallSite site, object? argument1, object? argument2,
        Func<Expression> terminalExpression) =>
        RelationQueryInterception.Execute(_root, _link, site, argument1, argument2, terminalExpression,
            _link != null);
}

sealed class RelationQueryProjectedEnumerator<TRow, TElement> : IEnumerator<TElement> where TRow : class
{
    readonly RelationQueryWindow<TRow> _window;
    readonly RelationQueryProjection<TRow> _projection;
    bool _hasCurrent;
    TElement _current = default!;

    public RelationQueryProjectedEnumerator(RelationQueryWindow<TRow> window, RelationQueryProjection<TRow> projection)
    {
        _window = window;
        _projection = projection;
        _typed = projection.IsTypedValue<TElement>();
    }

    readonly bool _typed;

    public bool MoveNext()
    {
        _hasCurrent = _window.MoveNext();
        if (_hasCurrent) _current = _projection.Build<TElement>(_window.Scanner, _typed);
        return _hasCurrent;
    }

    public TElement Current =>
        _hasCurrent ? _current : throw new InvalidOperationException("Enumerator is not positioned on a row.");

    object? IEnumerator.Current => Current;

    public void Reset() => throw new NotSupportedException();
    public void Dispose() => _window.Dispose();
}

/// Builds Select results of matching rows: values and strings computed by their programs, the row itself, or objects
/// constructed from those through a constructor, or through generated metadata or reflection for member bindings.
sealed class RelationQueryProjection<TRow> where TRow : class
{
    readonly IInternalObjectDBTransaction _transaction;
    readonly RelationInfo.ItemLoaderInfo _loaderInfo;
    readonly List<ProjectionArgument> _arguments = [];
    readonly List<QueryNode> _selectors = [];
    readonly NewExpression? _new;
    readonly List<(MemberInfo Member, int Argument, FieldMetadata? Field)>? _bindings;
    readonly ClassMetadata? _metadata;
    RelationQueryResult[] _results = [];

    // A copy for one execution of a cached projection: the analysis is shared, argument boxes are not.
    RelationQueryProjection(RelationQueryProjection<TRow> template, IInternalObjectDBTransaction transaction,
        RelationInfo.ItemLoaderInfo loaderInfo)
    {
        _transaction = transaction;
        _loaderInfo = loaderInfo;
        foreach (var argument in template._arguments) _arguments.Add(new(argument));
        _selectors = template._selectors;
        _new = template._new;
        _bindings = template._bindings;
        _metadata = template._metadata;
        _factory = template._factory;
        _argumentCount = template._argumentCount;
        _useCreator = template._useCreator;
        _constructorInvoker = template._constructorInvoker;
        _factoryInvoker = template._factoryInvoker;
    }

    /// The projection for an execution, which may be a different one than the projection was built in.
    public RelationQueryProjection<TRow> ForExecution(IInternalObjectDBTransaction transaction,
        RelationInfo.ItemLoaderInfo loaderInfo)
    {
        // Invokers are created once and shared by every execution.
        _constructorInvoker ??= _new is { Constructor: { } constructor } ? ConstructorInvoker.Create(constructor) : null;
        _factoryInvoker ??= _factory != null ? MethodInvoker.Create(_factory) : null;
        return new(this, transaction, loaderInfo);
    }

    public RelationQueryProjection(LambdaExpression lambda, QueryByIdQueryNode query,
        IInternalObjectDBTransaction transaction, RelationInfo.ItemLoaderInfo loaderInfo)
    {
        _transaction = transaction;
        _loaderInfo = loaderInfo;
        var parameter = lambda.Parameters[0];
        switch (lambda.Body)
        {
            case NewExpression { Constructor: not null } newExpression:
                _new = newExpression;
                foreach (var argument in newExpression.Arguments) AddArgument(argument, parameter, query);
                break;
            case MemberInitExpression { NewExpression.Constructor: not null } memberInit:
                _new = memberInit.NewExpression;
                foreach (var argument in _new.Arguments) AddArgument(argument, parameter, query);
                _bindings = [];
                _metadata = ReflectionMetadata.FindByType(memberInit.Type);
                foreach (var binding in memberInit.Bindings)
                {
                    if (binding is not MemberAssignment assignment)
                        throw new NotSupportedException("Relation query supports only member assignments in Select.");
                    FieldMetadata? field = null;
                    if (_metadata != null && !memberInit.Type.IsValueType)
                    {
                        foreach (var candidate in _metadata.Fields)
                            if (candidate.Name == assignment.Member.Name) field = candidate;
                        unsafe
                        {
                            // A generated setter, or the offset of an auto-property backing field of the same type.
                            if (field != null && field.PropRefSetter == null &&
                                (field.ByteOffset == null || field.Type != assignment.Expression.Type))
                                field = null;
                        }
                    }

                    _bindings.Add((assignment.Member, AddArgument(assignment.Expression, parameter, query), field));
                }

                break;
            case MethodCallExpression { Object: null, Method.Name: "Create" } create
                when create.Method.DeclaringType == typeof(ValueTuple) || create.Method.DeclaringType == typeof(Tuple):
                _factory = create.Method;
                foreach (var argument in create.Arguments) AddArgument(argument, parameter, query);
                break;
            default:
                AddArgument(lambda.Body, parameter, query);
                break;
        }

        _argumentCount = _new?.Arguments.Count ?? _arguments.Count;
        unsafe
        {
            _useCreator = _bindings != null && _argumentCount == 0 && _metadata?.Creator != null &&
                          !_new!.Type.IsValueType;
        }
    }

    /// Constructor or factory arguments; member bindings follow them in _arguments.
    readonly int _argumentCount;

    /// Member bindings of a parameterless class are set on an instance from its generated creator.
    readonly bool _useCreator;

    /// ValueTuple.Create or Tuple.Create.
    readonly MethodInfo? _factory;

    /// Values the programs compute for each row, in RelationQueryPlan.Projection order.
    public IReadOnlyList<QueryNode> Selectors => _selectors;

    /// A single value or string, which aggregates can use as their selector.
    public QueryNode? SingleSelector =>
        _new == null && _factory == null && !_arguments[0].IsRow ? _selectors[0] : null;

    public void Bind(RelationQueryPlan plan)
    {
        _results = plan.Projection;
        _eager = plan.ProjectionIsEager;
    }

    bool _eager;

    public bool CanThrow => _selectors.Exists(RelationQueryCompiler.CanThrowNode);

    int AddArgument(Expression expression, ParameterExpression parameter, QueryByIdQueryNode query)
    {
        if (expression == parameter)
        {
            _arguments.Add(new(true, -1, expression.Type));
            return _arguments.Count - 1;
        }

        var node = RelationQueryAstBuilder.BuildValue(expression, parameter, query);
        if (node.Type == typeof(string)
                ? node is not PropQueryNode
                : !RelationQueryTypes.TryGetStorage(node.Type, out _, out _, out _))
            throw new NotSupportedException(
                $"Relation query does not support selecting '{expression}' of type {expression.Type.ToSimpleName()}.");
        _selectors.Add(node);
        _arguments.Add(new(false, _selectors.Count - 1, expression.Type));
        return _arguments.Count - 1;
    }

    /// A constructor, factory or member argument, or the single selected value.
    sealed class ProjectionArgument
    {
        public ProjectionArgument(ProjectionArgument other)
        {
            IsRow = other.IsRow;
            Selector = other.Selector;
            Type = other.Type;
            EnumType = other.EnumType;
            IsNullable = other.IsNullable;
            StorageSize = other.StorageSize;
            NullableValueOffset = other.NullableValueOffset;
        }

        public ProjectionArgument(bool isRow, int selector, Type type)
        {
            IsRow = isRow;
            Selector = selector;
            Type = type;
            var target = Nullable.GetUnderlyingType(type) ?? type;
            EnumType = target.IsEnum ? target : null;
            IsNullable = target != type;
            if (!isRow && type.IsValueType && RelationQueryTypes.TryGetStorage(type, out var storage, out _, out _) &&
                (target == storage || EnumType != null))
            {
                StorageSize = RelationQueryTypes.ValueSize(storage);
                if (IsNullable) NullableValueOffset = GetNullableValueOffset(storage);
            }
        }

        public readonly bool IsRow;
        public readonly int Selector;
        public readonly Type Type;
        public readonly Type? EnumType;
        public readonly bool IsNullable;

        /// Size of the value in the frame when its bytes are the bytes of Type (or of its underlying type), else 0.
        public readonly int StorageSize;

        /// Offset of the value within a Nullable of Type, after its HasValue.
        public readonly int NullableValueOffset;

        /// Box reused for every row; parameters and members of value types copy the value out of it.
        public object? Box;
    }

    static int GetNullableValueOffset(Type storage)
    {
        if (storage == typeof(bool)) return NullableValueOffset<bool>();
        if (storage == typeof(byte)) return NullableValueOffset<byte>();
        if (storage == typeof(sbyte)) return NullableValueOffset<sbyte>();
        if (storage == typeof(ushort)) return NullableValueOffset<ushort>();
        if (storage == typeof(short)) return NullableValueOffset<short>();
        if (storage == typeof(uint)) return NullableValueOffset<uint>();
        if (storage == typeof(int)) return NullableValueOffset<int>();
        if (storage == typeof(ulong)) return NullableValueOffset<ulong>();
        if (storage == typeof(long)) return NullableValueOffset<long>();
        if (storage == typeof(Half)) return NullableValueOffset<Half>();
        if (storage == typeof(float)) return NullableValueOffset<float>();
        if (storage == typeof(double)) return NullableValueOffset<double>();
        if (storage == typeof(DateTime)) return NullableValueOffset<DateTime>();
        if (storage == typeof(Guid)) return NullableValueOffset<Guid>();
        if (storage == typeof(decimal)) return NullableValueOffset<decimal>();
        if (storage == typeof(TimeSpan)) return NullableValueOffset<TimeSpan>();
        if (storage == typeof(DateTimeOffset)) return NullableValueOffset<DateTimeOffset>();
        throw new NotSupportedException($"Relation query cannot select nullable {storage.ToSimpleName()} values.");
    }

    static int NullableValueOffset<T>() where T : struct
    {
        T? nullable = default;
        return (int)Unsafe.ByteOffset(ref Unsafe.As<T?, byte>(ref nullable),
            ref Unsafe.As<T, byte>(ref Unsafe.AsRef(in Nullable.GetValueRefOrDefaultRef(in nullable))));
    }

    /// Whether a single value can be returned as TElement straight from its frame bytes, without boxing.
    public bool IsTypedValue<TElement>()
    {
        if (_new != null || _factory != null || _arguments[0].IsRow || !typeof(TElement).IsValueType ||
            Nullable.GetUnderlyingType(typeof(TElement)) != null)
            return false;
        var result = _results[0];
        return !result.IsString && Unsafe.SizeOf<TElement>() == RelationQueryTypes.ValueSize(result.StorageType);
    }

    /// Builds the result; typed is IsTypedValue of TElement.
    public TElement Build<TElement>(RelationQueryScanner scanner, bool typed)
    {
        if (_selectors.Count > 0 && !_eager) scanner.ComputeProjection();
        if (typed) return Unsafe.ReadUnaligned<TElement>(ref scanner.FrameBytes[_results[0].Slot]);
        return (TElement)Construct(scanner)!;
    }

    public object? BuildBoxed(RelationQueryScanner scanner)
    {
        if (_selectors.Count > 0 && !_eager) scanner.ComputeProjection();
        return Construct(scanner);
    }

    object? Construct(RelationQueryScanner scanner)
    {
        // The single value is returned, so it cannot reuse a box.
        if (_new == null && _factory == null) return ReadArgument(0, scanner, false);
        var count = _argumentCount;
        object target;
        unsafe
        {
            if (_useCreator)
            {
                target = _metadata!.Creator();
            }
            else
            {
                // Invokers avoid most of the reflection overhead; up to four arguments need no array.
                _constructorInvoker ??= _new != null ? ConstructorInvoker.Create(_new.Constructor!) : null;
                _factoryInvoker ??= _factory != null ? MethodInvoker.Create(_factory) : null;
                target = count switch
                {
                    0 => Invoke(),
                    1 => Invoke(ReadArgument(0, scanner, true)),
                    2 => Invoke(ReadArgument(0, scanner, true), ReadArgument(1, scanner, true)),
                    3 => Invoke(ReadArgument(0, scanner, true), ReadArgument(1, scanner, true),
                        ReadArgument(2, scanner, true)),
                    4 => Invoke(ReadArgument(0, scanner, true), ReadArgument(1, scanner, true),
                        ReadArgument(2, scanner, true), ReadArgument(3, scanner, true)),
                    _ => InvokeMany(scanner, count)
                };
            }
        }

        if (_bindings == null) return target;
        foreach (var (member, argument, field) in _bindings)
        {
            if (field != null && TrySetFromFrame(field, target, _arguments[argument], scanner)) continue;
            var value = ReadArgument(argument, scanner, true);
            if (field != null && HasSetter(field))
                SetField(field, target, value,
                    member is PropertyInfo p ? p.PropertyType : ((FieldInfo)member).FieldType);
            else if (member is PropertyInfo property) property.SetValue(target, value);
            else ((FieldInfo)member).SetValue(target, value);
        }

        return target;
    }

    ConstructorInvoker? _constructorInvoker;
    MethodInvoker? _factoryInvoker;

    object Invoke() => _constructorInvoker?.Invoke() ?? _factoryInvoker!.Invoke(null)!;
    object Invoke(object? a) => _constructorInvoker?.Invoke(a) ?? _factoryInvoker!.Invoke(null, a)!;
    object Invoke(object? a, object? b) => _constructorInvoker?.Invoke(a, b) ?? _factoryInvoker!.Invoke(null, a, b)!;

    object Invoke(object? a, object? b, object? c) =>
        _constructorInvoker?.Invoke(a, b, c) ?? _factoryInvoker!.Invoke(null, a, b, c)!;

    object Invoke(object? a, object? b, object? c, object? d) =>
        _constructorInvoker?.Invoke(a, b, c, d) ?? _factoryInvoker!.Invoke(null, a, b, c, d)!;

    object InvokeMany(RelationQueryScanner scanner, int count)
    {
        var values = new object?[count];
        for (var i = 0; i < count; i++) values[i] = ReadArgument(i, scanner, true);
        return _constructorInvoker?.Invoke(values) ?? _factoryInvoker!.Invoke(null, values)!;
    }

    object? ReadArgument(int index, RelationQueryScanner scanner, bool reuseBox)
    {
        var argument = _arguments[index];
        if (argument.IsRow) return RelationQueryRows.Load<TRow>(_transaction, _loaderInfo, scanner.Cursor);
        var result = _results[argument.Selector];
        if (reuseBox && argument.StorageSize != 0 && !result.IsString)
        {
            var frame = scanner.FrameBytes;
            if (frame[result.HasValueSlot] == 0) return null;
            if (argument.Box is { } box)
            {
                Unsafe.CopyBlockUnaligned(ref RawData.Ref(box, (uint)Unsafe.SizeOf<object>()), ref frame[result.Slot],
                    (uint)argument.StorageSize);
                return box;
            }
        }

        var value = scanner.ReadResult(result);
        if (value != null && argument.EnumType != null) value = Enum.ToObject(argument.EnumType, value);
        if (reuseBox && argument.StorageSize != 0) argument.Box = value;
        return value;
    }

    /// Generated setters of non-nullable value members read the value straight from the frame; auto-property backing
    /// fields are written in place, like generated deserializers do.
    unsafe bool TrySetFromFrame(FieldMetadata field, object target, ProjectionArgument argument,
        RelationQueryScanner scanner)
    {
        if (argument.IsRow) return false;
        var result = _results[argument.Selector];
        var frame = scanner.FrameBytes;
        if (field.PropRefSetter != null)
        {
            if (argument.StorageSize == 0 || argument.IsNullable || result.IsString ||
                frame[result.HasValueSlot] == 0) return false;
            field.PropRefSetter(target, ref frame[result.Slot]);
            return true;
        }

        ref var member = ref RawData.Ref(target, field.ByteOffset!.Value);
        if (result.IsString)
        {
            Unsafe.As<byte, object?>(ref member) = scanner.ReadResult(result);
            return true;
        }

        var size = (uint)argument.StorageSize;
        if (size == 0) return false;
        var hasValue = frame[result.HasValueSlot] != 0;
        if (argument.IsNullable)
        {
            member = hasValue ? (byte)1 : (byte)0;
            member = ref Unsafe.Add(ref member, argument.NullableValueOffset);
        }

        if (hasValue) Unsafe.CopyBlockUnaligned(ref member, ref frame[result.Slot], size);
        else Unsafe.InitBlockUnaligned(ref member, 0, size);
        return true;
    }

    static unsafe bool HasSetter(FieldMetadata field) => field.PropRefSetter != null;

    // Generated setters take a reference to the value, so the boxed value is unboxed into its storage type; enums
    // unbox to their underlying type.
    static unsafe void SetField(FieldMetadata field, object target, object? value, Type memberType)
    {
        var setter = field.PropRefSetter;
        if (!memberType.IsValueType)
        {
            var reference = value;
            setter(target, ref Unsafe.As<object?, byte>(ref reference));
            return;
        }

        RelationQueryTypes.TryGetStorage(memberType, out var storage, out _, out var nullable);
        if (storage == typeof(bool)) Set<bool>(setter, target, value, nullable);
        else if (storage == typeof(byte)) Set<byte>(setter, target, value, nullable);
        else if (storage == typeof(sbyte)) Set<sbyte>(setter, target, value, nullable);
        else if (storage == typeof(ushort)) Set<ushort>(setter, target, value, nullable);
        else if (storage == typeof(short)) Set<short>(setter, target, value, nullable);
        else if (storage == typeof(uint)) Set<uint>(setter, target, value, nullable);
        else if (storage == typeof(int)) Set<int>(setter, target, value, nullable);
        else if (storage == typeof(ulong)) Set<ulong>(setter, target, value, nullable);
        else if (storage == typeof(long)) Set<long>(setter, target, value, nullable);
        else if (storage == typeof(Half)) Set<Half>(setter, target, value, nullable);
        else if (storage == typeof(float)) Set<float>(setter, target, value, nullable);
        else if (storage == typeof(double)) Set<double>(setter, target, value, nullable);
        else if (storage == typeof(DateTime)) Set<DateTime>(setter, target, value, nullable);
        else if (storage == typeof(Guid)) Set<Guid>(setter, target, value, nullable);
        else if (storage == typeof(decimal)) Set<decimal>(setter, target, value, nullable);
        else if (storage == typeof(TimeSpan)) Set<TimeSpan>(setter, target, value, nullable);
        else if (storage == typeof(DateTimeOffset)) Set<DateTimeOffset>(setter, target, value, nullable);
        else throw new NotSupportedException($"Relation query cannot set {memberType.ToSimpleName()} members.");
    }

    static unsafe void Set<T>(delegate*<object, ref byte, void> setter, object target, object? value, bool nullable)
        where T : struct
    {
        if (nullable)
        {
            T? nullableValue = value == null ? null : (T)value;
            setter(target, ref Unsafe.As<T?, byte>(ref nullableValue));
            return;
        }

        var plainValue = (T)value!;
        setter(target, ref Unsafe.As<T, byte>(ref plainValue));
    }
}

static class RelationQueryRows
{
    [SkipLocalsInit]
    public static T Load<T>(IInternalObjectDBTransaction transaction, RelationInfo.ItemLoaderInfo loaderInfo,
        IKeyValueDBCursor cursor) where T : class
    {
        Span<byte> keyBuffer = stackalloc byte[2048];
        var key = cursor.GetKeySpan(ref keyBuffer);
        return (T)loaderInfo.CreateInstance(transaction, cursor, key);
    }
}

/// Accumulates the selector values of matching rows for Queryable aggregates with LINQ-to-Objects semantics.
abstract class RelationQueryAggregator
{
    public abstract void Add(ReadOnlySpan<byte> frame);
    public abstract object? Result();

    public static RelationQueryAggregator Create(string operation, RelationQueryResult result)
    {
        var storage = result.StorageType;
        var nullable = Nullable.GetUnderlyingType(result.Type) != null;
        switch (operation)
        {
            case nameof(Queryable.Sum):
                if (storage == typeof(int)) return new SumInt32Aggregator(result);
                if (storage == typeof(long)) return new SumInt64Aggregator(result);
                if (storage == typeof(float)) return new SumSingleAggregator(result);
                if (storage == typeof(double)) return new SumDoubleAggregator(result);
                if (storage == typeof(decimal)) return new SumDecimalAggregator(result);
                break;
            case nameof(Queryable.Average):
                if (storage == typeof(int)) return new AverageInt64Aggregator<int>(result, nullable);
                if (storage == typeof(long)) return new AverageInt64Aggregator<long>(result, nullable);
                if (storage == typeof(float)) return new AverageSingleAggregator(result, nullable);
                if (storage == typeof(double)) return new AverageDoubleAggregator(result, nullable);
                if (storage == typeof(decimal)) return new AverageDecimalAggregator(result, nullable);
                break;
            case nameof(Queryable.Min):
            case nameof(Queryable.Max):
                return CreateMinMax(storage, result, operation == nameof(Queryable.Max), nullable);
        }

        throw new NotSupportedException(
            $"Relation query does not support Queryable.{operation} of {result.Type.ToSimpleName()}.");
    }

    static RelationQueryAggregator CreateMinMax(Type storage, RelationQueryResult result, bool max, bool nullable)
    {
        if (storage == typeof(bool)) return new MinMaxAggregator<bool>(result, max, nullable);
        if (storage == typeof(byte)) return new MinMaxAggregator<byte>(result, max, nullable);
        if (storage == typeof(sbyte)) return new MinMaxAggregator<sbyte>(result, max, nullable);
        if (storage == typeof(ushort)) return new MinMaxAggregator<ushort>(result, max, nullable);
        if (storage == typeof(short)) return new MinMaxAggregator<short>(result, max, nullable);
        if (storage == typeof(uint)) return new MinMaxAggregator<uint>(result, max, nullable);
        if (storage == typeof(int)) return new MinMaxAggregator<int>(result, max, nullable);
        if (storage == typeof(ulong)) return new MinMaxAggregator<ulong>(result, max, nullable);
        if (storage == typeof(long)) return new MinMaxAggregator<long>(result, max, nullable);
        if (storage == typeof(Half)) return new MinMaxAggregator<Half>(result, max, nullable);
        if (storage == typeof(float)) return new MinMaxAggregator<float>(result, max, nullable);
        if (storage == typeof(double)) return new MinMaxAggregator<double>(result, max, nullable);
        if (storage == typeof(DateTime)) return new MinMaxAggregator<DateTime>(result, max, nullable);
        if (storage == typeof(Guid)) return new MinMaxAggregator<Guid>(result, max, nullable);
        if (storage == typeof(decimal)) return new MinMaxAggregator<decimal>(result, max, nullable);
        if (storage == typeof(TimeSpan)) return new MinMaxAggregator<TimeSpan>(result, max, nullable);
        if (storage == typeof(DateTimeOffset)) return new MinMaxAggregator<DateTimeOffset>(result, max, nullable);
        throw new NotSupportedException($"Relation query does not support Min or Max of {storage.ToSimpleName()}.");
    }

    protected static InvalidOperationException NoElements() => new("Sequence contains no elements");
}

sealed class RelationQueryCounter : RelationQueryAggregator
{
    public long Count;

    public override void Add(ReadOnlySpan<byte> frame) => Count++;
    public override object Result() => Count;
}

/// Reads the selector value of a matching row; null values are skipped by every aggregate.
abstract class RelationQueryValueAggregator<T> : RelationQueryAggregator where T : unmanaged
{
    readonly int _slot;
    readonly int _hasValueSlot;

    protected RelationQueryValueAggregator(RelationQueryResult result)
    {
        _slot = result.Slot;
        _hasValueSlot = result.HasValueSlot;
    }

    public override void Add(ReadOnlySpan<byte> frame)
    {
        if (frame[_hasValueSlot] == 0) return;
        Add(MemoryMarshal.Read<T>(frame[_slot..]));
    }

    protected abstract void Add(T value);
}

sealed class SumInt32Aggregator(RelationQueryResult result) : RelationQueryValueAggregator<int>(result)
{
    int _sum;

    protected override void Add(int value) => _sum = checked(_sum + value);
    public override object Result() => _sum;
}

sealed class SumInt64Aggregator(RelationQueryResult result) : RelationQueryValueAggregator<long>(result)
{
    long _sum;

    protected override void Add(long value) => _sum = checked(_sum + value);
    public override object Result() => _sum;
}

// Like Enumerable.Sum, floats are summed as double.
sealed class SumSingleAggregator(RelationQueryResult result) : RelationQueryValueAggregator<float>(result)
{
    double _sum;

    protected override void Add(float value) => _sum += value;
    public override object Result() => (float)_sum;
}

sealed class SumDoubleAggregator(RelationQueryResult result) : RelationQueryValueAggregator<double>(result)
{
    double _sum;

    protected override void Add(double value) => _sum += value;
    public override object Result() => _sum;
}

sealed class SumDecimalAggregator(RelationQueryResult result) : RelationQueryValueAggregator<decimal>(result)
{
    decimal _sum;

    protected override void Add(decimal value) => _sum += value;
    public override object Result() => _sum;
}

// Like Enumerable.Average, integers are summed as checked long and the result is double.
sealed class AverageInt64Aggregator<T>(RelationQueryResult result, bool nullable)
    : RelationQueryValueAggregator<T>(result) where T : unmanaged, IBinaryInteger<T>
{
    long _sum;
    long _count;

    protected override void Add(T value)
    {
        _sum = checked(_sum + long.CreateTruncating(value));
        _count++;
    }

    public override object? Result()
    {
        if (_count == 0) return nullable ? null : throw NoElements();
        return (double)_sum / _count;
    }
}

sealed class AverageSingleAggregator(RelationQueryResult result, bool nullable)
    : RelationQueryValueAggregator<float>(result)
{
    double _sum;
    long _count;

    protected override void Add(float value)
    {
        _sum += value;
        _count++;
    }

    public override object? Result()
    {
        if (_count == 0) return nullable ? null : throw NoElements();
        return (float)(_sum / _count);
    }
}

sealed class AverageDoubleAggregator(RelationQueryResult result, bool nullable)
    : RelationQueryValueAggregator<double>(result)
{
    double _sum;
    long _count;

    protected override void Add(double value)
    {
        _sum += value;
        _count++;
    }

    public override object? Result()
    {
        if (_count == 0) return nullable ? null : throw NoElements();
        return _sum / _count;
    }
}

sealed class AverageDecimalAggregator(RelationQueryResult result, bool nullable)
    : RelationQueryValueAggregator<decimal>(result)
{
    decimal _sum;
    long _count;

    protected override void Add(decimal value)
    {
        _sum += value;
        _count++;
    }

    public override object? Result()
    {
        if (_count == 0) return nullable ? null : throw NoElements();
        return _sum / _count;
    }
}

// Like Enumerable.Min and Max with a selector: Comparer<T>.Default, so NaN is the smallest double; an empty sequence
// returns null for nullable selectors and throws otherwise.
sealed class MinMaxAggregator<T> : RelationQueryValueAggregator<T> where T : unmanaged
{
    readonly bool _max;
    readonly bool _nullable;
    readonly Type? _enumType;
    bool _any;
    T _value;

    public MinMaxAggregator(RelationQueryResult result, bool max, bool nullable) : base(result)
    {
        _max = max;
        _nullable = nullable;
        var type = Nullable.GetUnderlyingType(result.Type) ?? result.Type;
        if (type.IsEnum) _enumType = type;
    }

    protected override void Add(T value)
    {
        if (_any)
        {
            var order = Comparer<T>.Default.Compare(value, _value);
            if (_max ? order <= 0 : order >= 0) return;
        }

        _value = value;
        _any = true;
    }

    public override object? Result()
    {
        if (!_any) return _nullable ? null : throw NoElements();
        return _enumType != null ? Enum.ToObject(_enumType, _value) : _value;
    }
}

/// A contiguous part of the key space scanned by a relation query: keys starting with Prefix, from Start on and up to
/// the Stop bound of the field following Prefix.
/// Key ranges of an access path: equalities on leading key fields, then StartsWith or bounds on the next field. Values
/// may be parameter references, encoded from each execution's values.
sealed class RelationQueryRangeBuilder
{
    public RelationQueryRangeBuilder(byte[] prefix)
    {
        Prefix = prefix;
    }

    public byte[] Prefix { get; }

    /// Equality fields in key order; Single fields have one value in every execution.
    public readonly List<(FieldHandlerSave Save, Type Type, List<object?> Values, bool Single)> Fields = [];

    /// Encoded ordinal StartsWith prefix of the next field, with the encoded null key scanned too.
    public byte[]? StartsWith;
    public byte[]? NullKey;

    public FieldHandlerSave? BoundSave;
    public Type? BoundType;
    public object? Lower;
    public object? Upper;
    public bool LowerInclusive;
    public bool UpperInclusive;

    public bool HasParameters;

    /// Set when a key handler needs a writer context; such plans are not cached.
    public IWriterCtx? WriterCtx;

    public RelationQueryRange[] Build(object?[]? values, out int commonPrefixLength)
    {
        if (StartsWith == null && BoundType == null && Fields.TrueForAll(f => f.Single))
        {
            // The common shape, one range of equalities, is written at once.
            var writer = new MemWriter();
            writer.WriteBlock(Prefix);
            foreach (var (save, type, fieldValues, _) in Fields)
                RelationQueryCompiler.SaveConstant(save, ref writer, WriterCtx, type,
                    RelationQueryParameterRef.Resolve(fieldValues[0], values));
            var key = writer.GetSpan().ToArray();
            commonPrefixLength = key.Length;
            return [new(key, key, false, null, false)];
        }

        var prefixes = new List<byte[]> { Prefix };
        commonPrefixLength = Prefix.Length;
        var allSingle = true;
        foreach (var (save, type, fieldValues, single) in Fields)
        {
            var encodings = Distinct(save, WriterCtx, type, fieldValues, values);
            var next = new List<byte[]>(prefixes.Count * encodings.Count);
            foreach (var p in prefixes)
            foreach (var e in encodings)
                next.Add([..p, ..e]);
            prefixes = next;
            if (allSingle && single) commonPrefixLength += encodings[0].Length;
            else allSingle = false;
        }

        byte[]? lowerBytes = null, upperBytes = null;
        if (BoundType != null)
        {
            var lower = RelationQueryParameterRef.Resolve(Lower, values);
            var upper = RelationQueryParameterRef.Resolve(Upper, values);
            if (lower != null) lowerBytes = RelationQueryCompiler.Encode(BoundSave!, WriterCtx, BoundType, lower);
            if (upper != null) upperBytes = RelationQueryCompiler.Encode(BoundSave!, WriterCtx, BoundType, upper);
        }

        var ranges = new List<RelationQueryRange>(prefixes.Count * (NullKey == null ? 1 : 2));
        foreach (var p in prefixes)
        {
            if (StartsWith != null)
            {
                byte[] start = [..p, ..StartsWith];
                ranges.Add(new(start, start, false, null, false));
                byte[] nullKeys = [..p, ..NullKey!];
                ranges.Add(new(nullKeys, nullKeys, false, null, false));
            }
            else if (BoundType != null)
            {
                ranges.Add(new(p, lowerBytes == null ? p : [..p, ..lowerBytes], lowerBytes != null && !LowerInclusive,
                    upperBytes, UpperInclusive));
            }
            else
            {
                ranges.Add(new(p, p, false, null, false));
            }
        }

        ranges.Sort(static (a, b) => a.Start.AsSpan().SequenceCompareTo(b.Start));
        return ranges.ToArray();
    }

    public static List<byte[]> Distinct(FieldHandlerSave save, IWriterCtx? writerCtx, Type type, List<object?> values,
        object?[]? parameterValues)
    {
        var encodings = new List<byte[]>(values.Count);
        foreach (var value in values)
        {
            var encoded = RelationQueryCompiler.Encode(save, writerCtx, type,
                RelationQueryParameterRef.Resolve(value, parameterValues));
            if (!encodings.Exists(e => e.AsSpan().SequenceEqual(encoded))) encodings.Add(encoded);
        }

        return encodings;
    }
}

sealed class RelationQueryRange
{
    public RelationQueryRange(byte[] prefix, byte[] start, bool excludeStart, byte[]? stop, bool stopInclusive)
    {
        Prefix = prefix;
        Start = start;
        ExcludeStart = excludeStart;
        Stop = stop;
        StopInclusive = stopInclusive;
        IsWholePrefix = stop == null && !excludeStart && start.Length == prefix.Length;
    }

    public byte[] Prefix { get; }

    /// Prefix followed by the encoded lower bound, or just Prefix.
    public byte[] Start { get; }

    /// Keys starting with Start are excluded (exclusive lower bound).
    public bool ExcludeStart { get; }

    /// Encoded upper bound of the field following Prefix.
    public byte[]? Stop { get; }

    public bool StopInclusive { get; }

    /// The range is all keys with Prefix, so its size is a difference of key indexes.
    public bool IsWholePrefix { get; }

    public bool Contains(ReadOnlySpan<byte> key)
    {
        if (!key.StartsWith(Prefix)) return false;
        if (Start.Length != Prefix.Length)
        {
            if (ExcludeStart && key.StartsWith(Start)) return false;
            if (!key.StartsWith(Start) && key.SequenceCompareTo(Start) < 0) return false;
        }

        return Stop == null || !IsPastStop(key[Prefix.Length..]);
    }

    /// Whether the key, already known to be at or after Start, is past the upper bound.
    public bool IsPastStop(ReadOnlySpan<byte> rest)
    {
        // Encodings are prefix free, so a key either starts with the bound encoding or differs within it
        // (BTDB/Proofs, KeyRange.lean).
        var equal = rest.StartsWith(Stop);
        return StopInclusive ? !equal && rest.SequenceCompareTo(Stop) > 0 : equal || rest.SequenceCompareTo(Stop) > 0;
    }
}

static class RelationQueryKeyRanges
{
    /// Field handlers whose key encoding orders bytes like values (checked by RelationQueryKeyOrderTest), so
    /// comparisons can become range seeks. DateTime is not one: its encoding includes Kind, which C# ignores.
    public static bool IsOrderPreserving(IFieldHandler handler)
    {
        return handler is EnumFieldHandler || handler.Name is "Unsigned" or "Signed" or "Bool" or "UInt8" or "Int8Orderable";
    }
}

static class RelationQueryPropertyName
{
    public static string GetPersistentName(PropertyInfo propertyInfo)
    {
        return propertyInfo.GetCustomAttribute<PersistedNameAttribute>()?.Name ?? propertyInfo.Name;
    }
}
