using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;

namespace BTDB.ODBLayer;

/// A value of the current execution in place of a constant: a maximal subtree of the query expression independent of
/// the row. Reading Value bakes the parameter into what is being built, which is then reused only for an equal value;
/// parameter-aware code reads PeekValue and binds the value for each execution instead.
sealed class ParameterQueryNode : ConstantQueryNode
{
    public ParameterQueryNode(int index, Type type) : base(null, type)
    {
        Index = index;
    }

    public int Index { get; }

    public override object? Value => RelationQueryParameterScope.Bake(Index);

    public object? PeekValue => RelationQueryParameterScope.Peek(Index);
}

/// A parameter in the value lists of key ranges, encoded from the values of each execution.
sealed class RelationQueryParameterRef
{
    public RelationQueryParameterRef(int index)
    {
        Index = index;
    }

    public int Index { get; }

    public static object? Resolve(object? value, object?[]? values) =>
        value is RelationQueryParameterRef parameter ? values![parameter.Index] : value;
}

/// Parameter values and the expression nodes they came from, while templates convert expressions and compile plans.
sealed class RelationQueryParameterScope : IDisposable
{
    [ThreadStatic] static RelationQueryParameterScope? _current;

    readonly RelationQueryParameterScope? _previous;
    readonly object?[] _values;
    readonly RelationQueryExecution _execution;
    readonly HashSet<int> _baked = [];

    RelationQueryParameterScope(RelationQueryExecution execution)
    {
        _execution = execution;
        _values = execution.Values!;
        _previous = _current;
        _current = this;
    }

    public static RelationQueryParameterScope? Enter(RelationQueryExecution execution) =>
        execution.Values == null ? null : new RelationQueryParameterScope(execution);

    public IReadOnlyCollection<int> Baked => _baked;

    /// Values of the current scope, or null outside of one.
    public static object?[]? CurrentValues => _current?._values;

    public static object? Bake(int index)
    {
        var scope = _current ?? throw OutsideScope();
        scope._baked.Add(index);
        return scope._values[index];
    }

    public static object? Peek(int index)
    {
        var scope = _current ?? throw OutsideScope();
        return scope._values[index];
    }

    static InvalidOperationException OutsideScope() => new("Relation query parameter read outside of a scope.");

    /// The parameter node for an expression the converter would evaluate, when it is a parameter.
    public static ParameterQueryNode? TryGetParameter(Expression expression)
    {
        var scope = _current;
        if (scope == null || !scope._execution.RootIndexes.TryGetValue(expression, out var index)) return null;
        return new(index, expression.Type);
    }

    /// Evaluating anything within a parameter makes its value part of the result.
    public static void BakeIfParameter(Expression expression)
    {
        var scope = _current;
        if (scope != null && scope._execution.NodeIndexes.TryGetValue(expression, out var index))
            scope._baked.Add(index);
    }

    public void Dispose()
    {
        // What an enclosing build uses from this one depends on the same values.
        _previous?._baked.UnionWith(_baked);
        _current = _previous;
    }
}

/// Structural key of a query expression with parameters replaced by placeholders.
sealed class RelationQueryShape : IEquatable<RelationQueryShape>
{
    readonly object?[] _tokens;
    readonly int _count;
    readonly int _hash;

    public RelationQueryShape(object?[] tokens, int count, int hash)
    {
        _tokens = tokens;
        _count = count;
        _hash = hash;
    }

    /// A copy owning its tokens, for a shape built over a reused buffer.
    public RelationQueryShape Freeze() => new(_tokens.AsSpan(0, _count).ToArray(), _count, _hash);

    public bool Equals(RelationQueryShape? other)
    {
        if (other is null || other._hash != _hash || other._count != _count) return false;
        for (var i = 0; i < _count; i++)
        {
            var a = _tokens[i];
            var b = other._tokens[i];
            if (!ReferenceEquals(a, b) && (a is Type || !Equals(a, b))) return false;
        }

        return true;
    }

    public override bool Equals(object? obj) => obj is RelationQueryShape other && Equals(other);
    public override int GetHashCode() => _hash;
}

/// Walks a query expression once, producing its shape and the parameters: maximal subtrees which depend neither on a
/// lambda parameter nor on the query root and are not literals.
sealed class RelationQueryShapeBuilder
{
    const ulong RootBit = 1ul << 63;
    static readonly object[] NodeTypes = CreateNodeTypes();
    static readonly object RootToken = new();
    static readonly object NullToken = new();
    static readonly object ParameterToken = new();
    static readonly object NullValueToken = new();
    static readonly object ValueToken = new();
    static readonly object TrueToken = true;
    static readonly object FalseToken = false;

    [ThreadStatic] static RelationQueryShapeBuilder? _cached;

    static readonly object[] SmallInts = CreateSmallInts();

    object?[] _tokens = new object?[64];
    int _count;
    int _hash;
    readonly List<ParameterExpression> _lambdaParameters = [];
    readonly List<Expression> _parameters = [];
    bool _unsupported;

    static object[] CreateSmallInts()
    {
        var result = new object[64];
        for (var i = 0; i < result.Length; i++) result[i] = i;
        return result;
    }

    // The hash is built while adding: reflection objects in expressions are reused instances whose own hashing is
    // slow, so they hash by identity, and small numbers use shared boxes so they compare by reference.
    void Add(object? token, int hash)
    {
        if (_count == _tokens.Length) Array.Resize(ref _tokens, _count * 2);
        _tokens[_count++] = token;
        _hash = (_hash * 31) ^ hash;
    }

    void Add(MemberInfo? member) => Add(member, member == null ? 0 : RuntimeHelpers.GetHashCode(member));
    void Add(int value) => Add((uint)value < SmallInts.Length ? SmallInts[value] : value, value);
    void AddNodeType(ExpressionType nodeType) => Add(NodeTypes[(int)nodeType], (int)nodeType);
    void AddMarker(object marker) => Add(marker, RuntimeHelpers.GetHashCode(marker));
    void AddLiteral(object? value) => Add(value, value?.GetHashCode() ?? 0);

    static object[] CreateNodeTypes()
    {
        var result = new object[(int)ExpressionType.IsFalse + 1];
        for (var i = 0; i < result.Length; i++) result[i] = (ExpressionType)i;
        return result;
    }

    /// False when the expression contains something the shape cannot describe; it is then not cached. The shape
    /// uses a buffer reused by the next call on this thread, so it must be frozen to be kept.
    public static bool TryBuild(Expression expression, out RelationQueryShape shape, out Expression[] parameters,
        out object?[] values)
    {
        // Evaluating parameters can run user code, which may build another shape on this thread.
        var builder = _cached ?? new RelationQueryShapeBuilder();
        _cached = null;
        builder.Reset();
        try
        {
            builder.Walk(expression);
            if (builder._unsupported)
            {
                shape = null!;
                parameters = [];
                values = [];
                return false;
            }

            parameters = builder._parameters.ToArray();
            values = new object?[parameters.Length];
            for (var i = 0; i < parameters.Length; i++)
            {
                var value = RelationQueryAstBuilder.EvaluateConstant(parameters[i]);
                values[i] = value;
                var type = parameters[i].Type;
                if (!type.IsValueType || Nullable.GetUnderlyingType(type) != null)
                    builder.AddMarker(value == null ? NullValueToken : ValueToken);
            }

            shape = new(builder._tokens, builder._count, builder._hash);
            return true;
        }
        finally
        {
            _cached = builder;
        }
    }

    void Reset()
    {
        Array.Clear(_tokens, 0, _count);
        _count = 0;
        _hash = 0;
        _lambdaParameters.Clear();
        _parameters.Clear();
        _unsupported = false;
    }

    ulong Walk(Expression? expression)
    {
        if (expression == null)
        {
            AddMarker(NullToken);
            return 0;
        }

        var start = _count;
        var parameterStart = _parameters.Count;
        var nodeType = expression.NodeType;
        var mask = WalkNode(expression, nodeType, out var literal);
        // Lambdas are converted, not evaluated; spans cannot be boxed, so their operands are the parameters.
        if (mask != 0 || literal || _unsupported || nodeType is ExpressionType.Quote or ExpressionType.Lambda ||
            expression.Type.IsByRefLike)
            return mask;
        // Replaced by a parameter; its node type stays part of the shape, because conversions inspect it.
        Array.Clear(_tokens, start, _count - start);
        _count = start;
        _parameters.RemoveRange(parameterStart, _parameters.Count - parameterStart);
        AddParameter(expression);
        return 0;
    }

    void AddParameter(Expression expression)
    {
        AddMarker(ParameterToken);
        AddNodeType(expression.NodeType);
        Add(expression.Type);
        _parameters.Add(expression);
    }

    ulong WalkNode(Expression expression, ExpressionType nodeType, out bool literal)
    {
        literal = false;
        if ((int)nodeType >= NodeTypes.Length)
        {
            _unsupported = true;
            return 0;
        }

        AddNodeType(nodeType);
        switch (nodeType)
        {
            case ExpressionType.Constant:
            {
                var constant = (ConstantExpression)expression;
                var value = constant.Value;
                Add(constant.Type);
                if (value is IQueryable)
                {
                    AddMarker(RootToken);
                    return RootBit;
                }

                literal = IsLiteralValue(value);
                AddLiteral(literal ? value : null);
                return 0;
            }
            case ExpressionType.Parameter:
            {
                var index = _lambdaParameters.IndexOf((ParameterExpression)expression);
                if (index is < 0 or >= 63)
                {
                    _unsupported = true;
                    return 0;
                }

                Add(index);
                return 1ul << index;
            }
            case ExpressionType.Lambda:
            {
                var lambda = (LambdaExpression)expression;
                Add(lambda.Type);
                var first = _lambdaParameters.Count;
                var parameters = lambda.Parameters;
                for (var i = 0; i < parameters.Count; i++) _lambdaParameters.Add(parameters[i]);
                var mask = Walk(lambda.Body);
                // Parameter ordinals stay reserved, so later lambdas get new ones and the shape stays unambiguous.
                for (var i = first; i < _lambdaParameters.Count && i < 63; i++) mask &= ~(1ul << i);
                return mask;
            }
            case ExpressionType.MemberAccess:
            {
                var member = (MemberExpression)expression;
                Add(member.Member);
                return Walk(member.Expression);
            }
            case ExpressionType.Call:
            {
                var call = (MethodCallExpression)expression;
                Add(call.Method);
                if (call.Method.DeclaringType != typeof(Queryable))
                    return Walk(call.Object) | WalkList(call.Arguments);
                // Queryable operators wrap values like Skip and Take counts in constants, which are parameters
                // too. Skip of zero or less is dropped from the query, so it stays a literal.
                var arguments = call.Arguments;
                Add(arguments.Count);
                var mask = Walk(arguments[0]);
                for (var i = 1; i < arguments.Count; i++)
                {
                    if (arguments[i] is ConstantExpression { Value: not IQueryable } constant &&
                        !(constant.Value is int count && count <= 0 && call.Method.Name == nameof(Queryable.Skip)))
                        AddParameter(constant);
                    else mask |= Walk(arguments[i]);
                }

                return mask;
            }
            case ExpressionType.Conditional:
            {
                var conditional = (ConditionalExpression)expression;
                Add(conditional.Type);
                return Walk(conditional.Test) | Walk(conditional.IfTrue) | Walk(conditional.IfFalse);
            }
            case ExpressionType.New:
                return WalkNew((NewExpression)expression);
            case ExpressionType.MemberInit:
            {
                var memberInit = (MemberInitExpression)expression;
                var mask = WalkNew(memberInit.NewExpression);
                Add(memberInit.Bindings.Count);
                foreach (var binding in memberInit.Bindings)
                {
                    if (binding is not MemberAssignment assignment)
                    {
                        _unsupported = true;
                        return 0;
                    }

                    Add(assignment.Member);
                    mask |= Walk(assignment.Expression);
                }

                return mask;
            }
            case ExpressionType.NewArrayInit:
            case ExpressionType.NewArrayBounds:
            {
                var newArray = (NewArrayExpression)expression;
                Add(newArray.Type);
                return WalkList(newArray.Expressions);
            }
            case ExpressionType.TypeIs:
            case ExpressionType.TypeEqual:
            {
                var typeBinary = (TypeBinaryExpression)expression;
                Add(typeBinary.TypeOperand);
                return Walk(typeBinary.Expression);
            }
            case ExpressionType.Invoke:
            {
                var invocation = (InvocationExpression)expression;
                return Walk(invocation.Expression) | WalkList(invocation.Arguments);
            }
            case ExpressionType.Default:
                Add(expression.Type);
                return 0;
        }

        switch (expression)
        {
            case UnaryExpression unary:
                Add(unary.Type);
                Add(unary.Method);
                return Walk(unary.Operand);
            case BinaryExpression binary:
                if (binary.Conversion != null)
                {
                    _unsupported = true;
                    return 0;
                }

                Add(binary.Type);
                Add(binary.Method);
                AddMarker(binary.IsLiftedToNull ? TrueToken : FalseToken);
                return Walk(binary.Left) | Walk(binary.Right);
            default:
                _unsupported = true;
                return 0;
        }
    }

    ulong WalkNew(NewExpression newExpression)
    {
        Add(newExpression.Type);
        Add(newExpression.Constructor);
        Add(newExpression.Members?.Count ?? -1);
        if (newExpression.Members != null)
            foreach (var member in newExpression.Members)
                Add(member);
        return WalkList(newExpression.Arguments);
    }

    ulong WalkList(IReadOnlyList<Expression> expressions)
    {
        Add(expressions.Count);
        var mask = 0ul;
        for (var i = 0; i < expressions.Count; i++) mask |= Walk(expressions[i]);
        return mask;
    }

    internal static bool IsLiteralValue(object? value) =>
        value is null or string or bool or char or byte or sbyte or short or ushort or int or uint or long or ulong
            or float or double or decimal or Half or DateTime or DateTimeOffset or TimeSpan or Guid or Enum;
}

/// Bounded cache of query templates for one relation, keyed by expression shape.
sealed class RelationQueryCache
{
    /// Shapes kept per relation; the least recently used quarter is dropped when the cache grows past it.
    internal const int Capacity = 256;

    readonly ConcurrentDictionary<RelationQueryShape, RelationQueryTemplate> _templates = new();
    readonly object _versionStamp;
    long _clock;

    public RelationQueryCache(object versionStamp)
    {
        _versionStamp = versionStamp;
    }

    public object VersionStamp => _versionStamp;

    internal int Count => _templates.Count;

    /// Conversions and plans reused from templates, and built because no template had them.
    internal long Hits;
    internal long Misses;

    public RelationQueryExecution Start(Expression expression)
    {
        if (!RelationQueryShapeBuilder.TryBuild(expression, out var shape, out var parameters, out var values))
            return new(null, null, null, expression, []);
        var tick = Interlocked.Increment(ref _clock);
        if (!_templates.TryGetValue(shape, out var template))
        {
            template = _templates.GetOrAdd(shape.Freeze(), static _ => new());
            if (_templates.Count > Capacity) Evict();
        }

        template.LastUsed = tick;
        return new(this, template, values, expression, parameters);
    }

    void Evict()
    {
        lock (_templates)
        {
            if (_templates.Count <= Capacity) return;
            var byAge = _templates.ToArray();
            Array.Sort(byAge, static (a, b) => a.Value.LastUsed.CompareTo(b.Value.LastUsed));
            for (var i = 0; i < byAge.Length - Capacity * 3 / 4; i++) _templates.TryRemove(byAge[i].Key, out _);
        }
    }
}

/// What executions of one expression shape convert and compile, each valid for the parameter values it baked.
sealed class RelationQueryTemplate
{
    // Entries kept per key: values baked into them can differ between executions.
    const int VariantsPerKey = 8;

    // Variant arrays are replaced, never changed, so lookups need no lock.
    readonly ConcurrentDictionary<object, Variant[]> _entries = new();

    public long LastUsed;

    sealed record Variant(int[] Baked, object?[] Values, object Result);

    public bool TryGet(object key, object?[] values, out object result)
    {
        if (_entries.TryGetValue(key, out var variants))
        {
            foreach (var variant in variants)
            {
                if (!Matches(variant, values)) continue;
                result = variant.Result;
                return true;
            }
        }

        result = null!;
        return false;
    }

    public void Add(object key, int[] baked, object?[] values, object result)
    {
        var bakedValues = new object?[baked.Length];
        for (var i = 0; i < baked.Length; i++) bakedValues[i] = values[baked[i]];
        var variant = new Variant(baked, bakedValues, result);
        _entries.AddOrUpdate(key, static (_, v) => [v], static (_, existing, v) =>
        {
            var keep = Math.Min(existing.Length, VariantsPerKey - 1);
            var updated = new Variant[keep + 1];
            Array.Copy(existing, existing.Length - keep, updated, 0, keep);
            updated[keep] = v;
            return updated;
        }, variant);
    }

    static bool Matches(Variant variant, object?[] values)
    {
        var baked = variant.Baked;
        for (var i = 0; i < baked.Length; i++)
            if (!Equals(variant.Values[i], values[baked[i]]))
                return false;
        return true;
    }
}

/// One execution of a query: its parameter values and the template that conversions and plans are memoized in.
sealed class RelationQueryExecution
{
    readonly RelationQueryCache? _cache;
    readonly RelationQueryTemplate? _template;
    readonly Expression _expression;
    readonly Expression[] _parameters;
    Dictionary<Expression, int>? _rootIndexes;
    Dictionary<Expression, int>? _nodeIndexes;

    public RelationQueryExecution(RelationQueryCache? cache, RelationQueryTemplate? template, object?[]? values,
        Expression expression, Expression[] parameters)
    {
        _cache = cache;
        _template = template;
        Values = values;
        _expression = expression;
        _parameters = parameters;
    }

    /// Parameter values of this execution; null when it is not cached.
    public object?[]? Values { get; }

    /// Something this execution built could not be cached, so what depends on it is not cached either.
    bool _uncached;

    internal bool IsCached => _template != null && !_uncached;

    public Dictionary<Expression, int> RootIndexes
    {
        get
        {
            if (_rootIndexes != null) return _rootIndexes;
            var result = new Dictionary<Expression, int>(ReferenceEqualityComparer.Instance);
            for (var i = 0; i < _parameters.Length; i++) result.TryAdd(_parameters[i], i);
            return _rootIndexes = result;
        }
    }

    public Dictionary<Expression, int> NodeIndexes
    {
        get
        {
            if (_nodeIndexes != null) return _nodeIndexes;
            var result = new Dictionary<Expression, int>(ReferenceEqualityComparer.Instance);
            for (var i = 0; i < _parameters.Length; i++) new NodeCollector(result, i).Visit(_parameters[i]);
            return _nodeIndexes = result;
        }
    }

    /// Returns what create builds for this site, reusing it from earlier executions when the values it baked match.
    public TResult Memo<TResult>(object key, Func<TResult> create) where TResult : class
    {
        if (_template == null) return create();
        if (!_uncached && _template.TryGet(key, Values!, out var cached))
        {
            Interlocked.Increment(ref _cache!.Hits);
            return (TResult)cached;
        }

        Interlocked.Increment(ref _cache!.Misses);
        int[] baked;
        TResult result;
        using (var scope = RelationQueryParameterScope.Enter(this))
        {
            result = create();
            baked = scope!.Baked.ToArray();
        }

        if (_uncached || !CanStore(result, baked))
        {
            _uncached = true;
            return result;
        }

        _template.Add(key, baked, Values!, result);
        return result;
    }

    bool CanStore(object result, int[] baked)
    {
        if (result is RelationQueryPlan { Cacheable: false }) return false;
        foreach (var index in baked)
            if (!RelationQueryShapeBuilder.IsLiteralValue(Values![index]))
                return false;
        return true;
    }

    sealed class NodeCollector : ExpressionVisitor
    {
        readonly Dictionary<Expression, int> _nodes;
        readonly int _index;

        public NodeCollector(Dictionary<Expression, int> nodes, int index)
        {
            _nodes = nodes;
            _index = index;
        }

        public override Expression? Visit(Expression? node)
        {
            if (node != null) _nodes.TryAdd(node, _index);
            return base.Visit(node);
        }
    }
}
