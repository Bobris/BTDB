using System;
using System.Collections;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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
    public ConstantQueryNode(object? value, Type type) : base(type)
    {
        Value = value;
    }

    public object? Value { get; }
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

class RelationQuery<T> : IQueryable<T>, IQueryProvider where T : class
{
    readonly IInternalObjectDBTransaction _transaction;
    readonly RelationInfo _relationInfo;
    readonly RelationInfo.ItemLoaderInfo _loaderInfo;
    readonly Expression _expression;
    readonly QueryByIdQueryNode _queryNode;

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

    public IEnumerator<T> GetEnumerator()
    {
        var ast = RelationQueryAstBuilder.Build(_expression, _queryNode);
        return RelationQueryExecutionPlan.Create<T>(_transaction, _relationInfo, _loaderInfo, ast).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public Type ElementType => typeof(T);
    public Expression Expression => _expression;
    public IQueryProvider Provider => this;

    public IQueryable CreateQuery(Expression expression)
    {
        ValidateQueryExpression(expression);
        return new RelationQuery<T>(this, expression);
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        if (typeof(TElement) != typeof(T))
            throw new NotSupportedException("Relation query does not support changing element type.");
        ValidateQueryExpression(expression);
        return (IQueryable<TElement>)(object)new RelationQuery<T>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        throw new NotSupportedException("Relation query supports enumeration only.");
    }

    public TResult Execute<TResult>(Expression expression)
    {
        throw new NotSupportedException("Relation query supports enumeration only.");
    }

    static void ValidateQueryExpression(Expression expression)
    {
        if (expression is MethodCallExpression methodCall &&
            methodCall.Method.DeclaringType == typeof(Queryable) &&
            methodCall.Method.Name == nameof(Queryable.Where))
            return;
        if (expression is ConstantExpression)
            return;
        throw new NotSupportedException("Relation query supports only Queryable.Where.");
    }
}

static class RelationQueryAstBuilder
{
    public static QueryNode Build(Expression expression, QueryByIdQueryNode query)
    {
        if (expression is ConstantExpression) return query;
        if (expression is MethodCallExpression methodCall &&
            methodCall.Method.DeclaringType == typeof(Queryable) &&
            methodCall.Method.Name == nameof(Queryable.Where))
        {
            var source = Build(methodCall.Arguments[0], query);
            var lambda = (LambdaExpression)StripQuote(methodCall.Arguments[1]);
            var converter = new ExpressionConverter(lambda.Parameters[0], query);
            return new WhereQueryNode(source, converter.ConvertPredicate(lambda.Body));
        }

        throw new NotSupportedException("Relation query supports only Queryable.Where.");
    }

    static Expression StripQuote(Expression expression)
    {
        // Queryable.Where receives Expression<Func<...>> as a quoted lambda argument.
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
                return new ConstantQueryNode(EvaluateConstant(expression), expression.Type);
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

    static object? EvaluateConstant(Expression expression)
    {
        return expression switch
        {
            ConstantExpression constant => constant.Value,
            MemberExpression member => EvaluateMember(member),
            UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked, Method: null } convert =>
                ConvertConstant(EvaluateConstant(convert.Operand), convert.Type),
            // Anything else independent of the row is evaluated once, by the expression interpreter.
            _ => Expression.Lambda<Func<object?>>(Expression.Convert(expression, typeof(object)))
                .Compile(preferInterpretation: true)()
        };
    }

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

static class RelationQueryExecutionPlan
{
    public static IEnumerable<T> Create<T>(IInternalObjectDBTransaction transaction, RelationInfo relationInfo,
        RelationInfo.ItemLoaderInfo loaderInfo, QueryNode ast) where T : class
    {
        var plan = new RelationQueryCompiler(relationInfo, typeof(T)).Compile(transaction, ast);
        return new RelationQueryEnumerator<T>(transaction, loaderInfo, plan);
    }
}

/// Compiled relation query: a cursor prefix plus interpreter programs evaluated on serialized key and value bytes.
sealed class RelationQueryPlan
{
    public RelationQueryPlan(byte[] cursorPrefix, bool neverMatches, RelationQueryProgram? keyProgram,
        RelationQueryProgram?[]? valuePrograms, int frameSize, int objectCount)
    {
        CursorPrefix = cursorPrefix;
        NeverMatches = neverMatches;
        KeyProgram = keyProgram;
        ValuePrograms = valuePrograms;
        FrameSize = frameSize;
        ObjectCount = objectCount;
    }

    public byte[] CursorPrefix { get; }

    /// A conjunct folded to false, so no row can match.
    public bool NeverMatches { get; }

    /// Tests conjuncts using only primary key fields before the value is read. Null when there are none.
    public RelationQueryProgram? KeyProgram { get; }

    /// Indexed by persisted value version, tests the remaining conjuncts. Null when there are none. A null item is an
    /// unknown version whose rows never match.
    public RelationQueryProgram?[]? ValuePrograms { get; }

    /// Size in bytes of the frame shared by all programs; a multiple of 8.
    public int FrameSize { get; }

    public int ObjectCount { get; }
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

    public RelationQueryPlan Compile(IInternalObjectDBTransaction transaction, QueryNode ast)
    {
        var predicate = ExtractPredicate(ast);
        if (predicate != null) CollectFields(predicate);
        _frameTemporary = AllocFrame(16);
        _objectTemporary = AllocObjectSlot();

        var nodes = new List<QueryNode>();
        if (predicate != null) FlattenConjuncts(Simplify(predicate), nodes);
        var conjuncts = new List<Conjunct>();
        var seenThrowing = false;
        foreach (var node in nodes)
        {
            // A false conjunct makes the query empty unless an earlier conjunct could throw first.
            if (IsFalse(node) && !seenThrowing) return new(_relationInfo.Prefix, true, null, null, 0, 0);
            if (IsTrue(node)) continue;
            var throws = CanThrow(node);
            conjuncts.Add(new(node, conjuncts.Count, throws));
            seenThrowing |= throws;
        }

        // Equalities encoded in the cursor prefix are removed; every row under the prefix satisfies them.
        var cursorPrefix = BuildCursorPrefix(transaction, conjuncts, out var prefixFieldCount);

        // Conjuncts are evaluated in a different order than written, so a throwing conjunct must still see every
        // earlier conjunct evaluated first, and nothing after it may move before it.
        var keyConjuncts = new List<Conjunct>();
        var valueConjuncts = new List<Conjunct>();
        var barrier = false;
        var allPreviousInKey = true;
        foreach (var conjunct in conjuncts)
        {
            if (UsesOnlyPrimaryKeyFields(conjunct.Node) && !barrier && (!conjunct.Throws || allPreviousInKey))
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

        var keyProgram = keyConjuncts.Count == 0
            ? null
            : BuildProgram(keyConjuncts, null, cursorPrefix.Length, prefixFieldCount);
        RelationQueryProgram?[]? valuePrograms = null;
        if (valueConjuncts.Count > 0)
        {
            var maxVersion = 0u;
            foreach (var (version, _) in _relationInfo.RelationVersions) maxVersion = Math.Max(maxVersion, version);
            valuePrograms = new RelationQueryProgram?[maxVersion + 1];
            foreach (var (version, info) in _relationInfo.RelationVersions)
                valuePrograms[version] = BuildProgram(valueConjuncts, info.Fields, cursorPrefix.Length,
                    prefixFieldCount);
        }

        return new(cursorPrefix, false, keyProgram, valuePrograms, _frameSize, _objectCount);
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

    RelationQueryProgram BuildProgram(List<Conjunct> conjuncts, ReadOnlyMemory<TableFieldInfo>? valueFields,
        int cursorPrefixLength, int prefixFieldCount)
    {
        var keyFields = _primaryKeyFields.Span;
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

        if (specialized.Count == 0) return new(null, 0, true);

        var names = new HashSet<string>();
        foreach (var conjunct in specialized) CollectPropertyNames(conjunct.Node, names);
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
        var keyStart = startField == prefixFieldCount ? cursorPrefixLength : _relationInfo.Prefix.Length;

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
        _builder.AddInstruction(OpCode.SetBoolResultTrue);
        _builder.AddInstruction(OpCode.Stop);
        _builder.MarkLabel(falseLabel);
        _builder.AddInstruction(OpCode.SetBoolResultFalse);
        _builder.AddInstruction(OpCode.Stop);
        var program = _builder.Materialize();
        _builder = null!;
        return new(program, keyStart, false);
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

    [SkipLocalsInit]
    byte[] BuildCursorPrefix(IInternalObjectDBTransaction transaction, List<Conjunct> conjuncts,
        out int prefixFieldCount)
    {
        prefixFieldCount = 0;
        var primaryKeyFields = _primaryKeyFields.Span;
        if (primaryKeyFields.Length == 0) return _relationInfo.Prefix;
        var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        writer.WriteBlock(_relationInfo.Prefix);
        IWriterCtx? writerCtx = null;
        foreach (var field in primaryKeyFields)
        {
            var conjunctIndex = FindExactProperty(conjuncts, field.Name, out var property, out var constant);
            if (conjunctIndex < 0) break;
            if (constant.Value == null && property.Type != typeof(string) ||
                constant.Value != null && constant.Value.GetType() != property.Type ||
                !CanSaveConstant(property.Type))
                break;

            if (field.Handler!.NeedsCtx()) writerCtx ??= new DBWriterCtx(transaction);
            var saver = field.Handler.Save(property.Type, _relationInfo.TypeConverterFactory);
            SaveConstant(saver, ref writer, writerCtx, property.Type, constant.Value);
            conjuncts.RemoveAt(conjunctIndex);
            prefixFieldCount++;
        }

        return prefixFieldCount == 0 ? _relationInfo.Prefix : writer.GetSpan().ToArray();
    }

    // Only an equality before every throwing conjunct can move into the prefix.
    static int FindExactProperty(List<Conjunct> conjuncts, string propertyName, out PropQueryNode property,
        out ConstantQueryNode constant)
    {
        for (var i = 0; i < conjuncts.Count; i++)
        {
            if (conjuncts[i].Node is CompareQueryNode { Compare: QueryCompare.Equal } equals &&
                TrySplitPropertyConstant(equals, out property, out constant) && property.Name == propertyName)
                return i;
            if (conjuncts[i].Throws) break;
        }

        property = null!;
        constant = null!;
        return -1;
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

    static bool CanSaveConstant(Type type)
    {
        return type == typeof(string) || type == typeof(bool) || type == typeof(byte) || type == typeof(sbyte) ||
               type == typeof(ushort) || type == typeof(short) || type == typeof(uint) || type == typeof(int) ||
               type == typeof(ulong) || type == typeof(long) || type == typeof(Half) || type == typeof(float) ||
               type == typeof(double) || type == typeof(DateTime) || type == typeof(Guid) ||
               type == typeof(decimal) || type == typeof(TimeSpan) || type == typeof(DateTimeOffset);
    }

    static void SaveConstant(FieldHandlerSave saver, ref MemWriter writer, IWriterCtx? writerCtx, Type type,
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

    bool UsesOnlyPrimaryKeyFields(QueryNode node)
    {
        var names = new HashSet<string>();
        CollectPropertyNames(node, names);
        foreach (var name in names)
            if (!IsPrimaryKeyField(name)) return false;
        return true;
    }

    static void CollectPropertyNames(QueryNode node, HashSet<string> names)
    {
        if (node is PropQueryNode prop) names.Add(prop.Name);
        ForEachChild(node, child => CollectPropertyNames(child, names));
    }
}

sealed class RelationQueryEnumerator<T> : IEnumerator<T>, IEnumerable<T> where T : class
{
    const int ValueBufferSize = 4096;

    readonly IInternalObjectDBTransaction _transaction;
    readonly RelationInfo.ItemLoaderInfo _loaderInfo;
    readonly RelationQueryPlan _plan;
    readonly CursorIterateCallback _testRow;
    readonly long[] _frame;
    readonly object?[] _objects;
    readonly byte[]? _valueBuffer;
    IKeyValueDBCursor? _cursor;
    bool _first = true;
    bool _afterEnd;
    bool _matched;
    bool _skipCurrentRow;
    bool _hasCurrent;
    T? _current;

    public RelationQueryEnumerator(IInternalObjectDBTransaction transaction, RelationInfo.ItemLoaderInfo loaderInfo,
        RelationQueryPlan plan)
    {
        _transaction = transaction;
        _loaderInfo = loaderInfo;
        _plan = plan;
        _testRow = TestRow;
        _frame = new long[plan.FrameSize / sizeof(long)];
        _objects = new object?[plan.ObjectCount];
        if (plan.ValuePrograms != null) _valueBuffer = new byte[ValueBufferSize];
        _cursor = transaction.KeyValueDBTransaction.CreateCursor();
    }

    [SkipLocalsInit]
    public bool MoveNext()
    {
        ObjectDisposedException.ThrowIf(_cursor == null, this);
        _transaction.ThrowIfDisposed();
        _hasCurrent = false;
        _current = null;
        if (_afterEnd) return false;
        var prefix = _plan.CursorPrefix;
        var hasPredicate = _plan.KeyProgram != null || _plan.ValuePrograms != null;
        if (_first)
        {
            _first = false;
            if (_plan.NeverMatches || !_cursor.FindFirstKey(prefix)) return AfterEnd();
            _skipCurrentRow = false;
        }
        else if (!hasPredicate)
        {
            if (!_cursor.FindNextKey(prefix)) return AfterEnd();
        }
        else if (_cursor.IsValid())
        {
            // FastIterate continues from the previously matched row, which is cheaper than moving the cursor; that row
            // itself is skipped.
            _skipCurrentRow = true;
        }
        else
        {
            // The previously matched row was removed, FindNextKey moves to its next sibling.
            if (!_cursor.FindNextKey(prefix)) return AfterEnd();
            _skipCurrentRow = false;
        }

        if (!hasPredicate)
        {
            _hasCurrent = true;
            return true;
        }

        // Tests rows starting at the cursor position without moving the cursor per row; iteration stops on the first
        // matching row or the first key outside the prefix.
        _matched = false;
        Span<byte> keyBuffer = stackalloc byte[512];
        if (_plan.ValuePrograms == null)
            _cursor.FastIterateNoCursor(ref keyBuffer, _testRow);
        else
            _cursor.FastIterate(ref keyBuffer, _testRow);
        if (!_matched) return AfterEnd();
        _hasCurrent = true;
        return true;
    }

    bool AfterEnd()
    {
        _afterEnd = true;
        return false;
    }

    bool TestRow(long keyIndex, ReadOnlySpan<byte> key)
    {
        if (_skipCurrentRow)
        {
            _skipCurrentRow = false;
            return false;
        }

        if (!key.StartsWith(_plan.CursorPrefix)) return true;
        if (!Evaluate(key)) return false;
        _matched = true;
        return true;
    }

    unsafe bool Evaluate(ReadOnlySpan<byte> key)
    {
        ref var frame = ref Unsafe.As<long, byte>(ref MemoryMarshal.GetArrayDataReference(_frame));
        ref var objects = ref MemoryMarshal.GetArrayDataReference(_objects);
        var keyReader = new MemReader();
        var valueReader = new MemReader();
        fixed (byte* keyPointer = key)
        {
            var keyProgram = _plan.KeyProgram;
            if (keyProgram != null)
            {
                if (keyProgram.Program == null)
                {
                    if (!keyProgram.ConstantResult) return false;
                }
                else
                {
                    keyReader = new(keyPointer + keyProgram.KeyStart, key.Length - keyProgram.KeyStart);
                    var ctx = new InterpreterCtx(keyProgram.Program, ref frame, ref objects, ref keyReader,
                        ref valueReader);
                    BTDB.Interpreter.Interpreter.Run(ref ctx);
                    if (!ctx.BoolResult) return false;
                }
            }

            var valuePrograms = _plan.ValuePrograms;
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
                keyReader = new(keyPointer + program.KeyStart, key.Length - program.KeyStart);
                var ctx = new InterpreterCtx(program.Program, ref frame, ref objects, ref keyReader, ref valueReader);
                BTDB.Interpreter.Interpreter.Run(ref ctx);
                return ctx.BoolResult;
            }
        }
    }

    public T Current
    {
        [SkipLocalsInit]
        get
        {
            if (!_hasCurrent) throw new InvalidOperationException("Enumerator is not positioned on a row.");
            if (_current != null) return _current;
            Span<byte> keyBuffer = stackalloc byte[2048];
            var key = _cursor!.GetKeySpan(ref keyBuffer);
            return _current = (T)_loaderInfo.CreateInstance(_transaction, _cursor, key);
        }
    }

    object IEnumerator.Current => Current;

    public void Reset()
    {
        ObjectDisposedException.ThrowIf(_cursor == null, this);
        _cursor.Invalidate();
        _first = true;
        _afterEnd = false;
        _hasCurrent = false;
        _current = null;
    }

    public void Dispose()
    {
        _cursor?.Dispose();
        _cursor = null;
        Array.Clear(_objects);
    }

    public IEnumerator<T> GetEnumerator() => this;
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

static class RelationQueryPropertyName
{
    public static string GetPersistentName(PropertyInfo propertyInfo)
    {
        return propertyInfo.GetCustomAttribute<PersistedNameAttribute>()?.Name ?? propertyInfo.Name;
    }
}
