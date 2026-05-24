using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using BTDB.FieldHandler;
using BTDB.Interpreter;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.Serialization;

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

class EqualsQueryNode : QueryNode
{
    public EqualsQueryNode(QueryNode left, QueryNode right) : base(typeof(bool))
    {
        Left = left;
        Right = right;
    }

    public QueryNode Left { get; }
    public QueryNode Right { get; }
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
        var context = RelationQueryEvaluationContext.Create<T>(ast);
        return RelationQueryAstInterpreter.Interpret<T>(_transaction, _relationInfo, _loaderInfo, ast, context)
            .GetEnumerator();
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
            return new WhereQueryNode(source, BuildPredicate(lambda.Body, lambda.Parameters[0], query));
        }

        throw new NotSupportedException("Relation query supports only Queryable.Where.");
    }

    static QueryNode BuildPredicate(Expression expression, ParameterExpression parameter,
        QueryByIdQueryNode query)
    {
        return expression.NodeType switch
        {
            ExpressionType.AndAlso => BuildBinary<BoolAndQueryNode>((BinaryExpression)expression, parameter,
                query),
            ExpressionType.OrElse => BuildBinary<BoolOrQueryNode>((BinaryExpression)expression, parameter,
                query),
            ExpressionType.Equal => BuildBinary<EqualsQueryNode>((BinaryExpression)expression, parameter,
                query),
            ExpressionType.Constant => new ConstantQueryNode(((ConstantExpression)expression).Value,
                expression.Type),
            ExpressionType.MemberAccess when TryBuildProp((MemberExpression)expression, parameter, query,
                out var prop) => prop,
            _ when !DependsOnParameter(expression, parameter) => new ConstantQueryNode(EvaluateConstant(expression),
                expression.Type),
            _ => throw new NotSupportedException($"Expression '{expression}' is not supported in relation query.")
        };
    }

    static QueryNode BuildBinary<TNode>(BinaryExpression expression, ParameterExpression parameter,
        QueryByIdQueryNode query) where TNode : QueryNode
    {
        var left = BuildPredicate(expression.Left, parameter, query);
        var right = BuildPredicate(expression.Right, parameter, query);
        return (TNode)Activator.CreateInstance(typeof(TNode), left, right)!;
    }

    static bool TryBuildProp(MemberExpression expression, ParameterExpression parameter, QueryByIdQueryNode query,
        out QueryNode prop)
    {
        prop = null!;
        if (expression.Expression != parameter) return false;
        if (expression.Member is not PropertyInfo propertyInfo)
            throw new NotSupportedException($"Member '{expression.Member.Name}' is not supported in relation query.");
        prop = new PropQueryNode(query, RelationQueryPropertyName.GetPersistentName(propertyInfo),
            propertyInfo.PropertyType);
        return true;
    }

    static Expression StripQuote(Expression expression)
    {
        // Queryable.Where receives Expression<Func<...>> as a quoted lambda argument.
        while (expression.NodeType == ExpressionType.Quote)
            expression = ((UnaryExpression)expression).Operand;
        return expression;
    }

    static bool DependsOnParameter(Expression expression, ParameterExpression parameter)
    {
        var visitor = new ParameterFinder(parameter);
        visitor.Visit(expression);
        return visitor.Found;
    }

    static object? EvaluateConstant(Expression expression)
    {
        return expression switch
        {
            ConstantExpression constant => constant.Value,
            MemberExpression member => EvaluateMember(member),
            UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert =>
                ConvertConstant(EvaluateConstant(convert.Operand), convert.Type),
            _ => throw new NotSupportedException($"Expression '{expression}' is not supported in relation query.")
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

    static object? ConvertConstant(object? value, Type type)
    {
        if (value == null) return null;
        var targetType = Nullable.GetUnderlyingType(type) ?? type;
        if (targetType.IsInstanceOfType(value)) return value;
        if (targetType.IsEnum) return Enum.ToObject(targetType, value);
        return Convert.ChangeType(value, targetType);
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

static class RelationQueryAstInterpreter
{
    public static IEnumerable<T> Interpret<T>(IInternalObjectDBTransaction transaction, RelationInfo relationInfo,
        RelationInfo.ItemLoaderInfo loaderInfo, QueryNode ast, RelationQueryEvaluationContext context) where T : class
    {
        return ast switch
        {
            QueryByIdQueryNode => new RelationEnumerator<T>(transaction, relationInfo.Prefix, loaderInfo),
            WhereQueryNode where => Interpret<T>(transaction, relationInfo, loaderInfo, where.Source, context)
                .Where(item => EvaluateBool(where.Predicate, item, context)),
            _ => throw new NotSupportedException("Relation query source is not supported.")
        };
    }

    static bool EvaluateBool<T>(QueryNode ast, T item, RelationQueryEvaluationContext context) where T : class
    {
        return ast switch
        {
            BoolAndQueryNode and => EvaluateBool(and.Left, item, context) && EvaluateBool(and.Right, item, context),
            BoolOrQueryNode or => EvaluateBool(or.Left, item, context) || EvaluateBool(or.Right, item, context),
            EqualsQueryNode equals => object.Equals(EvaluateValue(equals.Left, item, context),
                EvaluateValue(equals.Right, item, context)),
            ConstantQueryNode { Value: bool value } => value,
            _ => throw new NotSupportedException("Relation query predicate is not supported.")
        };
    }

    static object? EvaluateValue<T>(QueryNode ast, T item, RelationQueryEvaluationContext context) where T : class
    {
        return ast switch
        {
            ConstantQueryNode constant => constant.Value,
            PropQueryNode prop => context.GetAccessor(prop).Get(item),
            _ => EvaluateBool(ast, item, context)
        };
    }
}

class RelationQueryEvaluationContext
{
    readonly Dictionary<PropQueryNode, RelationQueryValueAccessor> _accessors;

    RelationQueryEvaluationContext(Dictionary<PropQueryNode, RelationQueryValueAccessor> accessors)
    {
        _accessors = accessors;
    }

    public static RelationQueryEvaluationContext Create<T>(QueryNode ast) where T : class
    {
        var metadata = ReflectionMetadata.FindByType(typeof(T)) ??
                       throw new BTDBException($"Type {typeof(T).ToSimpleName()} does not have registered metadata.");
        var accessors = new Dictionary<PropQueryNode, RelationQueryValueAccessor>();
        CollectAccessors(ast, metadata, accessors);
        return new(accessors);
    }

    public RelationQueryValueAccessor GetAccessor(PropQueryNode prop)
    {
        return _accessors[prop];
    }

    static void CollectAccessors(QueryNode node, ClassMetadata metadata,
        Dictionary<PropQueryNode, RelationQueryValueAccessor> accessors)
    {
        switch (node)
        {
            case PropQueryNode prop:
                accessors.TryAdd(prop, CreateAccessor(metadata, prop));
                break;
            case WhereQueryNode where:
                CollectAccessors(where.Source, metadata, accessors);
                CollectAccessors(where.Predicate, metadata, accessors);
                break;
            case BoolAndQueryNode and:
                CollectAccessors(and.Left, metadata, accessors);
                CollectAccessors(and.Right, metadata, accessors);
                break;
            case BoolOrQueryNode or:
                CollectAccessors(or.Left, metadata, accessors);
                CollectAccessors(or.Right, metadata, accessors);
                break;
            case EqualsQueryNode equals:
                CollectAccessors(equals.Left, metadata, accessors);
                CollectAccessors(equals.Right, metadata, accessors);
                break;
        }
    }

    static unsafe RelationQueryValueAccessor CreateAccessor(ClassMetadata metadata, PropQueryNode prop)
    {
        var field = metadata.Fields.FirstOrDefault(f => f.Name == prop.Name);
        if (field == null || field.PropRefGetter == null && field.ByteOffset == null)
            throw new BTDBException($"Property {metadata.Type.ToSimpleName()}.{prop.Name} was not found.");
        if (field.ByteOffset != null)
            return RelationQueryValueAccessor.CreateOffsetAccessor(field.Type, field.ByteOffset.Value);
        return RelationQueryValueAccessor.CreateGetterAccessor(field.Type, field.PropRefGetter);
    }
}

abstract class RelationQueryValueAccessor
{
    public abstract object? Get(object item);

    public static RelationQueryValueAccessor CreateOffsetAccessor(Type type, uint offset)
    {
        return (RelationQueryValueAccessor)Activator.CreateInstance(typeof(RelationQueryOffsetAccessor<>)
            .MakeGenericType(type), offset)!;
    }

    public static unsafe RelationQueryValueAccessor CreateGetterAccessor(Type type,
        delegate*<object, ref byte, void> getter)
    {
        return (RelationQueryValueAccessor)Activator.CreateInstance(typeof(RelationQueryGetterAccessor<>)
            .MakeGenericType(type), (nint)getter)!;
    }
}

class RelationQueryOffsetAccessor<T> : RelationQueryValueAccessor
{
    readonly uint _offset;

    public RelationQueryOffsetAccessor(uint offset)
    {
        _offset = offset;
    }

    public override object? Get(object item)
    {
        return Unsafe.As<byte, T>(ref RawData.Ref(item, _offset));
    }
}

unsafe class RelationQueryGetterAccessor<T> : RelationQueryValueAccessor
{
    readonly delegate*<object, ref byte, void> _getter;

    public RelationQueryGetterAccessor(nint getter)
    {
        _getter = (delegate*<object, ref byte, void>)getter;
    }

    public override object? Get(object item)
    {
        T value = default!;
        _getter(item, ref Unsafe.As<T, byte>(ref value));
        return value;
    }
}

static class RelationQueryPropertyName
{
    public static string GetPersistentName(PropertyInfo propertyInfo)
    {
        return propertyInfo.GetCustomAttribute<PersistedNameAttribute>()?.Name ?? propertyInfo.Name;
    }
}
