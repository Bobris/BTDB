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
            return new WhereQueryNode(source, BuildPredicate(lambda.Body, lambda.Parameters[0], query));
        }

        throw new NotSupportedException("Relation query supports only Queryable.Where.");
    }

    static QueryNode BuildPredicate(Expression expression, ParameterExpression parameter,
        QueryByIdQueryNode query)
    {
        return expression.NodeType switch
        {
            ExpressionType.AndAlso => BuildBinary((BinaryExpression)expression, parameter, query,
                static (left, right) => new BoolAndQueryNode(left, right)),
            ExpressionType.OrElse => BuildBinary((BinaryExpression)expression, parameter, query,
                static (left, right) => new BoolOrQueryNode(left, right)),
            ExpressionType.Equal => BuildBinary((BinaryExpression)expression, parameter, query,
                static (left, right) => new EqualsQueryNode(left, right)),
            ExpressionType.Constant => new ConstantQueryNode(((ConstantExpression)expression).Value,
                expression.Type),
            ExpressionType.MemberAccess when TryBuildProp((MemberExpression)expression, parameter, query,
                out var prop) => prop,
            _ when !DependsOnParameter(expression, parameter) => new ConstantQueryNode(EvaluateConstant(expression),
                expression.Type),
            _ => throw new NotSupportedException($"Expression '{expression}' is not supported in relation query.")
        };
    }

    static QueryNode BuildBinary(BinaryExpression expression, ParameterExpression parameter,
        QueryByIdQueryNode query, Func<QueryNode, QueryNode, QueryNode> create)
    {
        var left = BuildPredicate(expression.Left, parameter, query);
        var right = BuildPredicate(expression.Right, parameter, query);
        return create(left, right);
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

static class RelationQueryExecutionPlan
{
    public static IEnumerable<T> Create<T>(IInternalObjectDBTransaction transaction, RelationInfo relationInfo,
        RelationInfo.ItemLoaderInfo loaderInfo, QueryNode ast) where T : class
    {
        var compiler = new RelationQueryPredicateCompiler<T>(transaction, relationInfo, ast);
        return new RelationQueryEnumerator<T>(transaction, loaderInfo, compiler);
    }
}

sealed class RelationQueryFieldSlot
{
    public RelationQueryFieldSlot(Type type)
    {
        Type = type;
    }

    public Type Type { get; }
    public uint ByteOffset { get; set; }
    public uint ObjectIndex { get; set; }
    public bool IsObject => Type == typeof(string);

    public void EmitSetResult(InterpreterBuilder builder, ulong bytesPointerOffset, ulong objectsPointerOffset)
    {
        builder.AddInstruction(OpCode.LoadResultBySP);
        builder.AddVUInt64(IsObject ? objectsPointerOffset : bytesPointerOffset);
        var offset = IsObject ? ObjectIndex * (uint)Unsafe.SizeOf<nint>() : ByteOffset;
        if (offset == 0) return;
        builder.AddInstruction(OpCode.IncrementRefResult);
        builder.AddVUInt64(offset);
    }

    public void EmitSetParam1(InterpreterBuilder builder, ulong bytesPointerOffset, ulong objectsPointerOffset)
    {
        builder.AddInstruction(OpCode.LoadParam1BySP);
        builder.AddVUInt64(IsObject ? objectsPointerOffset : bytesPointerOffset);
        var offset = IsObject ? ObjectIndex * (uint)Unsafe.SizeOf<nint>() : ByteOffset;
        if (offset == 0) return;
        builder.AddInstruction(OpCode.IncrementRefParam1);
        builder.AddVUInt64(offset);
    }
}

sealed class RelationQueryPredicateCompiler<T> where T : class
{
    readonly RelationInfo _relationInfo;
    readonly Dictionary<string, RelationQueryFieldSlot> _fields = new();
    readonly RelationQueryFieldSlot _version = new(typeof(uint));
    uint _bytesSize;
    uint _temporaryOffset;
    uint _objectCount;
    uint _stringScratch2;

    const ulong PointerStackCount = 4;
    static readonly ulong PointerSize = (ulong)Unsafe.SizeOf<nint>();
    static readonly ulong PointerStackSize = PointerStackCount * PointerSize;
    static readonly ulong KeyReaderPointerOffset = PointerStackSize;
    static readonly ulong ValueReaderPointerOffset = PointerStackSize - PointerSize;
    static readonly ulong BytesPointerOffset = PointerStackSize - 2 * PointerSize;
    static readonly ulong ObjectsPointerOffset = PointerStackSize - 3 * PointerSize;

    public RelationQueryPredicateCompiler(IInternalObjectDBTransaction transaction, RelationInfo relationInfo,
        QueryNode ast)
        : this(relationInfo, ExtractPredicate(ast), true)
    {
        CursorPrefix = BuildCursorPrefix(transaction, relationInfo, ExtractPredicate(ast));
    }

    RelationQueryPredicateCompiler(RelationInfo relationInfo, QueryNode? predicate, bool splitKeyPredicate)
    {
        _relationInfo = relationInfo;
        CursorPrefix = relationInfo.Prefix;
        if (splitKeyPredicate)
        {
            SplitKeyPredicate(predicate, relationInfo.ClientRelationVersionInfo.PrimaryKeyFields.Span,
                out var keyPredicate, out predicate);
            if (keyPredicate != null) KeyCompiler = new(relationInfo, keyPredicate, false);
        }

        HasPredicate = predicate != null;
        CollectFields(predicate);
        ConfigureSlots();
        foreach (var field in relationInfo.ClientRelationVersionInfo.PrimaryKeyFields.Span)
        {
            if (!_fields.ContainsKey(field.Name)) continue;
            HasKeyFields = true;
            break;
        }
        foreach (var field in relationInfo.ClientRelationVersionInfo.Fields.Span)
        {
            if (!_fields.ContainsKey(field.Name)) continue;
            HasValueFields = true;
            break;
        }
        Program = BuildProgram(predicate);
    }

    public InterpreterProgram Program { get; }
    public byte[] CursorPrefix { get; }
    public bool HasKeyFields { get; }
    public bool HasValueFields { get; }
    public bool HasPredicate { get; }
    public RelationQueryPredicateCompiler<T>? KeyCompiler { get; }

    void CollectFields(QueryNode? node)
    {
        if (node == null) return;
        switch (node)
        {
            case PropQueryNode prop:
                if (_fields.ContainsKey(prop.Name)) return;
                var tableField = _relationInfo.ClientRelationVersionInfo[prop.Name] ??
                                 throw new BTDBException($"Property {typeof(T).ToSimpleName()}.{prop.Name} was not found.");
                if (tableField.Computed)
                    throw new NotSupportedException($"Computed property {typeof(T).ToSimpleName()}.{prop.Name} is not supported in relation query.");
                EnsureSupportedType(prop.Type);
                _fields.Add(prop.Name, new(prop.Type));
                break;
            case BoolAndQueryNode and:
                CollectFields(and.Left);
                CollectFields(and.Right);
                break;
            case BoolOrQueryNode or:
                CollectFields(or.Left);
                CollectFields(or.Right);
                break;
            case EqualsQueryNode equals:
                CollectFields(equals.Left);
                CollectFields(equals.Right);
                break;
        }
    }

    void ConfigureSlots()
    {
        _version.ByteOffset = 0;
        var byteOffset = (uint)Unsafe.SizeOf<uint>();
        foreach (var field in _fields.Values)
        {
            if (field.IsObject)
            {
                field.ObjectIndex = _objectCount++;
                continue;
            }

            var size = SizeOf(field.Type);
            byteOffset = AlignUp(byteOffset, Math.Min(size, 8));
            field.ByteOffset = byteOffset;
            byteOffset += size;
        }

        _temporaryOffset = AlignUp(byteOffset, 8);
        _bytesSize = _temporaryOffset + 16;
        _stringScratch2 = _objectCount++;
        if (_objectCount > 16)
            throw new NotSupportedException("Relation query supports at most 15 referenced object fields.");
    }

    static void EnsureSupportedType(Type type)
    {
        if (type == typeof(string) || type == typeof(bool) || type == typeof(byte) || type == typeof(sbyte) ||
            type == typeof(ushort) || type == typeof(short) || type == typeof(uint) || type == typeof(int) ||
            type == typeof(ulong) || type == typeof(long) || type == typeof(Half) || type == typeof(float) ||
            type == typeof(double) || type == typeof(DateTime)) return;
        throw new NotSupportedException($"Relation query does not support fields of type {type.ToSimpleName()} yet.");
    }

    static uint SizeOf(Type type)
    {
        if (type == typeof(bool) || type == typeof(byte) || type == typeof(sbyte)) return 1;
        if (type == typeof(ushort) || type == typeof(short) || type == typeof(Half)) return 2;
        if (type == typeof(uint) || type == typeof(int) || type == typeof(float)) return 4;
        return 8;
    }

    static uint AlignUp(uint value, uint alignment) => (value + alignment - 1) / alignment * alignment;

    InterpreterProgram BuildProgram(QueryNode? predicate)
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.PushStack);
        builder.AddVUInt64(PointerStackSize);
        builder.AddInstruction(OpCode.StoreParam1BySP);
        builder.AddVUInt64(KeyReaderPointerOffset);
        builder.AddInstruction(OpCode.StoreParam2BySP);
        builder.AddVUInt64(ValueReaderPointerOffset);

        builder.AddInstruction(OpCode.StackBytesAlloc);
        builder.AddVUInt64(_bytesSize);
        builder.AddInstruction(OpCode.StoreParam2BySP);
        builder.AddVUInt64(BytesPointerOffset);
        builder.AddInstruction(OpCode.StackAllocObject);
        builder.AddVUInt64(_objectCount);
        builder.AddInstruction(OpCode.StoreParam2BySP);
        builder.AddVUInt64(ObjectsPointerOffset);

        if (HasKeyFields)
        {
            EmitSkipKeyPrefix(builder);
            EmitReadFields(builder, _relationInfo.ClientRelationVersionInfo.PrimaryKeyFields.Span,
                KeyReaderPointerOffset);
        }

        if (HasValueFields)
        {
            builder.AddInstruction(OpCode.LoadParam1BySP);
            builder.AddVUInt64(ValueReaderPointerOffset);
            _version.EmitSetResult(builder, BytesPointerOffset, ObjectsPointerOffset);
            builder.AddInstruction(OpCode.CodeOps);
            builder.AddCodeOp(CodeOp.ReadVUInt32);

            var predicateLabel = builder.DeclareLabel("predicate");
            foreach (var (version, info) in _relationInfo.RelationVersions)
            {
                var nextVersionLabel = builder.DeclareLabel($"notVersion{version}");
                _version.EmitSetParam1(builder, BytesPointerOffset, ObjectsPointerOffset);
                AddConstant(builder, version);
                builder.AddInstruction(OpCode.EqualUInt32);
                builder.AddInstruction(OpCode.JmpIfFalse);
                builder.AddLabelParameter(nextVersionLabel);
                EmitReadFields(builder, info.Fields.Span, ValueReaderPointerOffset);
                builder.AddInstruction(OpCode.Jmp);
                builder.AddLabelParameter(predicateLabel);
                builder.MarkLabel(nextVersionLabel);
            }

            builder.AddInstruction(OpCode.SetBoolResultFalse);
            builder.AddInstruction(OpCode.Jmp);
            var finishLabel = builder.DeclareLabel("finish");
            builder.AddLabelParameter(finishLabel);
            builder.MarkLabel(predicateLabel);
            EmitPredicateOrTrue(builder, predicate);
            builder.MarkLabel(finishLabel);
        }
        else
        {
            EmitPredicateOrTrue(builder, predicate);
        }

        builder.AddInstruction(OpCode.PopStack);
        builder.AddVUInt64(PointerStackSize);
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.Stop);
        return builder.Materialize();
    }

    void EmitPredicateOrTrue(InterpreterBuilder builder, QueryNode? predicate)
    {
        if (predicate == null)
            builder.AddInstruction(OpCode.SetBoolResultTrue);
        else
            EmitPredicate(builder, predicate);
    }

    void EmitSkipKeyPrefix(InterpreterBuilder builder)
    {
        builder.AddInstruction(OpCode.LoadParam1BySP);
        builder.AddVUInt64(KeyReaderPointerOffset);
        AddConstant(builder, (uint)_relationInfo.Prefix.Length);
        builder.AddInstruction(OpCode.CodeOps);
        builder.AddCodeOp(CodeOp.SkipBlock);
    }

    void EmitReadFields(InterpreterBuilder builder, ReadOnlySpan<TableFieldInfo> fields, ulong readerPointerOffset)
    {
        var lastNeeded = -1;
        for (var i = 0; i < fields.Length; i++)
            if (_fields.ContainsKey(fields[i].Name)) lastNeeded = i;

        for (var i = 0; i <= lastNeeded; i++)
        {
            var field = fields[i];
            if (field.Computed) continue;
            builder.AddInstruction(OpCode.LoadParam1BySP);
            builder.AddVUInt64(readerPointerOffset);
            if (!_fields.TryGetValue(field.Name, out var target))
            {
                builder.AddInstruction(OpCode.CodeOps);
                builder.AddCodeOp(GetSkipCodeOp(field.Handler!));
                continue;
            }

            var sourceType = field.Handler!.HandledType()!;
            if (sourceType == target.Type)
            {
                target.EmitSetResult(builder, BytesPointerOffset, ObjectsPointerOffset);
                builder.AddInstruction(OpCode.CodeOps);
                builder.AddCodeOp(GetReadCodeOp(field.Handler));
                continue;
            }

            EmitSetResultToBytes(builder, _temporaryOffset);
            builder.AddInstruction(OpCode.CodeOps);
            builder.AddCodeOp(GetReadCodeOp(field.Handler));
            builder.AddInstruction(OpCode.AssignRefParam1Result);
            target.EmitSetResult(builder, BytesPointerOffset, ObjectsPointerOffset);
            builder.AddInstruction(OpCode.ConvertParam1ToResult);
            builder.AddConverterParameter(_relationInfo.TypeConverterFactory, sourceType, target.Type);
        }
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
            _ => throw new NotSupportedException(
                $"Field handler {handler.Name} cannot load a relation query field yet.")
        };
    }

    static CodeOp GetSkipCodeOp(IFieldHandler handler)
    {
        return handler.Name switch
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
            _ => throw new NotSupportedException(
                $"Field handler {handler.Name} cannot skip a relation query field yet.")
        };
    }

    static void EmitSetResultToBytes(InterpreterBuilder builder, uint offset)
    {
        builder.AddInstruction(OpCode.LoadResultBySP);
        builder.AddVUInt64(BytesPointerOffset);
        if (offset == 0) return;
        builder.AddInstruction(OpCode.IncrementRefResult);
        builder.AddVUInt64(offset);
    }

    void EmitPredicate(InterpreterBuilder builder, QueryNode node)
    {
        switch (node)
        {
            case ConstantQueryNode { Value: bool value }:
                builder.AddInstruction(value ? OpCode.SetBoolResultTrue : OpCode.SetBoolResultFalse);
                return;
            case BoolAndQueryNode and:
            {
                var falseLabel = builder.DeclareLabel("andFalse");
                var endLabel = builder.DeclareLabel("andEnd");
                EmitPredicate(builder, and.Left);
                builder.AddInstruction(OpCode.JmpIfFalse);
                builder.AddLabelParameter(falseLabel);
                EmitPredicate(builder, and.Right);
                builder.AddInstruction(OpCode.Jmp);
                builder.AddLabelParameter(endLabel);
                builder.MarkLabel(falseLabel);
                builder.AddInstruction(OpCode.SetBoolResultFalse);
                builder.MarkLabel(endLabel);
                return;
            }
            case BoolOrQueryNode or:
            {
                var trueLabel = builder.DeclareLabel("orTrue");
                var endLabel = builder.DeclareLabel("orEnd");
                EmitPredicate(builder, or.Left);
                builder.AddInstruction(OpCode.JmpIfTrue);
                builder.AddLabelParameter(trueLabel);
                EmitPredicate(builder, or.Right);
                builder.AddInstruction(OpCode.Jmp);
                builder.AddLabelParameter(endLabel);
                builder.MarkLabel(trueLabel);
                builder.AddInstruction(OpCode.SetBoolResultTrue);
                builder.MarkLabel(endLabel);
                return;
            }
            case EqualsQueryNode equals:
                EmitEquals(builder, equals);
                return;
            default:
                throw new NotSupportedException("Relation query predicate is not supported.");
        }
    }

    void EmitEquals(InterpreterBuilder builder, EqualsQueryNode equals)
    {
        var prop = equals.Left as PropQueryNode;
        var constant = equals.Right as ConstantQueryNode;
        if (prop == null || constant == null)
        {
            prop = equals.Right as PropQueryNode;
            constant = equals.Left as ConstantQueryNode;
        }

        if (prop == null || constant == null)
            throw new NotSupportedException("Relation query equality supports a property and a constant only.");
        var value = _fields[prop.Name];
        value.EmitSetParam1(builder, BytesPointerOffset, ObjectsPointerOffset);
        EmitConstantToParam2(builder, value.Type, constant.Value);
        builder.AddInstruction(GetEqualOpCode(value.Type));
    }

    void EmitConstantToParam2(InterpreterBuilder builder, Type type, object? value)
    {
        if (type == typeof(string))
        {
            if (value != null && value is not string)
                throw new NotSupportedException("Relation query constant type does not match property type.");
            builder.AddInstruction(OpCode.LoadParam2BySP);
            builder.AddVUInt64(ObjectsPointerOffset);
            if (_stringScratch2 != 0)
            {
                builder.AddInstruction(OpCode.IncrementRefParam2);
                builder.AddVUInt64(_stringScratch2 * (uint)Unsafe.SizeOf<nint>());
            }
            builder.AddInstruction(OpCode.SetParam2ObjectConst);
            builder.AddVUInt64((ulong)builder.AddObjectConstant(value));
            return;
        }

        if (value == null || value.GetType() != type)
            throw new NotSupportedException("Relation query constant type does not match property type.");
        if (type == typeof(bool)) AddConstant(builder, (bool)value);
        else if (type == typeof(byte)) AddConstant(builder, (byte)value);
        else if (type == typeof(sbyte)) AddConstant(builder, (sbyte)value);
        else if (type == typeof(ushort)) AddConstant(builder, (ushort)value);
        else if (type == typeof(short)) AddConstant(builder, (short)value);
        else if (type == typeof(uint)) AddConstant(builder, (uint)value);
        else if (type == typeof(int)) AddConstant(builder, (int)value);
        else if (type == typeof(ulong)) AddConstant(builder, (ulong)value);
        else if (type == typeof(long)) AddConstant(builder, (long)value);
        else if (type == typeof(Half)) AddConstant(builder, (Half)value);
        else if (type == typeof(float)) AddConstant(builder, (float)value);
        else if (type == typeof(double)) AddConstant(builder, (double)value);
        else if (type == typeof(DateTime)) AddConstant(builder, (DateTime)value);
        else throw new NotSupportedException($"Relation query constants of type {type.ToSimpleName()} are not supported yet.");
    }

    static void AddConstant<TValue>(InterpreterBuilder builder, TValue value) where TValue : unmanaged
    {
        var offset = builder.AllocAlignedConst((uint)Unsafe.SizeOf<TValue>());
        builder.ConstSpan<TValue>(offset) = value;
        builder.AddInstruction(OpCode.SetParam2Const);
        builder.AddVUInt64(offset);
    }

    static OpCode GetEqualOpCode(Type type)
    {
        if (type == typeof(string)) return OpCode.EqualString;
        if (type == typeof(bool) || type == typeof(byte)) return OpCode.EqualByte;
        if (type == typeof(sbyte)) return OpCode.EqualSByte;
        if (type == typeof(ushort)) return OpCode.EqualUInt16;
        if (type == typeof(short)) return OpCode.EqualInt16;
        if (type == typeof(uint)) return OpCode.EqualUInt32;
        if (type == typeof(int)) return OpCode.EqualInt32;
        if (type == typeof(ulong)) return OpCode.EqualUInt64;
        if (type == typeof(long)) return OpCode.EqualInt64;
        if (type == typeof(Half)) return OpCode.EqualHalf;
        if (type == typeof(float)) return OpCode.EqualFloat;
        if (type == typeof(double)) return OpCode.EqualDouble;
        if (type == typeof(DateTime)) return OpCode.EqualDateTime;
        throw new NotSupportedException($"Relation query equality for {type.ToSimpleName()} is not supported yet.");
    }

    [SkipLocalsInit]
    static byte[] BuildCursorPrefix(IInternalObjectDBTransaction transaction, RelationInfo relationInfo,
        QueryNode? predicate)
    {
        var primaryKeyFields = relationInfo.ClientRelationVersionInfo.PrimaryKeyFields.Span;
        if (primaryKeyFields.Length == 0) return relationInfo.Prefix;
        var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        writer.WriteBlock(relationInfo.Prefix);
        IWriterCtx? writerCtx = null;
        var constrainedFields = 0;
        foreach (var field in primaryKeyFields)
        {
            if (!TryFindExactProperty(predicate, field.Name, out var property, out var constant)) break;
            if (constant.Value == null && property.Type != typeof(string) ||
                constant.Value != null && constant.Value.GetType() != property.Type)
                break;

            if (field.Handler!.NeedsCtx()) writerCtx ??= new DBWriterCtx(transaction);
            var saver = field.Handler.Save(property.Type, relationInfo.TypeConverterFactory);
            SaveConstant(saver, ref writer, writerCtx, property.Type, constant.Value);
            constrainedFields++;
        }

        return constrainedFields == 0 ? relationInfo.Prefix : writer.GetSpan().ToArray();
    }

    static bool TryFindExactProperty(QueryNode? node, string propertyName, out PropQueryNode property,
        out ConstantQueryNode constant)
    {
        if (node is BoolAndQueryNode and)
        {
            if (TryFindExactProperty(and.Left, propertyName, out property, out constant)) return true;
            return TryFindExactProperty(and.Right, propertyName, out property, out constant);
        }

        if (node is EqualsQueryNode equals)
        {
            property = equals.Left as PropQueryNode;
            constant = equals.Right as ConstantQueryNode;
            if (property == null || constant == null)
            {
                property = equals.Right as PropQueryNode;
                constant = equals.Left as ConstantQueryNode;
            }

            if (property != null && constant != null && property.Name == propertyName) return true;
        }

        property = null!;
        constant = null!;
        return false;
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
        else throw new NotSupportedException(
            $"Relation query cursor prefix does not support {type.ToSimpleName()} constants yet.");
        saver(ref writer, writerCtx, ref Unsafe.As<Int128, byte>(ref storage));
    }

    static QueryNode? ExtractPredicate(QueryNode node)
    {
        return node switch
        {
            QueryByIdQueryNode => null,
            WhereQueryNode where => Combine(ExtractPredicate(where.Source), where.Predicate),
            _ => throw new NotSupportedException("Relation query source is not supported.")
        };
    }

    static void SplitKeyPredicate(QueryNode? node, ReadOnlySpan<TableFieldInfo> primaryKeyFields,
        out QueryNode? keyPredicate, out QueryNode? remainingPredicate)
    {
        if (node == null)
        {
            keyPredicate = null;
            remainingPredicate = null;
            return;
        }

        if (node is BoolAndQueryNode and)
        {
            SplitKeyPredicate(and.Left, primaryKeyFields, out var leftKey, out var leftRemaining);
            SplitKeyPredicate(and.Right, primaryKeyFields, out var rightKey, out var rightRemaining);
            keyPredicate = CombineNullable(leftKey, rightKey);
            remainingPredicate = CombineNullable(leftRemaining, rightRemaining);
            return;
        }

        if (UsesOnlyPrimaryKeyFields(node, primaryKeyFields) &&
            ContainsPrimaryKeyField(node, primaryKeyFields))
        {
            keyPredicate = node;
            remainingPredicate = null;
        }
        else
        {
            keyPredicate = null;
            remainingPredicate = node;
        }
    }

    static bool UsesOnlyPrimaryKeyFields(QueryNode node, ReadOnlySpan<TableFieldInfo> primaryKeyFields)
    {
        switch (node)
        {
            case PropQueryNode prop:
                foreach (var field in primaryKeyFields)
                    if (field.Name == prop.Name) return true;
                return false;
            case BoolAndQueryNode and:
                return UsesOnlyPrimaryKeyFields(and.Left, primaryKeyFields) &&
                       UsesOnlyPrimaryKeyFields(and.Right, primaryKeyFields);
            case BoolOrQueryNode or:
                return UsesOnlyPrimaryKeyFields(or.Left, primaryKeyFields) &&
                       UsesOnlyPrimaryKeyFields(or.Right, primaryKeyFields);
            case EqualsQueryNode equals:
                return UsesOnlyPrimaryKeyFields(equals.Left, primaryKeyFields) &&
                       UsesOnlyPrimaryKeyFields(equals.Right, primaryKeyFields);
            case ConstantQueryNode:
                return true;
            default:
                return false;
        }
    }

    static bool ContainsPrimaryKeyField(QueryNode node, ReadOnlySpan<TableFieldInfo> primaryKeyFields)
    {
        switch (node)
        {
            case PropQueryNode prop:
                foreach (var field in primaryKeyFields)
                    if (field.Name == prop.Name) return true;
                return false;
            case BoolAndQueryNode and:
                return ContainsPrimaryKeyField(and.Left, primaryKeyFields) ||
                       ContainsPrimaryKeyField(and.Right, primaryKeyFields);
            case BoolOrQueryNode or:
                return ContainsPrimaryKeyField(or.Left, primaryKeyFields) ||
                       ContainsPrimaryKeyField(or.Right, primaryKeyFields);
            case EqualsQueryNode equals:
                return ContainsPrimaryKeyField(equals.Left, primaryKeyFields) ||
                       ContainsPrimaryKeyField(equals.Right, primaryKeyFields);
            default:
                return false;
        }
    }

    static QueryNode? CombineNullable(QueryNode? left, QueryNode? right)
    {
        if (left == null) return right;
        return right == null ? left : new BoolAndQueryNode(left, right);
    }

    static QueryNode Combine(QueryNode? left, QueryNode right)
    {
        return left == null ? right : new BoolAndQueryNode(left, right);
    }
}

sealed class RelationQueryEnumerator<T> : IEnumerator<T>, IEnumerable<T> where T : class
{
    readonly IInternalObjectDBTransaction _transaction;
    readonly RelationInfo.ItemLoaderInfo _loaderInfo;
    readonly RelationQueryPredicateCompiler<T> _compiler;
    IKeyValueDBCursor? _cursor;
    bool _first = true;
    bool _hasCurrent;
    T? _current;
    Memory<byte> _keyBuffer;
    Memory<byte> _valueBuffer;

    public RelationQueryEnumerator(IInternalObjectDBTransaction transaction, RelationInfo.ItemLoaderInfo loaderInfo,
        RelationQueryPredicateCompiler<T> compiler)
    {
        _transaction = transaction;
        _loaderInfo = loaderInfo;
        _compiler = compiler;
        _cursor = transaction.KeyValueDBTransaction.CreateCursor();
    }

    public bool MoveNext()
    {
        ObjectDisposedException.ThrowIf(_cursor == null, this);
        _transaction.ThrowIfDisposed();
        _hasCurrent = false;
        _current = null;
        while (MoveCursor())
        {
            var needsKey = _compiler.KeyCompiler != null ||
                           _compiler.HasPredicate && _compiler.HasKeyFields;
            var key = needsKey ? _cursor!.GetKeyMemory(ref _keyBuffer) : default;
            if (_compiler.KeyCompiler != null)
            {
                var keyReader = MemReader.CreateFromReadOnlyMemory(key);
                try
                {
                    var unusedValueReader = default(MemReader);
                    if (!EvaluatePredicate(_compiler.KeyCompiler, ref keyReader, ref unusedValueReader)) continue;
                }
                finally
                {
                    keyReader.Dispose();
                }
            }

            if (_compiler.HasPredicate)
            {
                var keyReader = _compiler.HasKeyFields ? MemReader.CreateFromReadOnlyMemory(key) : default;
                var valueReader = _compiler.HasValueFields
                    ? MemReader.CreateFromReadOnlyMemory(_cursor.GetValueMemory(ref _valueBuffer))
                    : default;
                try
                {
                    if (!EvaluatePredicate(_compiler, ref keyReader, ref valueReader)) continue;
                }
                finally
                {
                    valueReader.Dispose();
                    keyReader.Dispose();
                }
            }

            _hasCurrent = true;
            return true;
        }

        return false;
    }

    public T Current
    {
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

    static bool EvaluatePredicate(RelationQueryPredicateCompiler<T> compiler, ref MemReader keyReader,
        ref MemReader valueReader)
    {
        Span<byte> stack = stackalloc byte[4 * Unsafe.SizeOf<nint>()];
        var ctx = new InterpreterCtx(ref Unsafe.NullRef<byte>(), ref Unsafe.As<MemReader, byte>(ref keyReader),
            ref Unsafe.As<MemReader, byte>(ref valueReader), stack, compiler.Program);
        BTDB.Interpreter.Interpreter.Run(ref ctx);
        return ctx.BoolResult;
    }

    bool MoveCursor()
    {
        if (_first)
        {
            _first = false;
            return _cursor!.FindFirstKey(_compiler.CursorPrefix);
        }

        return _cursor!.FindNextKey(_compiler.CursorPrefix);
    }

    public void Reset()
    {
        ObjectDisposedException.ThrowIf(_cursor == null, this);
        _cursor.Invalidate();
        _first = true;
        _hasCurrent = false;
        _current = null;
    }

    public void Dispose()
    {
        _cursor?.Dispose();
        _cursor = null;
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
