using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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

sealed class RelationQuerySlot
{
    public RelationQuerySlot(Type type, bool isObject, ushort index)
    {
        Type = type;
        IsObject = isObject;
        Index = index;
    }

    public Type Type { get; }
    public bool IsObject { get; }

    /// Frame byte offset, or object slot index when IsObject.
    public ushort Index { get; }
}

sealed class RelationQueryCompiler
{
    readonly RelationInfo _relationInfo;
    readonly Type _itemType;
    readonly ReadOnlyMemory<TableFieldInfo> _primaryKeyFields;
    readonly Dictionary<string, RelationQuerySlot> _slots = new();
    int _frameSize;
    int _objectCount;
    ushort _frameTemporary;
    ushort _objectTemporary;

    public RelationQueryCompiler(RelationInfo relationInfo, Type itemType)
    {
        _relationInfo = relationInfo;
        _itemType = itemType;
        _primaryKeyFields = relationInfo.ClientRelationVersionInfo.PrimaryKeyFields;
    }

    public RelationQueryPlan Compile(IInternalObjectDBTransaction transaction, QueryNode ast)
    {
        var predicate = ExtractPredicate(ast);
        CollectFields(predicate);
        _frameTemporary = AllocFrameSlot();
        _objectTemporary = AllocObjectSlot();

        var conjuncts = new List<QueryNode>();
        if (predicate != null) FlattenConjuncts(Simplify(predicate), conjuncts);
        if (conjuncts.Exists(IsFalse)) return new(_relationInfo.Prefix, true, null, null, 0, 0);
        conjuncts.RemoveAll(IsTrue);

        // Equalities encoded in the cursor prefix are removed; every row under the prefix satisfies them.
        var cursorPrefix = BuildCursorPrefix(transaction, conjuncts, out var prefixFieldCount);
        var keyConjuncts = new List<QueryNode>();
        var valueConjuncts = new List<QueryNode>();
        foreach (var conjunct in conjuncts)
            (UsesOnlyPrimaryKeyFields(conjunct) ? keyConjuncts : valueConjuncts).Add(conjunct);

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

    void CollectFields(QueryNode? node)
    {
        switch (node)
        {
            case PropQueryNode prop:
                if (_slots.ContainsKey(prop.Name)) return;
                var tableField = _relationInfo.ClientRelationVersionInfo[prop.Name] ??
                                 throw new BTDBException($"Property {_itemType.ToSimpleName()}.{prop.Name} was not found.");
                if (tableField.Computed)
                    throw new NotSupportedException(
                        $"Computed property {_itemType.ToSimpleName()}.{prop.Name} is not supported in relation query.");
                EnsureSupportedType(prop.Type);
                var isObject = prop.Type == typeof(string);
                _slots.Add(prop.Name, new(prop.Type, isObject, isObject ? AllocObjectSlot() : AllocFrameSlot()));
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

    ushort AllocFrameSlot()
    {
        // Every frame slot has 8 bytes, so a narrower property can be read by truncating a wider stored integer.
        if (_frameSize + 8 > ushort.MaxValue)
            throw new NotSupportedException("Relation query references too many fields.");
        var offset = (ushort)_frameSize;
        _frameSize += 8;
        return offset;
    }

    ushort AllocObjectSlot()
    {
        if (_objectCount >= ushort.MaxValue)
            throw new NotSupportedException("Relation query references too many fields.");
        return (ushort)_objectCount++;
    }

    static void EnsureSupportedType(Type type)
    {
        if (type == typeof(string) || type == typeof(bool) || type == typeof(byte) || type == typeof(sbyte) ||
            type == typeof(ushort) || type == typeof(short) || type == typeof(uint) || type == typeof(int) ||
            type == typeof(ulong) || type == typeof(long) || type == typeof(Half) || type == typeof(float) ||
            type == typeof(double) || type == typeof(DateTime)) return;
        throw new NotSupportedException($"Relation query does not support fields of type {type.ToSimpleName()} yet.");
    }

    RelationQueryProgram BuildProgram(List<QueryNode> conjuncts, ReadOnlyMemory<TableFieldInfo>? valueFields,
        int cursorPrefixLength, int prefixFieldCount)
    {
        var keyFields = _primaryKeyFields.Span;
        var specialized = new List<QueryNode>(conjuncts.Count);
        foreach (var conjunct in conjuncts)
        {
            // Properties missing in this value version read as their default value.
            var node = valueFields == null ? conjunct : Simplify(SubstituteMissing(conjunct, valueFields.Value.Span));
            if (IsFalse(node)) return new(null, 0, false);
            if (IsTrue(node)) continue;
            specialized.Add(node);
        }

        if (specialized.Count == 0) return new(null, 0, true);

        var names = new HashSet<string>();
        foreach (var conjunct in specialized) CollectPropertyNames(conjunct, names);
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

        var builder = new InterpreterBuilder();
        var falseLabel = builder.DeclareLabel("false");
        var emitted = 0;
        var skip = new SkipRun(OpCode.SkipKey);
        for (var i = startField; i <= lastKeyField; i++)
        {
            var field = keyFields[i];
            if (field.Computed) continue;
            if (!names.Contains(field.Name))
            {
                skip.Add(builder, GetSkipCodeOp(field.Handler!));
                continue;
            }

            skip.Flush(builder);
            EmitRead(builder, true, field, _slots[field.Name]);
            // The key program tests each conjunct as soon as the last key field it needs was read.
            if (valueFields == null) emitted += EmitReadyConjuncts(builder, specialized, keyFields, i, falseLabel);
        }

        if (valueFields != null)
        {
            var fields = valueFields.Value.Span;
            emitted += EmitReadyConjuncts(builder, specialized, fields, -1, falseLabel);
            var lastValueField = -1;
            for (var i = 0; i < fields.Length; i++)
                if (names.Contains(fields[i].Name) && !fields[i].Computed) lastValueField = i;
            skip = new(OpCode.SkipValue);
            for (var i = 0; i <= lastValueField; i++)
            {
                var field = fields[i];
                if (field.Computed) continue;
                if (!names.Contains(field.Name))
                {
                    skip.Add(builder, GetSkipCodeOp(field.Handler!));
                    continue;
                }

                skip.Flush(builder);
                EmitRead(builder, false, field, _slots[field.Name]);
                emitted += EmitReadyConjuncts(builder, specialized, fields, i, falseLabel);
            }
        }

        if (emitted != specialized.Count)
            throw new InvalidOperationException("Relation query compiler did not emit every conjunct.");

        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.Stop);
        builder.MarkLabel(falseLabel);
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.AddInstruction(OpCode.Stop);
        return new(builder.Materialize(), keyStart, false);
    }

    // Returns the number of emitted conjuncts.
    int EmitReadyConjuncts(InterpreterBuilder builder, List<QueryNode> conjuncts, ReadOnlySpan<TableFieldInfo> fields,
        int fieldIndex, uint falseLabel)
    {
        var emitted = 0;
        var names = new HashSet<string>();
        foreach (var conjunct in conjuncts)
        {
            names.Clear();
            CollectPropertyNames(conjunct, names);
            var lastField = -1;
            for (var i = 0; i < fields.Length; i++)
                if (names.Contains(fields[i].Name)) lastField = i;
            if (lastField != fieldIndex) continue;
            EmitBranch(builder, conjunct, falseLabel, false);
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

    void EmitRead(InterpreterBuilder builder, bool fromKey, TableFieldInfo field, RelationQuerySlot slot)
    {
        var handler = field.Handler!;
        var codeOp = GetReadCodeOp(handler);
        var sourceType = handler.HandledType()!;
        if (sourceType == slot.Type || CanTruncate(sourceType, slot.Type))
        {
            EmitReadInto(builder, fromKey, codeOp, slot.IsObject, slot.Index);
            return;
        }

        var sourceIsObject = !sourceType.IsValueType;
        var temporary = sourceIsObject ? _objectTemporary : _frameTemporary;
        EmitReadInto(builder, fromKey, codeOp, sourceIsObject, temporary);
        builder.AddInstruction(OpCode.ConvertSlot);
        builder.AddUInt8((byte)(sourceIsObject ? QuerySlotKind.Object : QuerySlotKind.Frame));
        builder.AddUInt16(temporary);
        builder.AddUInt8((byte)(slot.IsObject ? QuerySlotKind.Object : QuerySlotKind.Frame));
        builder.AddUInt16(slot.Index);
        builder.AddConverterIndex(_relationInfo.TypeConverterFactory, sourceType, slot.Type);
    }

    // The default converter narrows integers by unchecked casts, which on little endian is the same as reading the low
    // bytes of the wider value from the 8 byte frame slot.
    bool CanTruncate(Type sourceType, Type targetType)
    {
        return BitConverter.IsLittleEndian &&
               _relationInfo.TypeConverterFactory.GetType() == typeof(DefaultTypeConverterFactory) &&
               IsInteger(sourceType) && IsInteger(targetType) && SizeOf(sourceType) >= SizeOf(targetType);
    }

    static bool IsInteger(Type type)
    {
        return type == typeof(byte) || type == typeof(sbyte) || type == typeof(ushort) || type == typeof(short) ||
               type == typeof(uint) || type == typeof(int) || type == typeof(ulong) || type == typeof(long);
    }

    static int SizeOf(Type type)
    {
        if (type == typeof(byte) || type == typeof(sbyte)) return 1;
        if (type == typeof(ushort) || type == typeof(short)) return 2;
        if (type == typeof(uint) || type == typeof(int)) return 4;
        return 8;
    }

    static void EmitReadInto(InterpreterBuilder builder, bool fromKey, CodeOp codeOp, bool isObject, ushort index)
    {
        builder.AddInstruction(fromKey
            ? isObject ? OpCode.ReadKeyToObject : OpCode.ReadKeyToFrame
            : isObject ? OpCode.ReadValueToObject : OpCode.ReadValueToFrame);
        builder.AddUInt8((byte)codeOp);
        builder.AddUInt16(index);
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

    // Emits code jumping to label when node evaluates to jumpIfTrue and falling through otherwise.
    void EmitBranch(InterpreterBuilder builder, QueryNode node, uint label, bool jumpIfTrue)
    {
        switch (node)
        {
            case ConstantQueryNode { Value: bool value }:
                if (value != jumpIfTrue) return;
                builder.AddInstruction(OpCode.Jmp);
                builder.AddLabelParameter(label);
                return;
            case BoolAndQueryNode and:
                if (jumpIfTrue)
                {
                    var skipLabel = builder.DeclareLabel("andSkip");
                    EmitBranch(builder, and.Left, skipLabel, false);
                    EmitBranch(builder, and.Right, label, true);
                    builder.MarkLabel(skipLabel);
                }
                else
                {
                    EmitBranch(builder, and.Left, label, false);
                    EmitBranch(builder, and.Right, label, false);
                }

                return;
            case BoolOrQueryNode or:
                if (jumpIfTrue)
                {
                    EmitBranch(builder, or.Left, label, true);
                    EmitBranch(builder, or.Right, label, true);
                }
                else
                {
                    var skipLabel = builder.DeclareLabel("orSkip");
                    EmitBranch(builder, or.Left, skipLabel, true);
                    EmitBranch(builder, or.Right, label, false);
                    builder.MarkLabel(skipLabel);
                }

                return;
            case EqualsQueryNode equals:
                EmitEquals(builder, equals, label, jumpIfTrue);
                return;
            default:
                throw new NotSupportedException("Relation query predicate is not supported.");
        }
    }

    void EmitEquals(InterpreterBuilder builder, EqualsQueryNode equals, uint label, bool jumpIfTrue)
    {
        if (!TrySplitPropertyConstant(equals, out var prop, out var constant))
            throw new NotSupportedException("Relation query equality supports a property and a constant only.");
        var slot = _slots[prop.Name];
        if (slot.IsObject)
        {
            if (constant.Value != null && constant.Value is not string)
                throw new NotSupportedException("Relation query constant type does not match property type.");
            builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfStringConst : OpCode.JmpIfNotStringConst);
            builder.AddUInt8((byte)QueryCompare.Equal);
            builder.AddUInt16(slot.Index);
            builder.AddUInt32((uint)builder.AddObjectConstant(constant.Value));
            builder.AddLabelTarget(label);
            return;
        }

        if (constant.Value == null || constant.Value.GetType() != slot.Type)
            throw new NotSupportedException("Relation query constant type does not match property type.");
        var valueType = AddFrameConstant(builder, slot.Type, constant.Value, out var constantOffset);
        builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfFrameConst : OpCode.JmpIfNotFrameConst);
        builder.AddUInt8((byte)valueType);
        builder.AddUInt8((byte)QueryCompare.Equal);
        builder.AddUInt16(slot.Index);
        builder.AddUInt32(constantOffset);
        builder.AddLabelTarget(label);
    }

    static QueryValueType AddFrameConstant(InterpreterBuilder builder, Type type, object value, out uint offset)
    {
        if (type == typeof(bool)) return Add(builder, (bool)value, QueryValueType.UInt8, out offset);
        if (type == typeof(byte)) return Add(builder, (byte)value, QueryValueType.UInt8, out offset);
        if (type == typeof(sbyte)) return Add(builder, (sbyte)value, QueryValueType.Int8, out offset);
        if (type == typeof(ushort)) return Add(builder, (ushort)value, QueryValueType.UInt16, out offset);
        if (type == typeof(short)) return Add(builder, (short)value, QueryValueType.Int16, out offset);
        if (type == typeof(uint)) return Add(builder, (uint)value, QueryValueType.UInt32, out offset);
        if (type == typeof(int)) return Add(builder, (int)value, QueryValueType.Int32, out offset);
        if (type == typeof(ulong)) return Add(builder, (ulong)value, QueryValueType.UInt64, out offset);
        if (type == typeof(long)) return Add(builder, (long)value, QueryValueType.Int64, out offset);
        if (type == typeof(Half)) return Add(builder, (Half)value, QueryValueType.Half, out offset);
        if (type == typeof(float)) return Add(builder, (float)value, QueryValueType.Single, out offset);
        if (type == typeof(double)) return Add(builder, (double)value, QueryValueType.Double, out offset);
        if (type == typeof(DateTime)) return Add(builder, (DateTime)value, QueryValueType.DateTime, out offset);
        throw new NotSupportedException($"Relation query constants of type {type.ToSimpleName()} are not supported yet.");

        static QueryValueType Add<TValue>(InterpreterBuilder builder, TValue value, QueryValueType valueType,
            out uint offset) where TValue : unmanaged
        {
            offset = builder.AllocAlignedConst((uint)Unsafe.SizeOf<TValue>());
            builder.ConstSpan<TValue>(offset) = value;
            return valueType;
        }
    }

    static bool TrySplitPropertyConstant(EqualsQueryNode equals, out PropQueryNode prop,
        out ConstantQueryNode constant)
    {
        if (equals.Left is PropQueryNode leftProp && equals.Right is ConstantQueryNode rightConstant)
        {
            prop = leftProp;
            constant = rightConstant;
            return true;
        }

        if (equals.Right is PropQueryNode rightProp && equals.Left is ConstantQueryNode leftConstant)
        {
            prop = rightProp;
            constant = leftConstant;
            return true;
        }

        prop = null!;
        constant = null!;
        return false;
    }

    [SkipLocalsInit]
    byte[] BuildCursorPrefix(IInternalObjectDBTransaction transaction, List<QueryNode> conjuncts,
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
                constant.Value != null && constant.Value.GetType() != property.Type)
                break;

            if (field.Handler!.NeedsCtx()) writerCtx ??= new DBWriterCtx(transaction);
            var saver = field.Handler.Save(property.Type, _relationInfo.TypeConverterFactory);
            SaveConstant(saver, ref writer, writerCtx, property.Type, constant.Value);
            conjuncts.RemoveAt(conjunctIndex);
            prefixFieldCount++;
        }

        return prefixFieldCount == 0 ? _relationInfo.Prefix : writer.GetSpan().ToArray();
    }

    static int FindExactProperty(List<QueryNode> conjuncts, string propertyName, out PropQueryNode property,
        out ConstantQueryNode constant)
    {
        for (var i = 0; i < conjuncts.Count; i++)
        {
            if (conjuncts[i] is EqualsQueryNode equals && TrySplitPropertyConstant(equals, out property, out constant) &&
                property.Name == propertyName)
                return i;
        }

        property = null!;
        constant = null!;
        return -1;
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
            WhereQueryNode where => ExtractPredicate(where.Source) is { } source
                ? new BoolAndQueryNode(source, where.Predicate)
                : where.Predicate,
            _ => throw new NotSupportedException("Relation query source is not supported.")
        };
    }

    static void FlattenConjuncts(QueryNode node, List<QueryNode> conjuncts)
    {
        if (node is BoolAndQueryNode and)
        {
            FlattenConjuncts(and.Left, conjuncts);
            FlattenConjuncts(and.Right, conjuncts);
            return;
        }

        conjuncts.Add(node);
    }

    static bool IsTrue(QueryNode node) => node is ConstantQueryNode { Value: true };
    static bool IsFalse(QueryNode node) => node is ConstantQueryNode { Value: false };

    static QueryNode Simplify(QueryNode node)
    {
        switch (node)
        {
            case BoolAndQueryNode and:
            {
                var left = Simplify(and.Left);
                var right = Simplify(and.Right);
                if (IsFalse(left) || IsFalse(right)) return new ConstantQueryNode(false, typeof(bool));
                if (IsTrue(left)) return right;
                if (IsTrue(right)) return left;
                return ReferenceEquals(left, and.Left) && ReferenceEquals(right, and.Right)
                    ? and
                    : new BoolAndQueryNode(left, right);
            }
            case BoolOrQueryNode or:
            {
                var left = Simplify(or.Left);
                var right = Simplify(or.Right);
                if (IsTrue(left) || IsTrue(right)) return new ConstantQueryNode(true, typeof(bool));
                if (IsFalse(left)) return right;
                if (IsFalse(right)) return left;
                return ReferenceEquals(left, or.Left) && ReferenceEquals(right, or.Right)
                    ? or
                    : new BoolOrQueryNode(left, right);
            }
            case EqualsQueryNode { Left: ConstantQueryNode left, Right: ConstantQueryNode right }:
                return new ConstantQueryNode(ConstantsEqual(left.Value, right.Value), typeof(bool));
            default:
                return node;
        }
    }

    // C# == semantics for the supported constant types; object.Equals differs only for NaN.
    static bool ConstantsEqual(object? left, object? right)
    {
        if (left is double leftDouble && right is double rightDouble) return leftDouble == rightDouble;
        if (left is float leftFloat && right is float rightFloat) return leftFloat == rightFloat;
        if (left is Half leftHalf && right is Half rightHalf) return leftHalf == rightHalf;
        return Equals(left, right);
    }

    QueryNode SubstituteMissing(QueryNode node, ReadOnlySpan<TableFieldInfo> valueFields)
    {
        switch (node)
        {
            case PropQueryNode prop:
                if (IsPrimaryKeyField(prop.Name)) return prop;
                foreach (var field in valueFields)
                    if (field.Name == prop.Name && !field.Computed) return prop;
                return new ConstantQueryNode(DefaultValue(prop.Type), prop.Type);
            case BoolAndQueryNode and:
                return new BoolAndQueryNode(SubstituteMissing(and.Left, valueFields),
                    SubstituteMissing(and.Right, valueFields));
            case BoolOrQueryNode or:
                return new BoolOrQueryNode(SubstituteMissing(or.Left, valueFields),
                    SubstituteMissing(or.Right, valueFields));
            case EqualsQueryNode equals:
                return new EqualsQueryNode(SubstituteMissing(equals.Left, valueFields),
                    SubstituteMissing(equals.Right, valueFields));
            default:
                return node;
        }
    }

    static object? DefaultValue(Type type)
    {
        if (type == typeof(string)) return null;
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
        throw new NotSupportedException($"Relation query does not support fields of type {type.ToSimpleName()} yet.");
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
        switch (node)
        {
            case PropQueryNode prop:
                names.Add(prop.Name);
                break;
            case BoolAndQueryNode and:
                CollectPropertyNames(and.Left, names);
                CollectPropertyNames(and.Right, names);
                break;
            case BoolOrQueryNode or:
                CollectPropertyNames(or.Left, names);
                CollectPropertyNames(or.Right, names);
                break;
            case EqualsQueryNode equals:
                CollectPropertyNames(equals.Left, names);
                CollectPropertyNames(equals.Right, names);
                break;
        }
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
