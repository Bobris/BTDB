using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;

namespace BTDB.ODBLayer;

/// Entry points of interceptors that BTDB.SourceGenerator emits for Queryable calls on IRelation.Query&lt;T&gt;()
/// chains. Each returns false for sources which are not relation queries, so the interceptor calls the original
/// Queryable method.
[EditorBrowsable(EditorBrowsableState.Never)]
public static class RelationQueryInterception
{
    /// A Where, Select, ordering, Skip or Take call: remembers its argument instead of building an expression, which
    /// materialize builds with the original Queryable method when asked for.
    public static bool TryLink<TSource, TElement, TQueryable>(IQueryable<TSource> source, RelationQueryCallSite site,
        object? argument, Func<IQueryable<TSource>, object?, TQueryable> materialize,
        [NotNullWhen(true)] out TQueryable? result) where TQueryable : class, IQueryable<TElement>
    {
        result = (source as IRelationQueryChain)?.Append<TElement, TQueryable>(
            new RelationQueryLink<TSource, TQueryable>(source, site, argument, materialize));
        return result != null;
    }

    /// A terminal operator with up to two arguments besides the source (a lambda, a default value or both); execute
    /// calls the original Queryable method, and is used only to capture its expression.
    public static bool TryExecute<TSource, TResult>(IQueryable<TSource> source, RelationQueryCallSite site,
        object? argument1, object? argument2, Func<IQueryable<TSource>, object?, object?, TResult> execute,
        out TResult result)
    {
        if (source is not IRelationQueryChain chain)
        {
            result = default!;
            return false;
        }

        var value = chain.ExecuteIntercepted(site, argument1, argument2,
            () => RelationQueryCapture<TSource>.Capture(source, argument1, argument2, execute));
        result = value is null ? default! : (TResult)value;
        return true;
    }

    internal static IEnumerator<T> Enumerate<T>(RelationQuery<T> root, RelationQueryLink link, IQueryable<T> query)
        where T : class
    {
        var execution = Prepare(root.RelationInfo, link.Site, link, null, null, () => query.Expression,
            out var fromLinks);
        return root.Enumerate(execution, fromLinks
            ? RelationQueryRequest.ForLinks(() => execution.Expression, FindProjection(link))
            : RelationQueryRequest.ForSource(execution.Expression));
    }

    internal static IEnumerator<TElement> EnumerateProjected<TRow, TElement>(RelationQuery<TRow> root,
        RelationQueryLink link, IQueryable<TElement> query) where TRow : class
    {
        var execution = Prepare(root.RelationInfo, link.Site, link, null, null, () => query.Expression,
            out var fromLinks);
        return root.EnumerateProjected<TElement>(execution, fromLinks
            ? RelationQueryRequest.ForLinks(() => execution.Expression, FindProjection(link))
            : RelationQueryRequest.ForSource(execution.Expression));
    }

    /// A terminal operator; its source is replayable when it is the query of IRelation.Query&lt;T&gt;() or made of
    /// links.
    internal static object? Execute<T>(RelationQuery<T> root, RelationQueryLink? link, RelationQueryCallSite site,
        object? argument1, object? argument2, Func<Expression> terminalExpression, bool replayable) where T : class
    {
        var execution = Prepare(root.RelationInfo, site, link, argument1, argument2, terminalExpression,
            out var fromLinks, replayable);
        if (!fromLinks)
            return root.Execute(execution,
                RelationQueryRequest.ForTerminal((MethodCallExpression)execution.Expression));
        var lambda = argument1 as LambdaExpression;
        return root.Execute(execution, RelationQueryRequest.ForLinks(
            () => ((MethodCallExpression)execution.Expression).Arguments[0], FindProjection(link), site.Name, lambda,
            lambda != null ? argument2 : argument1));
    }

    // The fast path reads the values along the call site's recorded recipes; otherwise the whole expression is built,
    // runs the regular cached path and teaches the call site where its parameters are.
    static RelationQueryExecution Prepare(RelationInfo relationInfo, RelationQueryCallSite site,
        RelationQueryLink? last, object? argument1, object? argument2, Func<Expression> expression, out bool fromLinks,
        bool replayable = true)
    {
        var cache = relationInfo.QueryCache;
        var links = replayable ? RelationQueryCallSite.Collect(last) : null;
        if (links != null && site.TryPrepare(cache, links, argument1, argument2, out var template, out var values))
        {
            System.Threading.Interlocked.Increment(ref cache.Replays);
            fromLinks = true;
            return new(cache, template, values, expression);
        }

        fromLinks = false;
        var full = expression();
        var execution = cache.Start(full);
        // Enumeration is identified by the last link's call site, a terminal operator by its own.
        if (links != null && execution.Template != null)
            site.Record(cache, links, argument1, argument2, full, execution,
                last == null || !ReferenceEquals(site, last.Site));
        return execution;
    }

    static LambdaExpression? FindProjection(RelationQueryLink? link)
    {
        for (; link != null; link = link.Previous)
        {
            if (link.Site.Name != nameof(Queryable.Select)) continue;
            var lambda = (LambdaExpression)link.Argument!;
            return lambda.Body == lambda.Parameters[0] ? null : lambda;
        }

        return null;
    }
}

/// A relation query object which intercepted operators can extend with links.
interface IRelationQueryChain
{
    /// The link which made this query, or null for the query of IRelation.Query&lt;T&gt;() and for queries made by
    /// regular Queryable calls.
    RelationQueryLink? Link { get; }

    /// Whether this is the query of IRelation.Query&lt;T&gt;() itself.
    bool IsRoot { get; }

    /// The query of the link, or null when it is not supported and the original Queryable method must run.
    TQueryable? Append<TElement, TQueryable>(RelationQueryLink link) where TQueryable : class;

    object? ExecuteIntercepted(RelationQueryCallSite site, object? argument1, object? argument2,
        Func<Expression> terminalExpression);
}

/// An intercepted Queryable call: its call site and argument (a lambda or a count) after the previous query.
abstract class RelationQueryLink
{
    protected RelationQueryLink(RelationQueryCallSite site, object? argument, IRelationQueryChain? previous)
    {
        Site = site;
        Argument = argument;
        Previous = previous?.Link;
        // A chain is replayed only when it consists of links down to the query of IRelation.Query<T>().
        Replayable = previous != null && (previous.Link?.Replayable ?? previous.IsRoot);
    }

    public RelationQueryCallSite Site { get; }
    public object? Argument { get; }
    public RelationQueryLink? Previous { get; }
    public bool Replayable { get; }

    /// The expression of the query this link makes, built with the original Queryable method.
    public abstract Expression Materialize();
}

sealed class RelationQueryLink<TSource, TQueryable> : RelationQueryLink where TQueryable : IQueryable
{
    readonly IQueryable<TSource> _source;
    readonly Func<IQueryable<TSource>, object?, TQueryable> _materialize;

    public RelationQueryLink(IQueryable<TSource> source, RelationQueryCallSite site, object? argument,
        Func<IQueryable<TSource>, object?, TQueryable> materialize) : base(site, argument,
        source as IRelationQueryChain)
    {
        _source = source;
        _materialize = materialize;
    }

    public override Expression Materialize() => _materialize(_source, Argument).Expression;
}

/// Records the expression a terminal Queryable operator passes to its provider, without executing it.
sealed class RelationQueryCapture<TSource> : IQueryable<TSource>, IQueryProvider
{
    readonly IQueryable<TSource> _source;
    Expression? _captured;

    RelationQueryCapture(IQueryable<TSource> source)
    {
        _source = source;
    }

    public static Expression Capture<TResult>(IQueryable<TSource> source, object? argument1, object? argument2,
        Func<IQueryable<TSource>, object?, object?, TResult> execute)
    {
        var capture = new RelationQueryCapture<TSource>(source);
        execute(capture, argument1, argument2);
        return capture._captured ?? throw new InvalidOperationException("Queryable operator did not execute.");
    }

    public Type ElementType => typeof(TSource);
    public Expression Expression => _source.Expression;
    public IQueryProvider Provider => this;

    public IQueryable CreateQuery(Expression expression) => throw new NotSupportedException();

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression) => throw new NotSupportedException();

    public object? Execute(Expression expression)
    {
        _captured = expression;
        return null;
    }

    public TResult Execute<TResult>(Expression expression)
    {
        _captured = expression;
        return default!;
    }

    public IEnumerator<TSource> GetEnumerator() => throw new NotSupportedException();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// One intercepted Queryable call in source code. Its whole chain is written there, so every execution builds the
/// same expression shape except for what the shape builder also tells apart: which nullable or reference parameters
/// are null and which Skip counts are zero or less. Per relation and per such variant, the call site keeps the
/// template and where every parameter value is, so executions read the values without building the expression.
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class RelationQueryCallSite
{
    // Variants kept per relation.
    const int MaxVariants = 16;

    readonly object _lock = new();
    volatile Entry[] _entries = [];

    public RelationQueryCallSite(string name)
    {
        Name = name;
    }

    /// The Queryable operator called here.
    public string Name { get; }

    sealed record Entry(RelationQueryCache Cache, Variant[] Variants, bool NotReplayable);

    sealed record Variant(ulong SkipMask, ulong NullMask, ulong NullableMask, RelationQueryTemplate Template,
        Recipe[] Recipes);

    /// Where a parameter value is: inside a lambda (Path) or the argument itself; Source is a link index, or -1 and -2
    /// for the first and second argument of a terminal operator.
    sealed record Recipe(int Source, (int Index, ExpressionType NodeType)[]? Path);

    /// The links from the first after IRelation.Query&lt;T&gt;() to last, or null when the chain cannot be replayed.
    internal static RelationQueryLink[]? Collect(RelationQueryLink? last)
    {
        if (last is { Replayable: false }) return null;
        var count = 0;
        for (var link = last; link != null; link = link.Previous) count++;
        if (count > 63) return null;
        var links = new RelationQueryLink[count];
        for (var link = last; link != null; link = link.Previous) links[--count] = link;
        return links;
    }

    internal bool TryPrepare(RelationQueryCache cache, RelationQueryLink[] links, object? argument1, object? argument2,
        [NotNullWhen(true)] out RelationQueryTemplate? template, [NotNullWhen(true)] out object?[]? values)
    {
        template = null;
        values = null;
        Entry? entry = null;
        foreach (var candidate in _entries)
            if (ReferenceEquals(candidate.Cache, cache))
                entry = candidate;
        if (entry == null || entry.NotReplayable) return false;
        var skipMask = SkipMask(links);
        Recipe[]? recipes = null;
        var nullMask = 0ul;
        foreach (var variant in entry.Variants)
        {
            if (variant.SkipMask != skipMask) continue;
            if (recipes == null)
            {
                // Variants with the same Skip counts have the same parameters.
                recipes = variant.Recipes;
                values = new object?[recipes.Length];
                for (var i = 0; i < recipes.Length; i++)
                {
                    if (!TryRead(recipes[i], links, argument1, argument2, out var value)) return false;
                    values[i] = value;
                    if (value == null && (variant.NullableMask & (1ul << i)) != 0) nullMask |= 1ul << i;
                }
            }

            if (variant.NullMask != nullMask) continue;
            template = variant.Template;
            return true;
        }

        return false;
    }

    internal void Record(RelationQueryCache cache, RelationQueryLink[] links, object? argument1, object? argument2,
        Expression expression, RelationQueryExecution execution, bool terminal)
    {
        var parameters = execution.Parameters;
        var values = execution.Values!;
        Recipe[]? recipes = parameters.Length <= 63 ? new Recipe[parameters.Length] : null;
        var nullableMask = 0ul;
        var nullMask = 0ul;
        for (var i = 0; recipes != null && i < parameters.Length; i++)
        {
            var recipe = Locate(parameters[i], links, argument1, argument2, expression, terminal);
            if (recipe == null) recipes = null;
            else recipes[i] = recipe;
            var type = parameters[i].Type;
            if (type.IsValueType && Nullable.GetUnderlyingType(type) == null) continue;
            nullableMask |= 1ul << i;
            if (values[i] == null) nullMask |= 1ul << i;
        }

        lock (_lock)
        {
            var entries = _entries;
            var index = Array.FindIndex(entries, e => ReferenceEquals(e.Cache, cache));
            Entry entry;
            if (recipes == null)
            {
                entry = new(cache, [], true);
            }
            else
            {
                var variants = index < 0 ? [] : entries[index].Variants;
                if (variants.Length == MaxVariants) variants = variants[1..];
                entry = new(cache, [..variants, new(SkipMask(links), nullMask, nullableMask, execution.Template!,
                    recipes)], false);
            }

            // Entries of caches the relations no longer use are dropped.
            var kept = new List<Entry>(entries.Length + 1);
            foreach (var existing in entries)
                if (!ReferenceEquals(existing.Cache, cache) && kept.Count < 7)
                    kept.Add(existing);
            kept.Add(entry);
            _entries = kept.ToArray();
        }
    }

    static ulong SkipMask(RelationQueryLink[] links)
    {
        var mask = 0ul;
        for (var i = 0; i < links.Length; i++)
            if (links[i].Site.Name == nameof(Queryable.Skip) && links[i].Argument is int count && count <= 0)
                mask |= 1ul << i;
        return mask;
    }

    static object? SourceArgument(int source, RelationQueryLink[] links, object? argument1, object? argument2) =>
        source switch
        {
            -1 => argument1,
            -2 => argument2,
            _ => links[source].Argument
        };

    static bool TryRead(Recipe recipe, RelationQueryLink[] links, object? argument1, object? argument2,
        out object? value)
    {
        var argument = SourceArgument(recipe.Source, links, argument1, argument2);
        if (recipe.Path == null)
        {
            value = argument;
            return true;
        }

        value = null;
        var node = argument as Expression;
        foreach (var (index, nodeType) in recipe.Path)
        {
            node = node == null ? null : Child(node, index);
            // The structure is fixed by the source code; anything else makes the regular path run.
            if (node == null || node.NodeType != nodeType) return false;
        }

        value = RelationQueryAstBuilder.EvaluateConstant(node!);
        return true;
    }

    // A parameter is a whole argument of a Queryable call (a count or a default value, which Queryable wraps in a
    // constant) or a subtree of a link's or terminal's lambda. The expression is the terminal operator's call with the
    // links' calls below it, or the last link's call.
    static Recipe? Locate(Expression parameter, RelationQueryLink[] links, object? argument1, object? argument2,
        Expression expression, bool terminal)
    {
        var call = expression as MethodCallExpression;
        if (terminal && call != null)
        {
            for (var i = 1; i < call.Arguments.Count && i <= 2; i++)
                if (ReferenceEquals(call.Arguments[i], parameter))
                    return new(-i, null);
            call = call.Arguments[0] as MethodCallExpression;
        }

        for (var i = links.Length - 1; i >= 0 && call != null; i--)
        {
            if (call.Arguments.Count > 1 && ReferenceEquals(call.Arguments[1], parameter)) return new(i, null);
            call = call.Arguments[0] as MethodCallExpression;
        }

        for (var source = -2; source < links.Length; source++)
        {
            if (SourceArgument(source, links, argument1, argument2) is not LambdaExpression lambda) continue;
            var path = new List<(int, ExpressionType)>();
            if (Find(lambda, parameter, path)) return new(source, path.ToArray());
        }

        return null;
    }

    static bool Find(Expression node, Expression parameter, List<(int, ExpressionType)> path)
    {
        if (ReferenceEquals(node, parameter)) return true;
        for (var i = 0;; i++)
        {
            var child = Child(node, i);
            if (child == null) return false;
            path.Add((i, child.NodeType));
            if (Find(child, parameter, path)) return true;
            path.RemoveAt(path.Count - 1);
        }
    }

    // Children in a fixed order, the same when recording and when reading values.
    static Expression? Child(Expression node, int index)
    {
        switch (node)
        {
            case LambdaExpression lambda:
                return index == 0 ? lambda.Body : null;
            case UnaryExpression unary:
                return index == 0 ? unary.Operand : null;
            case BinaryExpression binary:
                return index switch { 0 => binary.Left, 1 => binary.Right, _ => null };
            case MemberExpression member:
                return index == 0 ? member.Expression : null;
            case MethodCallExpression call:
                if (call.Object != null)
                {
                    if (index == 0) return call.Object;
                    index--;
                }

                return index < call.Arguments.Count ? call.Arguments[index] : null;
            case ConditionalExpression conditional:
                return index switch
                {
                    0 => conditional.Test, 1 => conditional.IfTrue, 2 => conditional.IfFalse, _ => null
                };
            case NewExpression newExpression:
                return index < newExpression.Arguments.Count ? newExpression.Arguments[index] : null;
            case MemberInitExpression memberInit:
                if (index == 0) return memberInit.NewExpression;
                return index <= memberInit.Bindings.Count && memberInit.Bindings[index - 1] is MemberAssignment
                    assignment
                    ? assignment.Expression
                    : null;
            case NewArrayExpression newArray:
                return index < newArray.Expressions.Count ? newArray.Expressions[index] : null;
            case TypeBinaryExpression typeBinary:
                return index == 0 ? typeBinary.Expression : null;
            case InvocationExpression invocation:
                if (index == 0) return invocation.Expression;
                return index <= invocation.Arguments.Count ? invocation.Arguments[index - 1] : null;
            default:
                return null;
        }
    }
}
