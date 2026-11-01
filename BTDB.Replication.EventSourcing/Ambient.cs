using BTDB.ODBLayer;

namespace BTDB.Replication.EventSourcing;

/// <summary>Async-flow context; no process-global database or publisher. Query scopes own their snapshots.</summary>
public static class Ambient
{
    internal sealed class Context(EventPublisher publisher, bool query, IObjectDBTransaction? transaction, Event? ev, bool replay)
    {
        internal readonly EventPublisher Publisher = publisher;
        internal readonly bool Query = query;
        internal IObjectDBTransaction? Transaction = transaction;
        internal readonly Event? Event = ev;
        internal readonly bool Replay = replay;
        internal List<Action>? PostCommit;
        internal bool Active = true;
    }

    static readonly AsyncLocal<Context?> Current = new();

    public static bool IsQuery => GetContext().Query;
    public static bool IsReplay => GetContext().Replay;
    public static Event? CurrentEvent => GetContext().Event;
    public static ulong EventId => GetContext().Event?.EventId ?? 0;
    public static EventClusterSnapshot Cluster => GetContext().Publisher.Cluster;

    /// <summary>Run after the complete batch becomes visible, in registration order with a fresh query scope.
    /// Replay ignores registrations; rolled-back events discard them. Actions are not persisted or retried.</summary>
    public static void RegisterPostCommit(Action action)
    {
        ArgumentNullException.ThrowIfNull(action);
        var context = GetContext();
        if (context.Query) throw new InvalidOperationException("Register post-commit actions during consumption only.");
        if (!context.Replay) (context.PostCommit ??= new()).Add(action);
    }

    /// <summary>Borrowed transaction. Handlers must not commit or dispose it.</summary>
    public static IObjectDBTransaction Transaction
    {
        get
        {
            var context = GetContext();
            return context.Transaction ??= context.Publisher.Database.StartReadOnlyTransaction();
        }
    }

    public static TRelation Relation<TRelation>() where TRelation : class, IRelation =>
        Transaction.GetRelation<TRelation>();

    /// <summary>Deterministic application IDs stored in ObjectDB user Ulong slot 2.</summary>
    public static ulong AllocateId()
    {
        if (IsQuery) throw new InvalidOperationException("Cannot allocate an ID in a query.");
        var transaction = Transaction.KeyValueDBTransaction;
        var id = checked(transaction.GetUlong(2) + 1);
        transaction.SetUlong(2, id);
        return id;
    }

    /// <summary>Makes later <see cref="AllocateId"/> calls return IDs above <paramref name="id"/>, for example after
    /// importing IDs from another system; it never lowers the allocator.</summary>
    public static void ReserveIdsUpTo(ulong id)
    {
        if (IsQuery) throw new InvalidOperationException("Cannot reserve IDs in a query.");
        var transaction = Transaction.KeyValueDBTransaction;
        if (transaction.GetUlong(2) < id) transaction.SetUlong(2, id);
    }

    public static void TemporaryCloseTransaction()
    {
        var context = GetContext();
        if (!context.Query) throw new InvalidOperationException("Cannot close the consumer's transaction.");
        context.Transaction?.Dispose();
        context.Transaction = null;
    }

    public static Scope StartQuery(EventPublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        if (Current.Value is { Active: true } existing)
        {
            if (!existing.Query || !ReferenceEquals(existing.Publisher, publisher))
                throw new InvalidOperationException("Cannot mix publishers or query and consume scopes.");
            return default;
        }
        return Enter(new(publisher, true, null, null, false));
    }

    /// <summary>Owns an ambient query or consume context; a nested query reuses the outer one and owns nothing.</summary>
    public readonly struct Scope : IDisposable
    {
        readonly Context? _context;
        internal Scope(Context context) => _context = context;
        internal IReadOnlyList<Action> PostCommit => _context?.PostCommit ?? (IReadOnlyList<Action>)Array.Empty<Action>();
        public void Dispose()
        {
            if (_context is not { Active: true } context) return;
            if (!ReferenceEquals(Current.Value, context))
                throw new InvalidOperationException("Dispose the ambient scope in the flow that owns it.");
            context.Active = false;
            if (context.Query) context.Transaction?.Dispose();
            Current.Value = null;
        }
    }

    internal static Scope StartConsume(EventPublisher publisher, IObjectDBTransaction transaction, Event ev, bool replay)
    {
        if (Current.Value is { Active: true }) throw new InvalidOperationException("Ambient context already exists.");
        return Enter(new(publisher, false, transaction, ev, replay));
    }

    static Scope Enter(Context context)
    {
        Current.Value = context;
        return new(context);
    }

    public static ValueTask PublishEventAsync(PureEvent ev, CancellationToken cancellation = default) =>
        QueryPublisher().PublishAsync(ev, cancellation);

    public static Task<TResult> PublishAndWaitEventAsync<TResult>(EventWithResult<TResult> ev,
        CancellationToken cancellation = default) where TResult : class =>
        QueryPublisher().PublishAndWaitAsync(ev, cancellation);

    static EventPublisher QueryPublisher()
    {
        var context = GetContext();
        if (!context.Query) throw new InvalidOperationException("Publishing during consumption would duplicate events on every node.");
        return context.Publisher;
    }

    static Context GetContext() => Current.Value is { Active: true } context ? context
        : throw new InvalidOperationException("Ambient context is not active.");
}
