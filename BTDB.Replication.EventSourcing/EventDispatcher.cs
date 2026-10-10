namespace BTDB.Replication.EventSourcing;

/// <summary>Both operations are synchronous. Validation runs only at publication; consumption runs on every node.</summary>
public interface IEventDispatcher
{
    object? Validate(Event ev);
    object? Consume(Event ev);
}

/// <summary>Typed registration without reflection or process-global handler state. Register before constructing a publisher.</summary>
public sealed class EventDispatcher : IEventDispatcher
{
    readonly record struct Entry(Func<Event, object?> Validate, Func<Event, object?> Consume);

    readonly Dictionary<Type, Entry> _handlers = new();
    bool _frozen;

    public EventDispatcher Register<TEvent>(IEventHandler<TEvent> handler) where TEvent : PureEvent
    {
        ArgumentNullException.ThrowIfNull(handler);
        Add(typeof(TEvent), new(ev => { handler.Validate((TEvent)ev); return null; },
            ev => { handler.Consume((TEvent)ev); return null; }));
        return this;
    }

    public EventDispatcher Register<TEvent, TResult>(IEventHandler<TEvent, TResult> handler)
        where TEvent : EventWithResult<TResult> where TResult : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        Add(typeof(TEvent), new(ev => handler.Validate((TEvent)ev), ev => handler.Consume((TEvent)ev)));
        return this;
    }

    void Add(Type type, Entry entry)
    {
        if (_frozen) throw new InvalidOperationException("Register handlers before constructing the publisher.");
        _handlers.Add(type, entry);
    }

    internal void Freeze() => _frozen = true;

    public object? Validate(Event ev) => Handler(ev).Validate(ev);
    public object? Consume(Event ev) => Handler(ev).Consume(ev);

    Entry Handler(Event ev) =>
        _handlers.TryGetValue(ev.GetType(), out var handler) ? handler
            : throw new InvalidOperationException($"No event handler for {ev.GetType().FullName}.");
}
