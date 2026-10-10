namespace BTDB.Replication.EventSourcing;

[Generate]
public class Event
{
    /// <summary>EventLog offset plus one; zero during publication and validation.</summary>
    public ulong EventId { get; set; }
    /// <summary>UTC publication time, serialized once and shared by every consumer.</summary>
    public DateTime When { get; set; }
}

public class PureEvent : Event;

public class EventWithResult<TResult> : Event where TResult : class;

public interface IEventHandler<in TEvent> where TEvent : PureEvent
{
    void Validate(TEvent ev);
    void Consume(TEvent ev);
}

public interface IEventHandler<in TEvent, out TResult>
    where TEvent : EventWithResult<TResult> where TResult : class
{
    /// <summary>Null accepts the event; a result rejects publication and is returned to the caller.</summary>
    TResult? Validate(TEvent ev);
    TResult Consume(TEvent ev);
}

public interface IEventPublisher
{
    ValueTask PublishAsync(PureEvent ev, CancellationToken cancellation = default);
    Task<TResult> PublishAndWaitAsync<TResult>(EventWithResult<TResult> ev, CancellationToken cancellation = default)
        where TResult : class;
}
