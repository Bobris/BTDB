using System;
using System.Linq;
using System.Threading.Tasks;
using BTDB.FieldHandler;
using BTDB.ODBLayer;

namespace BTDB.Replication.ProcessTests;

public class Order
{
    [PrimaryKey] public ulong Id { get; set; }
    public string Customer { get; set; } = "";
    public int Amount { get; set; }
}

public class IndexedOrder
{
    [PrimaryKey] public ulong Id { get; set; }
    [SecondaryKey("Customer")] public string Customer { get; set; } = "";
    public int Amount { get; set; }
}

public class Rejection
{
    [PrimaryKey] public ulong EventId { get; set; }
    public string Reason { get; set; } = "";
}

[PersistedName("Orders")]
public interface IOrders : IRelation<Order>;

// Generation 2 adds a secondary index to the same persisted relation.
[PersistedName("Orders")]
public interface IIndexedOrders : IRelation<IndexedOrder>
{
    int CountByCustomer(string customer);
}

[PersistedName("Rejections")]
public interface IRejections : IRelation<Rejection>;

internal sealed record OrderSummary(ulong CommitUlong, int Orders, long Total, int Rejections, int IndexedFirstCustomer);

/// <summary>A small ObjectDB application for the subprocess tests: every event inserts an order inside a virtual batch;
/// an order with a negative amount is rejected after its insert, so its transaction rolls back inside the batch and a
/// second transaction with the same event ID records the rejection.</summary>
internal static class OrderApplication
{
    public static Type[] Relations(ulong generation) =>
        [generation >= 2 ? typeof(IIndexedOrders) : typeof(IOrders), typeof(IRejections)];

    public static (string Customer, int Amount) Event(ulong id) => ("c" + id % 5, id % 7 == 0 ? -(int)id : (int)id);

    public static async Task ApplyAsync(IObjectDB db, ulong generation, ulong id)
    {
        var (customer, amount) = Event(id);
        try
        {
            using var transaction = await db.StartWritingTransaction(id, inBatch: true);
            if (generation >= 2)
                transaction.GetRelation<IIndexedOrders>().Upsert(new() { Id = id, Customer = customer, Amount = amount });
            else transaction.GetRelation<IOrders>().Upsert(new() { Id = id, Customer = customer, Amount = amount });
            if (amount < 0) throw new InvalidOperationException("Negative amount.");
            transaction.Commit();
        }
        catch (InvalidOperationException error)
        {
            using var transaction = await db.StartWritingTransaction(id, inBatch: true);
            transaction.GetRelation<IRejections>().Upsert(new() { EventId = id, Reason = error.Message });
            transaction.Commit();
        }
    }

    public static OrderSummary Summary(IObjectDB db, ulong generation)
    {
        using var transaction = db.StartReadOnlyTransaction();
        var rejections = transaction.GetRelation<IRejections>().Count;
        if (generation >= 2)
        {
            var orders = transaction.GetRelation<IIndexedOrders>();
            return new(transaction.GetCommitUlong(), orders.Count, orders.Sum(o => (long)o.Amount), rejections,
                orders.CountByCustomer("c1"));
        }
        var old = transaction.GetRelation<IOrders>();
        return new(transaction.GetCommitUlong(), old.Count, old.Sum(o => (long)o.Amount), rejections, -1);
    }
}
