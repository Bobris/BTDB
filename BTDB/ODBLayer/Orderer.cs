using System;
using System.Linq.Expressions;
using BTDB.EventStoreLayer;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public interface IOrderer
{
    Type? ExpectedInput { get; }
    string? ColumnName { get; }
    void CopyOrderedField(ref SpanReader reader, uint start, uint length, ref SpanWriter writer);
}

public class Orderer
{
    public static IOrderer Ascending<TInput, TBy>(Expression<Func<TInput, TBy>> byGetter)
    {
        var propInfo = byGetter.GetPropertyInfo();
        if (propInfo.PropertyType != typeof(TBy)) throw new ArgumentException("Property getter is not returned as is");
        if (propInfo.DeclaringType != typeof(TInput)) throw new ArgumentException("Property getter is not called on "+typeof(TInput).ToSimpleName());
        return new AscendingPropertyOrderer(typeof(TInput), ObjectTypeDescriptor.GetPersistentName(propInfo));
    }

    public static IOrderer Descending<TInput, TBy>(Expression<Func<TInput, TBy>> byGetter)
    {
        return new FlipOrder(Ascending(byGetter));
    }
}

class AscendingPropertyOrderer : IOrderer
{
    readonly Type _ownerType;
    readonly string _propName;

    public AscendingPropertyOrderer(Type ownerType, string propName)
    {
        _ownerType = ownerType;
        _propName = propName;
    }

    public Type? ExpectedInput => _ownerType;
    public string? ColumnName => _propName;

    public void CopyOrderedField(ref SpanReader reader, uint start, uint length, ref SpanWriter writer)
    {
        reader.CopyAbsoluteToWriter(start, length, ref writer);
    }
}

class FlipOrder : IOrderer
{
    readonly IOrderer _wrapped;

    public FlipOrder(IOrderer wrapped)
    {
        _wrapped = wrapped;
    }

    public Type? ExpectedInput => _wrapped.ExpectedInput;

    public string? ColumnName => _wrapped.ColumnName;

    public void CopyOrderedField(ref SpanReader reader, uint start, uint length, ref SpanWriter writer)
    {
        var writeStart = writer.StartXor();
        _wrapped.CopyOrderedField(ref reader, start, length, ref writer);
        writer.FinishXor(writeStart);
    }
}
