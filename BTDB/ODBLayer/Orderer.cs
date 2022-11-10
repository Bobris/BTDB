using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.EventStoreLayer;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public interface IOrderer
{
    Type? ExpectedInput { get; }
    string? ColumnName { get; }
    void CopyOrderedField(scoped ReadOnlySpan<byte> key, scoped ref SpanWriter writer);
}

public class Orderer
{
    public static IOrderer GenericAscending<TInput, TBy>(Expression<Func<TInput, TBy>> byGetter)
    {
        var propInfo = byGetter.GetPropertyInfo();
        if (propInfo.PropertyType != typeof(TBy)) throw new ArgumentException("Property getter is not returned as is");
        if (propInfo.DeclaringType != typeof(TInput)) throw new ArgumentException("Property getter is not called on "+typeof(TInput).ToSimpleName());
        return new AscendingPropertyOrderer(null, ObjectTypeDescriptor.GetPersistentName(propInfo));
    }

    public static IOrderer Ascending<TInput, TBy>(Expression<Func<TInput, TBy>> byGetter)
    {
        var propInfo = byGetter.GetPropertyInfo();
        if (propInfo.PropertyType != typeof(TBy)) throw new ArgumentException("Property getter is not returned as is");
        if (propInfo.DeclaringType != typeof(TInput)) throw new ArgumentException("Property getter is not called on "+typeof(TInput).ToSimpleName());
        return new AscendingPropertyOrderer(typeof(TInput), ObjectTypeDescriptor.GetPersistentName(propInfo));
    }

    public static IOrderer AscendingStringByLocale<TInput>(Expression<Func<TInput, string>> byGetter,
        CompareInfo compareInfo, CompareOptions compareOptions = CompareOptions.None)
    {
        var propInfo = byGetter.GetPropertyInfo();
        if (propInfo.PropertyType != typeof(string)) throw new ArgumentException("Property getter is not returned as is");
        if (propInfo.DeclaringType != typeof(TInput)) throw new ArgumentException("Property getter is not called on "+typeof(TInput).ToSimpleName());
        return new AscendingLocalePropertyOrderer(typeof(TInput), ObjectTypeDescriptor.GetPersistentName(propInfo), compareInfo, compareOptions);
    }

    public static IOrderer Descending<TInput, TBy>(Expression<Func<TInput, TBy>> byGetter)
    {
        return new FlipOrder(Ascending(byGetter));
    }

    public static IOrderer GenericDescending<TInput, TBy>(Expression<Func<TInput, TBy>> byGetter)
    {
        return new FlipOrder(GenericAscending(byGetter));
    }

    public static IOrderer Backwards(IOrderer orderer)
    {
        if (orderer is FlipOrder flipOrder)
        {
            return flipOrder.Wrapped;
        }
        return new FlipOrder(orderer);
    }
}

class AscendingLocalePropertyOrderer : IOrderer
{
    readonly Type _ownerType;
    readonly string _propName;
    readonly CompareOptions _compareOptions;
    readonly CompareInfo _compareInfo;

    public AscendingLocalePropertyOrderer(Type ownerType, string propName, CompareInfo compareInfo,
        CompareOptions compareOptions)
    {
        _ownerType = ownerType;
        _propName = propName;
        _compareOptions = compareOptions;
        _compareInfo = compareInfo;
    }

    public Type? ExpectedInput => _ownerType;
    public string? ColumnName => _propName;

    [SkipLocalsInit]
    public void CopyOrderedField(scoped ReadOnlySpan<byte> key, scoped ref SpanWriter writer)
    {
        Span<char> bufStr = stackalloc char[512];
        SpanReader reader = new (key);
        var realStr = reader.ReadStringOrderedAsSpan(ref MemoryMarshal.GetReference(bufStr), bufStr.Length);
        var keyLength = _compareInfo.GetSortKeyLength(realStr, _compareOptions);
        var keySpan = writer.BlockWriteToSpan(keyLength);
        _compareInfo.GetSortKey(realStr, keySpan, _compareOptions);
    }
}

class AscendingPropertyOrderer : IOrderer
{
    readonly Type? _ownerType;
    readonly string _propName;

    public AscendingPropertyOrderer(Type? ownerType, string propName)
    {
        _ownerType = ownerType;
        _propName = propName;
    }

    public Type? ExpectedInput => _ownerType;
    public string? ColumnName => _propName;

    public void CopyOrderedField(scoped ReadOnlySpan<byte> key, scoped ref SpanWriter writer)
    {
        writer.WriteBlock(key);
    }
}

class FlipOrder : IOrderer
{
    public FlipOrder(IOrderer wrapped)
    {
        Wrapped = wrapped;
    }

    public Type? ExpectedInput => Wrapped.ExpectedInput;

    public string? ColumnName => Wrapped.ColumnName;

    public IOrderer Wrapped { get; }

    public void CopyOrderedField(scoped ReadOnlySpan<byte> key, scoped ref SpanWriter writer)
    {
        var writeStart = writer.StartXor();
        Wrapped.CopyOrderedField(key, ref writer);
        writer.FinishXor(writeStart);
    }
}
