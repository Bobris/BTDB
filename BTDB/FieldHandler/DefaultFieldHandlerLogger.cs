using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.IL;

namespace BTDB.FieldHandler;

public class DefaultFieldHandlerLogger : IFieldHandlerLogger
{
    readonly Action<string> _loggerFunction;

    public DefaultFieldHandlerLogger(Action<string>? loggerFunction = null)
    {
        _loggerFunction = loggerFunction ?? Console.WriteLine;
    }

    public void ReportTypeIncompatibility(Type? sourceType, IFieldHandler source, Type targetType,
        IFieldHandler? target)
    {
        if (target != null)
        {
            var details = new List<string> { "target: " + FormatType(targetType) };
            var nestedDifferences = DescribeNestedDifferences(source, target).ToArray();
            if (nestedDifferences.Length != 0)
            {
                details.AddRange(nestedDifferences);
            }
            else
            {
                var effectiveSourceType = sourceType ?? TryGetHandledType(source);
                if (effectiveSourceType != null)
                {
                    details.Add(FormatType(effectiveSourceType) + " -> " + FormatType(targetType));
                }
            }

            _loggerFunction.Invoke("Cannot load " + DescribeHandlerKind(source) + " into " +
                                   DescribeHandlerKind(target) + FormatDetails(details));
        }
        else if (sourceType != null)
        {
            _loggerFunction.Invoke("Cannot load " + FormatType(sourceType) + " into " +
                                   FormatType(targetType));
        }
        else
        {
            var details = new List<string>();
            var effectiveSourceType = TryGetHandledType(source);
            if (effectiveSourceType != null)
            {
                details.Add(FormatType(effectiveSourceType) + " -> " + FormatType(targetType));
            }

            _loggerFunction.Invoke("Cannot load " + DescribeHandlerKind(source) + " into " +
                                   FormatType(targetType) + FormatDetails(details));
        }
    }

    static Type? TryGetHandledType(IFieldHandler handler)
    {
        try
        {
            return handler.HandledType();
        }
        catch
        {
            return null;
        }
    }

    static string DescribeHandlerKind(IFieldHandler handler)
    {
        return handler.GetType().FullName ?? handler.GetType().Name;
    }

    static string DescribeHandler(IFieldHandler handler)
    {
        var description = handler.ToString();
        if (description == handler.GetType().ToString())
        {
            var handledType = TryGetHandledType(handler);
            if (handledType != null)
            {
                return DescribeHandlerKind(handler) + "<" + FormatType(handledType) + ">";
            }
        }

        return description;
    }

    static string FormatType(Type type)
    {
        return type.ToSimpleName().Split("__").First();
    }

    static string FormatDetails(IEnumerable<string> details)
    {
        var compact = details.Where(static detail => !string.IsNullOrEmpty(detail)).ToArray();
        return compact.Length == 0 ? "" : " (" + string.Join("; ", compact) + ")";
    }

    static IEnumerable<string> DescribeNestedDifferences(IFieldHandler source, IFieldHandler target)
    {
        if (source is not IFieldHandlerWithNestedFieldHandlers sourceNested ||
            target is not IFieldHandlerWithNestedFieldHandlers targetNested)
        {
            yield break;
        }

        var sourceHandlers = sourceNested.EnumerateNestedFieldHandlers().ToArray();
        var targetHandlers = targetNested.EnumerateNestedFieldHandlers().ToArray();
        var count = Math.Min(sourceHandlers.Length, targetHandlers.Length);
        for (var i = 0; i < count; i++)
        {
            foreach (var difference in DescribeDifferences(sourceHandlers[i], targetHandlers[i],
                         GetNestedName(i, count)))
            {
                yield return difference;
            }
        }

        if (sourceHandlers.Length != targetHandlers.Length)
        {
            yield return "nested handler count: " + sourceHandlers.Length + " -> " + targetHandlers.Length;
        }
    }

    static IEnumerable<string> DescribeDifferences(IFieldHandler source, IFieldHandler target, string path)
    {
        if (source is IFieldHandlerWithNestedFieldHandlers && target is IFieldHandlerWithNestedFieldHandlers)
        {
            foreach (var difference in DescribeNestedDifferencesWithPath(source, target, path))
            {
                yield return difference;
            }

            yield break;
        }

        var sourceDescription = DescribeHandler(source);
        var targetDescription = DescribeHandler(target);
        if (sourceDescription != targetDescription)
        {
            yield return path + ": " + sourceDescription + " -> " + targetDescription;
        }
    }

    static IEnumerable<string> DescribeNestedDifferencesWithPath(IFieldHandler source, IFieldHandler target, string path)
    {
        var nested = DescribeNestedDifferences(source, target).ToArray();
        if (nested.Length != 0)
        {
            foreach (var difference in nested)
            {
                yield return path + "." + difference;
            }

            yield break;
        }

        var sourceDescription = DescribeHandler(source);
        var targetDescription = DescribeHandler(target);
        if (sourceDescription != targetDescription)
        {
            yield return path + ": " + sourceDescription + " -> " + targetDescription;
        }
    }

    static string GetNestedName(int index, int count)
    {
        if (count == 1) return "item";
        if (count == 2) return index == 0 ? "key" : "value";
        return "item" + (index + 1);
    }
}
