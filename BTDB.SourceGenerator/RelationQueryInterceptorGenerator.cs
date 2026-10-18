using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

#pragma warning disable CS8620 // Params ReadOnlySpan<T> nullability warnings for direct arguments.

namespace BTDB.SourceGenerator;

/// Intercepts Queryable operators called on IRelation.Query&lt;T&gt;() chains written inline, so relation queries skip
/// building the operators' expressions and walking them for the plan cache. It needs a compiler with interceptable
/// locations (Roslyn 4.12) and the project's opt-in to the BTDB.Interceptors namespace in InterceptorsNamespaces.
[Generator]
public class RelationQueryInterceptorGenerator : IIncrementalGenerator
{
    public const string InterceptorsNamespace = "BTDB.Interceptors";

    static readonly HashSet<string> LinkOperators =
        ["Where", "Select", "OrderBy", "OrderByDescending", "ThenBy", "ThenByDescending", "Skip", "Take"];

    static readonly HashSet<string> TerminalOperators =
    [
        "Count", "LongCount", "Any", "All", "First", "FirstOrDefault", "Single", "SingleOrDefault", "Last",
        "LastOrDefault", "Min", "Max", "Sum", "Average"
    ];

    // Roslyn 4.12 API, found through reflection so the generator still loads on older compilers.
    static readonly System.Reflection.MethodInfo? GetInterceptableLocation =
        typeof(Microsoft.CodeAnalysis.CSharp.CSharpExtensions).GetMethod(
            "GetInterceptableLocation",
            [typeof(SemanticModel), typeof(InvocationExpressionSyntax), typeof(CancellationToken)]);

    // The InterceptsLocation attribute arguments of an InterceptableLocation.
    static readonly System.Reflection.PropertyInfo? LocationVersion =
        GetInterceptableLocation?.ReturnType.GetProperty("Version");

    static readonly System.Reflection.PropertyInfo? LocationData =
        GetInterceptableLocation?.ReturnType.GetProperty("Data");

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var enabled = context.ParseOptionsProvider.Select(static (options, _) => IsEnabled(options))
            .WithTrackingName("RelationQueryInterceptorsEnabled");
        var calls = context.SyntaxProvider.CreateSyntaxProvider(
                static (node, _) => node is InvocationExpressionSyntax
                {
                    Expression: MemberAccessExpressionSyntax { Name.Identifier.ValueText: var name }
                } && (LinkOperators.Contains(name) || TerminalOperators.Contains(name)),
                static (syntaxContext, cancellationToken) => Analyze(syntaxContext, cancellationToken))
            .WithTrackingName("RelationQueryCalls")
            .Where(static call => call != null)
            .Select(static (call, _) => call!);
        context.RegisterSourceOutput(calls.Collect().Combine(enabled),
            static (productionContext, pair) => Emit(productionContext, pair.Left, pair.Right));
    }

    static bool IsEnabled(ParseOptions options)
    {
        if (LocationData == null || options is not CSharpParseOptions csharpOptions) return false;
        foreach (var feature in new[] { "InterceptorsNamespaces", "InterceptorsPreviewNamespaces" })
        {
            if (!csharpOptions.Features.TryGetValue(feature, out var namespaces)) continue;
            if (namespaces.Split(';').Any(n => n.Trim() == InterceptorsNamespace)) return true;
        }

        return false;
    }

    static InterceptedCall? Analyze(GeneratorSyntaxContext syntaxContext, CancellationToken cancellationToken)
    {
        var invocation = (InvocationExpressionSyntax)syntaxContext.Node;
        var semanticModel = syntaxContext.SemanticModel;
        if (GetInterceptableLocation == null || LocationVersion == null || LocationData == null ||
            !IsSupported(invocation, semanticModel, cancellationToken))
            return null;
        var method = (IMethodSymbol)semanticModel.GetSymbolInfo(invocation, cancellationToken).Symbol!;
        var definition = (method.ReducedFrom ?? method).OriginalDefinition;
        object? location;
        try
        {
            location = GetInterceptableLocation.Invoke(null, [semanticModel, invocation, cancellationToken]);
        }
        catch (System.Reflection.TargetInvocationException)
        {
            return null;
        }

        if (location == null) return null;
        var attribute = "[global::System.Runtime.CompilerServices.InterceptsLocationAttribute(" +
                        LocationVersion.GetValue(location) + ", \"" + LocationData.GetValue(location) + "\")]";

        var format = SymbolDisplayFormat.FullyQualifiedFormat.WithMiscellaneousOptions(
            SymbolDisplayFormat.FullyQualifiedFormat.MiscellaneousOptions |
            SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);
        var returnType = definition.ReturnType.ToDisplayString(format);
        var elementType = LinkOperators.Contains(definition.Name)
            ? ((INamedTypeSymbol)definition.ReturnType).TypeArguments[0].ToDisplayString(format)
            : "";
        var sourceElementType =
            ((INamedTypeSymbol)definition.Parameters[0].Type).TypeArguments[0].ToDisplayString(format);
        return new(invocation.SyntaxTree.FilePath, invocation.SpanStart, definition.Name, attribute, returnType,
            elementType, sourceElementType,
            definition.TypeParameters.Select(p => p.Name).ToArray(),
            definition.Parameters.Select(p => p.Type.ToDisplayString(format)).ToArray(),
            definition.Parameters.Select(p => p.Name).ToArray());
    }

    // An operator of System.Linq.Queryable whose receiver is IRelation.Query<T>() or such an operator, and whose
    // expression arguments are lambdas written here, so every execution builds the same expression shape.
    static bool IsSupported(InvocationExpressionSyntax invocation, SemanticModel semanticModel,
        CancellationToken cancellationToken)
    {
        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess) return false;
        var name = memberAccess.Name.Identifier.ValueText;
        if (!LinkOperators.Contains(name) && !TerminalOperators.Contains(name)) return false;
        if (semanticModel.GetSymbolInfo(invocation, cancellationToken).Symbol is not IMethodSymbol
            {
                ReducedFrom: not null
            } method ||
            method.ContainingType.ToDisplayString() != "System.Linq.Queryable")
            return false;
        var definition = method.ReducedFrom!.OriginalDefinition;
        if (definition.Parameters.Length - 1 != invocation.ArgumentList.Arguments.Count) return false;
        for (var i = 1; i < definition.Parameters.Length; i++)
        {
            var parameterType = definition.Parameters[i].Type;
            var argument = invocation.ArgumentList.Arguments[i - 1];
            if (argument.NameColon != null || argument.RefKindKeyword.ValueText.Length != 0) return false;
            if (parameterType is INamedTypeSymbol { Name: "Expression", TypeArguments.Length: 1 } expressionType)
            {
                // Only lambdas of one parameter written here; indexed overloads are not supported.
                if (expressionType.TypeArguments[0] is not INamedTypeSymbol
                    {
                        DelegateInvokeMethod.Parameters.Length: 1
                    })
                    return false;
                if (argument.Expression is not LambdaExpressionSyntax) return false;
            }
            else if (parameterType.Name is "IComparer" or "IEqualityComparer")
            {
                return false;
            }
        }

        return IsRelationChain(memberAccess.Expression, semanticModel, cancellationToken);
    }

    static bool IsRelationChain(ExpressionSyntax receiver, SemanticModel semanticModel,
        CancellationToken cancellationToken)
    {
        while (receiver is ParenthesizedExpressionSyntax parenthesized) receiver = parenthesized.Expression;
        if (receiver is not InvocationExpressionSyntax invocation) return false;
        if (invocation.Expression is MemberAccessExpressionSyntax
            {
                Name: GenericNameSyntax { Identifier.ValueText: "Query" }
            } &&
            semanticModel.GetSymbolInfo(invocation, cancellationToken).Symbol is IMethodSymbol
            {
                Name: "Query", TypeArguments.Length: 1
            } query && IsRelation(query.ContainingType))
            return true;
        // Only operators which are links can be followed by another operator.
        return invocation.Expression is MemberAccessExpressionSyntax { Name.Identifier.ValueText: var name } &&
               LinkOperators.Contains(name) && IsSupported(invocation, semanticModel, cancellationToken);
    }

    static bool IsRelation(INamedTypeSymbol type) =>
        IsRelationInterface(type) || type.AllInterfaces.Any(IsRelationInterface);

    static bool IsRelationInterface(INamedTypeSymbol type) =>
        type.Name == "IRelation" && type.TypeArguments.Length == 0 && type.InODBLayerNamespace();

    static void Emit(SourceProductionContext context, IReadOnlyList<InterceptedCall> calls, bool enabled)
    {
        if (!enabled || calls.Count == 0) return;
        var builder = new StringBuilder();
        builder.Append("""
            // <auto-generated/>
            #nullable enable
            #pragma warning disable CS8603, CS8619, CS8620, CS9113

            namespace System.Runtime.CompilerServices
            {
                [global::System.AttributeUsage(global::System.AttributeTargets.Method, AllowMultiple = true)]
                file sealed class InterceptsLocationAttribute(int version, string data) : global::System.Attribute;
            }

            namespace BTDB.Interceptors
            {
                file static class RelationQueryInterceptors
                {

            """);
        // In source order, regardless of how the compilation was split.
        var ordered = calls.OrderBy(c => c.FilePath, StringComparer.Ordinal).ThenBy(c => c.Position).ToArray();
        for (var index = 0; index < ordered.Length; index++) EmitCall(builder, ordered[index], index);
        builder.Append("""
                }
            }

            """);
        context.AddSource("BTDB.RelationQueryInterceptors.g.cs", builder.ToString());
    }

    static void EmitCall(StringBuilder builder, InterceptedCall call, int index)
    {
        var typeParameters = call.TypeParameters.Count == 0 ? "" : "<" + string.Join(", ", call.TypeParameters) + ">";
        var parameters = string.Join(", ",
            call.ParameterTypes.Select((type, i) => (i == 0 ? "this " : "") + type + " " + call.ParameterNames[i]));
        var source = call.ParameterNames[0];
        var original = "global::System.Linq.Queryable." + call.Name + "(" + string.Join(", ", call.ParameterNames) +
                       ")";
        const string indent = "        ";
        builder.AppendLine(indent + "static readonly global::BTDB.ODBLayer.RelationQueryCallSite Site" + index +
                           " = new(\"" + call.Name + "\");");
        builder.AppendLine();
        builder.AppendLine(indent + call.Attribute);
        builder.AppendLine(indent + "public static " + call.ReturnType + " " + call.Name + index + typeParameters +
                           "(" + parameters + ")");
        builder.AppendLine(indent + "{");
        if (call.ElementType.Length != 0)
        {
            var argument = call.ParameterNames.Count > 1 ? call.ParameterNames[1] : "null";
            // ThenBy needs an IOrderedQueryable source, which the query of the previous link is.
            var query = call.ParameterTypes[0].Contains("IOrderedQueryable")
                ? "(" + call.ParameterTypes[0] + ")q"
                : "q";
            var materializeArguments = call.ParameterTypes.Count > 1
                ? query + ", (" + call.ParameterTypes[1] + ")a!"
                : query;
            builder.AppendLine(indent + "    return global::BTDB.ODBLayer.RelationQueryInterception.TryLink<" +
                               call.SourceElementType + ", " + call.ElementType + ", " + call.ReturnType + ">(");
            builder.AppendLine(indent + "        " + source + ", Site" + index + ", " + argument + ",");
            builder.AppendLine(indent + "        static (q, a) => global::System.Linq.Queryable." + call.Name + "(" +
                               materializeArguments + "), out var result)");
        }
        else
        {
            var argument1 = call.ParameterNames.Count > 1 ? call.ParameterNames[1] : "null";
            var argument2 = call.ParameterNames.Count > 2 ? call.ParameterNames[2] : "null";
            var executeArguments = "q";
            if (call.ParameterTypes.Count > 1) executeArguments += ", (" + call.ParameterTypes[1] + ")a1!";
            if (call.ParameterTypes.Count > 2) executeArguments += ", (" + call.ParameterTypes[2] + ")a2!";
            builder.AppendLine(indent + "    return global::BTDB.ODBLayer.RelationQueryInterception.TryExecute<" +
                               call.SourceElementType + ", " + call.ReturnType + ">(");
            builder.AppendLine(indent + "        " + source + ", Site" + index + ", " + argument1 + ", " + argument2 +
                               ",");
            builder.AppendLine(indent + "        static (q, a1, a2) => global::System.Linq.Queryable." + call.Name +
                               "(" + executeArguments + "), out var result)");
        }

        builder.AppendLine(indent + "        ? result");
        builder.AppendLine(indent + "        : " + original + ";");
        builder.AppendLine(indent + "}");
        builder.AppendLine();
    }

    sealed record InterceptedCall(
        string FilePath,
        int Position,
        string Name,
        string Attribute,
        string ReturnType,
        string ElementType,
        string SourceElementType,
        EquatableArray<string> TypeParameters,
        EquatableArray<string> ParameterTypes,
        EquatableArray<string> ParameterNames);
}
