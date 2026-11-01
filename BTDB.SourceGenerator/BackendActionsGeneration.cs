using System;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

#pragma warning disable CS8620 // Params ReadOnlySpan<T> nullability warnings for direct arguments.

namespace BTDB.SourceGenerator;

/// One input a handler handles, with the values registration otherwise reads from the input's attributes.
record BackendActionInfo(
    string InputType,
    string ResultType,
    string? Name,
    int Cpu,
    int MaxConcurrent,
    string? ConcurrencyGroup,
    bool SingleRunningPerTenant,
    string? Schedule,
    bool InputHasDefaultConstructor);

record BackendActionHandlerInfo(
    string HintName,
    string HandlerType,
    bool HandlerHasDefaultConstructor,
    EquatableArray<BackendActionInfo> Actions);

/// Registers every concrete class implementing BTDB.BackendActions.IActionHandler&lt;TInput, TResult&gt; in a module
/// initializer, with its inputs' [BackendAction] and schedule values, so BackendActions.RegisterGenerated needs no
/// reflection.
static class BackendActionsGeneration
{
    const string Namespace = "BTDB.BackendActions";

    internal static void Register(IncrementalGeneratorInitializationContext context)
    {
        var handlers = context.SyntaxProvider.CreateSyntaxProvider(
                static (node, _) => node is ClassDeclarationSyntax { BaseList: not null },
                static (syntaxContext, cancellation) => Analyze(syntaxContext, cancellation))
            .WithTrackingName("BackendActionHandlers")
            .Where(static handler => handler != null)
            .Select(static (handler, _) => handler!);
        context.RegisterSourceOutput(handlers, static (production, handler) =>
            production.AddSource(handler.HintName, Emit(handler)));
    }

    static BackendActionHandlerInfo? Analyze(GeneratorSyntaxContext syntaxContext, CancellationToken cancellation)
    {
        if (syntaxContext.SemanticModel.GetDeclaredSymbol(syntaxContext.Node, cancellation) is not INamedTypeSymbol
            handler)
            return null;
        // Partial classes have several declarations; analyze the first only.
        if (handler.DeclaringSyntaxReferences.Length > 1 &&
            handler.DeclaringSyntaxReferences[0].GetSyntax(cancellation) != syntaxContext.Node)
            return null;
        if (handler.IsAbstract || handler.IsStatic || handler.IsGenericType || !Accessible(handler)) return null;
        var actions = handler.AllInterfaces.Where(IsActionHandler)
            .Where(i => !ImplementedByRegisteredBase(handler, i))
            .Select(i => Action((INamedTypeSymbol)i.TypeArguments[0], i.TypeArguments[1]))
            .ToArray();
        if (actions.Length == 0) return null;
        var name = handler.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        var hint = "BackendActions." + handler.ToDisplayString().Replace('<', '[').Replace('>', ']') + ".g.cs";
        return new(hint, name, HasDefaultConstructor(handler), actions);
    }

    static bool IsActionHandler(INamedTypeSymbol type) => type is
    {
        Name: "IActionHandler", Arity: 2, ContainingNamespace: var ns
    } && ns.ToDisplayString() == Namespace;

    // A concrete, non-generic base class that implements the interface itself gets the registration instead.
    static bool ImplementedByRegisteredBase(INamedTypeSymbol handler, INamedTypeSymbol implemented)
    {
        for (var type = handler.BaseType; type != null; type = type.BaseType)
            if (!type.IsAbstract && !type.IsGenericType && type.AllInterfaces.Contains(implemented, SymbolEqualityComparer.Default))
                return true;
        return false;
    }

    static bool Accessible(INamedTypeSymbol type)
    {
        for (ISymbol? current = type; current is INamedTypeSymbol named; current = named.ContainingType)
            if (named.DeclaredAccessibility is not (Accessibility.Public or Accessibility.Internal))
                return false;
        return true;
    }

    static bool HasDefaultConstructor(INamedTypeSymbol type) => type.InstanceConstructors.Any(c =>
        c.Parameters.Length == 0 && c.DeclaredAccessibility is Accessibility.Public or Accessibility.Internal);

    static BackendActionInfo Action(INamedTypeSymbol input, ITypeSymbol result)
    {
        string? name = null, group = null, schedule = null;
        int cpu = 100, maxConcurrent = 0;
        var single = false;
        foreach (var attribute in input.GetAttributes())
        {
            if (attribute.AttributeClass is not { } attributeClass) continue;
            if (attributeClass.Name == "BackendActionAttribute" && attributeClass.ContainingNamespace.ToDisplayString() == Namespace)
            {
                foreach (var argument in attribute.NamedArguments)
                    switch (argument.Key)
                    {
                        case "Name": name = argument.Value.Value as string; break;
                        case "Cpu": cpu = (int)argument.Value.Value!; break;
                        case "MaxConcurrent": maxConcurrent = (int)argument.Value.Value!; break;
                        case "ConcurrencyGroup": group = argument.Value.Value as string; break;
                        case "SingleRunningPerTenant": single = (bool)argument.Value.Value!; break;
                    }
            }
            else if (IsSchedule(attributeClass))
                schedule = Construct(attribute);
        }
        return new(input.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            result.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), name, cpu, maxConcurrent, group, single,
            schedule, HasDefaultConstructor(input));
    }

    static bool IsSchedule(INamedTypeSymbol attribute)
    {
        for (var type = attribute.BaseType; type != null; type = type.BaseType)
            if (type.Name == "ActionScheduleAttribute" && type.ContainingNamespace.ToDisplayString() == Namespace)
                return true;
        return false;
    }

    // The attribute's construction as an expression, with its constructor and named arguments.
    static string Construct(AttributeData attribute)
    {
        var code = new StringBuilder("new ")
            .Append(attribute.AttributeClass!.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)).Append('(')
            .Append(string.Join(", ", attribute.ConstructorArguments.Select(a => a.ToCSharpString()))).Append(')');
        if (attribute.NamedArguments.Length != 0)
            code.Append(" { ")
                .Append(string.Join(", ", attribute.NamedArguments.Select(a => a.Key + " = " + a.Value.ToCSharpString())))
                .Append(" }");
        return code.ToString();
    }

    static string Literal(string? value) => value == null ? "null"
        : "\"" + value.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";

    static string Emit(BackendActionHandlerInfo handler)
    {
        var code = new StringBuilder();
        code.Append("// <auto-generated/>\n#nullable enable\n\nnamespace BTDB.BackendActions.Generated;\n\n");
        code.Append("file static class Registration\n{\n");
        code.Append("    [global::System.Runtime.CompilerServices.ModuleInitializer]\n");
        code.Append("    internal static void Register()\n    {\n");
        foreach (var action in handler.Actions)
        {
            code.Append("        global::BTDB.BackendActions.GeneratedActions.Add(new global::BTDB.BackendActions.GeneratedAction<")
                .Append(action.InputType).Append(", ").Append(action.ResultType).Append(">(\n");
            code.Append("            typeof(").Append(handler.HandlerType).Append("), ").Append(Literal(action.Name))
                .Append(", ").Append(action.Cpu).Append(", ").Append(action.MaxConcurrent).Append(", ")
                .Append(Literal(action.ConcurrencyGroup)).Append(", ").Append(action.SingleRunningPerTenant ? "true" : "false")
                .Append(",\n");
            code.Append("            ").Append(action.Schedule ?? "null").Append(",\n");
            code.Append("            ").Append(action.InputHasDefaultConstructor ? "static () => new " + action.InputType + "()" : "null")
                .Append(",\n");
            code.Append("            ").Append(handler.HandlerHasDefaultConstructor
                ? "static () => new " + handler.HandlerType + "()" : "null").Append("));\n");
        }
        code.Append("    }\n}\n");
        return code.ToString();
    }
}
