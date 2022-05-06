using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.IL;

public static class EmitHelpers
{
    public static MethodInfo GetMethodInfo(Expression<Action> expression)
    {
        return ((MethodCallExpression)expression.Body).Method;
    }

    public static PropertyInfo GetPropertyInfo<TType, TReturn>(this Expression<Func<TType, TReturn>> property)
    {
        LambdaExpression lambda = property;
        var memberExpression = lambda.Body is UnaryExpression expression
            ? (MemberExpression) expression.Operand
            : (MemberExpression) lambda.Body;

        return (PropertyInfo) memberExpression.Member;
    }

    public static T CreateDelegate<T>(this MethodInfo mi) where T : class
    {
        return (T)(object)Delegate.CreateDelegate(typeof(T), mi);
    }

    [return: NotNullIfNotNull("type")]
    public static Type? UnwrapTask(this Type? type)
    {
        if (type == null) return null;
        if (type == typeof(Task)) return typeof(void);
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Task<>))
            return type.GetGenericArguments()[0];
        return type;
    }

    public static bool IsDelegate(this Type type)
    {
        return type.IsSubclassOf(typeof(Delegate));
    }

    public static bool InheritsOrImplements(this Type child, Type parent)
    {
        parent = ResolveGenericTypeDefinition(parent);

        var currentChild = child.IsGenericType
            ? child.GetGenericTypeDefinition()
            : child;

        while (currentChild != typeof(object))
        {
            if (parent == currentChild || HasAnyInterfaces(parent, currentChild))
                return true;

            currentChild = currentChild.BaseType != null
                           && currentChild.BaseType.IsGenericType
                ? currentChild.BaseType.GetGenericTypeDefinition()
                : currentChild.BaseType;

            if (currentChild == null)
                return false;
        }

        return false;
    }

    static bool HasAnyInterfaces(Type parent, Type child)
    {
        return child.GetInterfaces()
            .Any(childInterface =>
            {
                var currentInterface = childInterface.IsGenericType
                    ? childInterface.GetGenericTypeDefinition()
                    : childInterface;

                return currentInterface == parent;
            });
    }

    static Type ResolveGenericTypeDefinition(Type parent)
    {
        var shouldUseGenericType = !(parent.IsGenericType && parent.GetGenericTypeDefinition() != parent);
        if (parent.IsGenericType && shouldUseGenericType)
            parent = parent.GetGenericTypeDefinition();
        return parent;
    }

    public static Type? SpecializationOf(this Type impl, Type generic)
    {
        var currentImpl = impl;

        while (currentImpl != typeof(object))
        {
            if (currentImpl.IsGenericType && currentImpl.GetGenericTypeDefinition() == generic)
                return currentImpl;
            var specific = SpecificInterfaces(generic, currentImpl);
            if (specific != null)
                return specific;
            currentImpl = currentImpl.BaseType;
            if (currentImpl == null)
                return null;
        }

        return null;
    }

    static Type? SpecificInterfaces(Type generic, Type impl)
    {
        foreach (var childInterface in impl.GetInterfaces())
        {
            if (childInterface.IsGenericType && childInterface.GetGenericTypeDefinition() == generic)
            {
                return childInterface;
            }
        }

        return null;
    }

    public static string ToSimpleName(this Type? type)
    {
        if (type == null) return "";
        if (type.IsArray)
            return
                $"{ToSimpleName(type.GetElementType())}[{new string(',', type.GetArrayRank() - 1)}]";
        if (type.IsGenericType)
        {
            var simpleName = type.Name;
            var backTickPos = simpleName.IndexOf('`');
            if (backTickPos > 0) simpleName = simpleName.Substring(0, backTickPos);
            return string.Format(type.Namespace == "System" ? "{1}<{2}>" : "{0}.{1}<{2}>",
                type.Namespace,
                simpleName,
                string.Join(",", type.GetGenericArguments().Select(p => p.ToSimpleName())));
        }

        if (type == typeof(byte)) return "byte";
        if (type == typeof(sbyte)) return "sbyte";
        if (type == typeof(ushort)) return "ushort";
        if (type == typeof(short)) return "short";
        if (type == typeof(int)) return "int";
        if (type == typeof(uint)) return "uint";
        if (type == typeof(long)) return "long";
        if (type == typeof(ulong)) return "ulong";
        if (type == typeof(float)) return "float";
        if (type == typeof(double)) return "double";
        if (type == typeof(string)) return "string";
        if (type == typeof(char)) return "char";
        if (type == typeof(bool)) return "bool";
        if (type == typeof(void)) return "void";
        if (type == typeof(object)) return "object";
        if (type == typeof(decimal)) return "decimal";
        if (string.IsNullOrEmpty(type.Namespace)) return type.Name;
        return type.Namespace + "." + type.Name;
    }

    public static IILMethod GenerateINotifyPropertyChangedImpl(IILDynamicType typeBuilder)
    {
        var fieldBuilder = typeBuilder.DefineField("_propertyChanged", typeof(PropertyChangedEventHandler),
            FieldAttributes.Private);
        var eventBuilder = typeBuilder.DefineEvent("PropertyChanged", EventAttributes.None,
            typeof(PropertyChangedEventHandler));
        eventBuilder.SetAddOnMethod(GenerateAddRemoveEvent(typeBuilder, fieldBuilder, true));
        eventBuilder.SetRemoveOnMethod(GenerateAddRemoveEvent(typeBuilder, fieldBuilder, false));
        var methodBuilder = typeBuilder.DefineMethod("RaisePropertyChanged", null, new[] { typeof(string) },
            MethodAttributes.Family);
        var ilGenerator = methodBuilder.Generator;
        ilGenerator.DeclareLocal(typeof(PropertyChangedEventHandler));
        var labelRet = ilGenerator.DefineLabel();
        ilGenerator
            .Ldarg(0)
            .Ldfld(fieldBuilder)
            .Stloc(0)
            .Ldloc(0)
            .BrfalseS(labelRet)
            .Ldloc(0)
            .Ldarg(0)
            .Ldarg(1)
            .Newobj(() => new PropertyChangedEventArgs(null))
            .Callvirt(() => default(PropertyChangedEventHandler).Invoke(null, null))
            .Mark(labelRet)
            .Ret();
        return methodBuilder;
    }

    static IILMethod GenerateAddRemoveEvent(IILDynamicType typeBuilder, IILField fieldBuilder, bool add)
    {
        Type typePropertyChangedEventHandler = typeof(PropertyChangedEventHandler);
        EventInfo eventPropertyChanged = typeof(INotifyPropertyChanged).GetEvent("PropertyChanged");
        var methodBuilder = typeBuilder.DefineMethod((add ? "add" : "remove") + "_PropertyChanged",
            typeof(void), new[] { typePropertyChangedEventHandler },
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName |
            MethodAttributes.HideBySig | MethodAttributes.NewSlot | MethodAttributes.Final);
        var ilGenerator = methodBuilder.Generator;
        ilGenerator.DeclareLocal(typePropertyChangedEventHandler);
        ilGenerator.DeclareLocal(typePropertyChangedEventHandler);
        ilGenerator.DeclareLocal(typePropertyChangedEventHandler);
        var label = ilGenerator.DefineLabel();
        ilGenerator
            .Ldarg(0)
            .Ldfld(fieldBuilder)
            .Stloc(0)
            .Mark(label)
            .Ldloc(0)
            .Stloc(1)
            .Ldloc(1)
            .Ldarg(1)
            .Call(add
                ? GetMethodInfo(() => Delegate.Combine(null, null))
                : GetMethodInfo(() => Delegate.Remove(null, null)))
            .Castclass(typePropertyChangedEventHandler)
            .Stloc(2)
            .Ldarg(0)
            .Ldflda(fieldBuilder)
            .Ldloc(2)
            .Ldloc(1);
        PropertyChangedEventHandler stub = null;
        ilGenerator
            .Call(() => Interlocked.CompareExchange(ref stub, null, null))
            .Stloc(0)
            .Ldloc(0)
            .Ldloc(1)
            .BneUnS(label)
            .Ret();
        typeBuilder.DefineMethodOverride(methodBuilder,
            add ? eventPropertyChanged.GetAddMethod() : eventPropertyChanged.GetRemoveMethod());
        return methodBuilder;
    }

    public static void GenerateJumpIfEqual(IILGen ilGenerator, Type type, IILLabel jumpTo, Action<IILGen> loadLeft,
        Action<IILGen> loadRight)
    {
        if (type == typeof(sbyte) || type == typeof(byte) || type == typeof(short) || type == typeof(ushort)
            || type == typeof(int) || type == typeof(uint) || type == typeof(long) || type == typeof(ulong)
            || type == typeof(float) || type == typeof(double) || type == typeof(bool) || type.IsEnum)
        {
            ilGenerator
                .Do(loadLeft)
                .Do(loadRight)
                .BeqS(jumpTo);
            return;
        }

        if (type.IsGenericType)
        {
            var genType = type.GetGenericTypeDefinition();
            if (genType == typeof(Nullable<>))
            {
                var localLeft = ilGenerator.DeclareLocal(type, "left");
                var localRight = ilGenerator.DeclareLocal(type, "right");
                var hasValueMethod = type.GetMethod("get_HasValue");
                var getValueMethod = type.GetMethod("GetValueOrDefault", Type.EmptyTypes);
                var labelLeftHasValue = ilGenerator.DefineLabel("leftHasValue");
                var labelDifferent = ilGenerator.DefineLabel("different");
                ilGenerator
                    .Do(loadLeft)
                    .Stloc(localLeft)
                    .Do(loadRight)
                    .Stloc(localRight)
                    .Ldloca(localLeft)
                    .Call(hasValueMethod)
                    .BrtrueS(labelLeftHasValue)
                    .Ldloca(localRight)
                    .Call(hasValueMethod)
                    .BrtrueS(labelDifferent)
                    .BrS(jumpTo)
                    .Mark(labelLeftHasValue)
                    .Ldloca(localRight)
                    .Call(hasValueMethod)
                    .BrfalseS(labelDifferent);
                GenerateJumpIfEqual(
                    ilGenerator,
                    type.GetGenericArguments()[0],
                    jumpTo,
                    g => g.Ldloca(localLeft).Call(getValueMethod),
                    g => g.Ldloca(localRight).Call(getValueMethod));
                ilGenerator.Mark(labelDifferent);
                return;
            }
        }

        var equalsMethod = type.GetMethod("Equals", new[] { type, type })
                           ?? type.GetMethod("op_Equality", new[] { type, type });
        if (equalsMethod != null)
        {
            loadLeft(ilGenerator);
            loadRight(ilGenerator);
            ilGenerator
                .Call(equalsMethod)
                .BrtrueS(jumpTo);
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(type), $"Don't know how to compare type {type}");
    }

    // Mostly taken from https://stackoverflow.com/questions/58453972/how-to-use-net-reflection-to-check-for-nullable-reference-type
    public static bool IsNullable(Type enclosingType, PropertyInfo property)
    {
#if DEBUG
        if (!enclosingType
            .GetProperties(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public |
                           BindingFlags.NonPublic | BindingFlags.DeclaredOnly).Contains(property))
            throw new ArgumentException("enclosingType must be the type which defines property");
#endif
        var nullable = property.CustomAttributes
            .FirstOrDefault(x => x.AttributeType.FullName == "System.Runtime.CompilerServices.NullableAttribute");
        if (nullable != null && nullable.ConstructorArguments.Count == 1)
        {
            var attributeArgument = nullable.ConstructorArguments[0];
            if (attributeArgument.ArgumentType == typeof(byte[]))
            {
                var args = (ReadOnlyCollection<CustomAttributeTypedArgument>)attributeArgument.Value;
                if (args!.Count > 0 && args[0].ArgumentType == typeof(byte))
                {
                    return (byte)args[0].Value! == 2;
                }
            }
            else if (attributeArgument.ArgumentType == typeof(byte))
            {
                return (byte)attributeArgument.Value! == 2;
            }
        }

        var context = enclosingType.CustomAttributes
            .FirstOrDefault(x =>
                x.AttributeType.FullName == "System.Runtime.CompilerServices.NullableContextAttribute");
        if (context != null &&
            context.ConstructorArguments.Count == 1 &&
            context.ConstructorArguments[0].ArgumentType == typeof(byte))
        {
            return (byte)context.ConstructorArguments[0].Value! == 2;
        }

        return false;
    }

    public static MethodInfo? GetAnyGetMethod(this PropertyInfo pi)
    {
        var res = pi.GetGetMethod(true);

        if (res == null)
            res = pi.DeclaringType?.GetProperty(pi.Name)?.GetGetMethod(true);
        return res;
    }

    public static MethodInfo? GetAnySetMethod(this PropertyInfo pi)
    {
        var res = pi.GetSetMethod(true);

        if (res == null)
            res = pi.DeclaringType?.GetProperty(pi.Name)?.GetSetMethod(true);
        return res;
    }

    public static ConstructorInfo? GetDefaultConstructor(this Type instanceType)
    {
        return instanceType.GetConstructor(
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            null, Type.EmptyTypes, null);
    }
}
