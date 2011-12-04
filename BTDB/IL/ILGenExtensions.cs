using System;
using System.Linq.Expressions;
using System.Reflection;

namespace BTDB.IL
{
    public static class ILGenExtensions
    {
        public static IILGen Callvirt(this IILGen il, Expression<Action> expression)
        {
            var methodInfo = (expression.Body as MethodCallExpression).Method;
            return il.Callvirt(methodInfo);
        }

        public static IILGen Callvirt<T>(this IILGen il, Expression<Func<T>> expression)
        {
            var newExpression = expression.Body as MemberExpression;
            if (newExpression != null)
            {
                return il.Callvirt(((PropertyInfo)newExpression.Member).GetGetMethod(true));
            }
            var methodInfo = (expression.Body as MethodCallExpression).Method;
            return il.Callvirt(methodInfo);
        }

        public static IILGen Call(this IILGen il, Expression<Action> expression)
        {
            var newExpression = expression.Body as NewExpression;
            if (newExpression != null)
            {
                return il.Call(newExpression.Constructor);
            }
            var methodInfo = (expression.Body as MethodCallExpression).Method;
            return il.Call(methodInfo);
        }

        public static IILGen Call<T>(this IILGen il, Expression<Func<T>> expression)
        {
            var memberExpression = expression.Body as MemberExpression;
            if (memberExpression != null)
            {
                return il.Call(((PropertyInfo)memberExpression.Member).GetGetMethod(true));
            }
            var newExpression = expression.Body as NewExpression;
            if (newExpression != null)
            {
                return il.Call(newExpression.Constructor);
            }
            var methodInfo = (expression.Body as MethodCallExpression).Method;
            return il.Call(methodInfo);
        }

        public static IILGen Newobj(this IILGen il, Expression<Action> expression)
        {
            var constructorInfo = (expression.Body as NewExpression).Constructor;
            return il.Newobj(constructorInfo);
        }

        public static IILGen Ldfld<T>(this IILGen il, Expression<Func<T>> expression)
        {
            return il.Ldfld((FieldInfo)(expression.Body as MemberExpression).Member);
        }
    }
}