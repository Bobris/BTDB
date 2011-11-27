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
                il.Callvirt(((PropertyInfo)newExpression.Member).GetGetMethod(true));
            }
            else
            {
                var methodInfo = (expression.Body as MethodCallExpression).Method;
                il.Callvirt(methodInfo);
            }
            return il;
        }

        public static IILGen Call(this IILGen il, Expression<Action> expression)
        {
            var newExpression = expression.Body as NewExpression;
            if (newExpression != null)
            {
                il.Call(newExpression.Constructor);
            }
            else
            {
                var methodInfo = (expression.Body as MethodCallExpression).Method;
                il.Call(methodInfo);
            }
            return il;
        }

        public static IILGen Call<T>(this IILGen il, Expression<Func<T>> expression)
        {
            var newExpression = expression.Body as MemberExpression;
            if (newExpression != null)
            {
                il.Call(((PropertyInfo)newExpression.Member).GetGetMethod(true));
            }
            else
            {
                var methodInfo = (expression.Body as MethodCallExpression).Method;
                il.Call(methodInfo);
            }
            return il;
        }

        public static IILGen Newobj(this IILGen il, Expression<Action> expression)
        {
            var constructorInfo = (expression.Body as NewExpression).Constructor;
            return il.Newobj(constructorInfo);
        }
    }
}