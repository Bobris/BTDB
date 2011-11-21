using System;
using System.Linq.Expressions;

namespace BTDB.IL
{
    public static class ILGenExtensions
    {
        public static IILGen Callvirt(this IILGen il, Expression<Action> expression)
        {
            var methodInfo = (expression.Body as MethodCallExpression).Method;
            return il.Callvirt(methodInfo);
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

        public static IILGen Newobj(this IILGen il, Expression<Action> expression)
        {
            var constructorInfo = (expression.Body as NewExpression).Constructor;
            return il.Newobj(constructorInfo);
        }
    }
}