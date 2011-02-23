using System.Linq;
using System.Linq.Expressions;

namespace BTDB.ODBLayer
{
    class MakeRunnableExpressionVisitor : ExpressionVisitor
    {
        internal MakeRunnableExpressionVisitor()
        {
        }

        internal Expression Translate(Expression exp)
        {
            return Visit(exp);
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            var queryable = c.Value as IQueryable;
            if (queryable != null && queryable.Provider is QueryProvider)
            {
                var qp = (QueryProvider)queryable.Provider;
                var elementType = TypeHelper.GetElementType(c.Type);
                var mi = typeof(QueryProvider).GetMethod("GetEnumerableAsQueryable").MakeGenericMethod(elementType);
                return Expression.Call(Expression.Constant(qp), mi);
            }
            return base.VisitConstant(c);
        }
    }
}