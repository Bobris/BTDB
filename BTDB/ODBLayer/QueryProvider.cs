// Code inspired by IQToolkit source code
// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace BTDB.ODBLayer
{
    class QueryProvider : IQueryProvider
    {
        readonly MidLevelDBTransaction _owner;

        internal QueryProvider(MidLevelDBTransaction owner)
        {
            _owner = owner;
        }

        IQueryable<TS> IQueryProvider.CreateQuery<TS>(Expression expression)
        {
            return new Query<TS>(this, expression);
        }

        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            Type elementType = TypeHelper.GetElementType(expression.Type);
            try
            {
                return (IQueryable)Activator.CreateInstance(typeof(Query<>).MakeGenericType(elementType), new object[] { this, expression });
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        public TS Execute<TS>(Expression expression)
        {
            return (TS)Execute(expression);
        }

        public object Execute(Expression expression)
        {
            var e = new MakeRunnableExpressionVisitor().Translate(expression);
            var @delegate = Expression.Lambda(e).Compile();
            return @delegate.DynamicInvoke();
        }

        public IQueryable<T> GetEnumerableAsQueryable<T>() where T : class
        {
            return _owner.Enumerate<T>().AsQueryable();
        }
    }
}