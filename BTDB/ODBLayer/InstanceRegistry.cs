using System;
using System.Threading;

namespace BTDB.ODBLayer
{
    public class InstanceRegistry : IInstanceRegistry
    {
        volatile object[] _objects;

        public int RegisterInstance(object content)
        {
            int id;
            object[] future;
            object[] current;
            do
            {
                current = _objects;
                if (current == null)
                {
                    future = new[] { content };
                    id = 0;
                }
                else
                {
                    id = Array.IndexOf(current, content);
                    if (id >= 0) return id;
                    future = new object[current.Length + 1];
                    Array.Copy(current, future, current.Length);
                    id = current.Length;
                    future[id] = content;
                }
#pragma warning disable 420
            } while (Interlocked.CompareExchange(ref _objects, future, current) != current);
#pragma warning restore 420
            return id;
        }

        public object FindInstance(int id)
        {
            return _objects[id];
        }
    }
}