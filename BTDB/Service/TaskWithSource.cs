using System.Threading.Tasks;

namespace BTDB.Service
{
    public struct TaskWithSource
    {
        public readonly object Source;
        public readonly Task Task;

        public TaskWithSource(object source, Task task)
        {
            Source = source;
            Task = task;
        }
    }
}