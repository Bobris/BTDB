using Xunit;

namespace BTDBTest
{
    public class RunnableInDebugOnlyAttribute : FactAttribute
    {
        public RunnableInDebugOnlyAttribute()
            : this("Only running in debug mode.")
        {
        }

        public RunnableInDebugOnlyAttribute(string reason)
        {
#if !DEBUG
            Skip = reason;
#endif
        }
    }
}