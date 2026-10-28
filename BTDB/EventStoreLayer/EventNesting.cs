using System.Runtime.CompilerServices;
using BTDB.KVDBLayer;

namespace BTDB.EventStoreLayer;

static class EventNesting
{
    // Events keep references, so a cycle ends, but a long chain of objects nests as deep as it is long, and damaged
    // data deeper still: fail with an exception instead of overflowing the stack, which ends the process.
    internal static void EnsureStack(string action)
    {
        if (!RuntimeHelpers.TryEnsureSufficientExecutionStack())
            throw new BTDBException("Event objects nest too deep to " + action + ".");
    }
}
