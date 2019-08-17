using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Xunit;

namespace BTDBTest
{
    public class SkipWhenAttribute : FactAttribute
    {
        internal const string DefaultSkipReason = "Not runnable in this configuration.";

        public enum Is
        {
            Debug,
            Release,
            NetCore,
            NetFramework,
        }

        public SkipWhenAttribute(Is cond, string skip = DefaultSkipReason)
        {
            if (IsValid(cond))
                Skip = skip;
        }

        static bool IsDebug()
        {
#if DEBUG
            return true;
#else
            return false;
#endif
        }

        internal static bool IsValid(Is cond)
        {
            switch (cond)
            {
                case Is.Debug:
                    return IsDebug();
                case Is.Release:
                    return !IsDebug();
                default:
                    throw new InvalidEnumArgumentException();
            }
        }
   }

    public class SkipWhenAllAttribute : FactAttribute
    {
        public SkipWhenAllAttribute(SkipWhenAttribute.Is cond1, SkipWhenAttribute.Is cond2, string skip = SkipWhenAttribute.DefaultSkipReason)
        {
            if (SkipWhenAttribute.IsValid(cond1) && SkipWhenAttribute.IsValid(cond2))
                Skip = skip;
        }
    }

    public class SkipWhenAnyAttribute : FactAttribute
    {
        public SkipWhenAnyAttribute(SkipWhenAttribute.Is cond1, SkipWhenAttribute.Is cond2, string skip = SkipWhenAttribute.DefaultSkipReason)
        {
            if (SkipWhenAttribute.IsValid(cond1) || SkipWhenAttribute.IsValid(cond2))
                Skip = skip;
        }
    }
}