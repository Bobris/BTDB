using System;

namespace BTDB.ODBLayer
{
    public static class FieldHandlerHelpers
    {
        public static bool SkipLoadIfNeeded(IFieldHandler fieldHandler, FieldHandlerLoad ctx)
        {
            if (ctx.TargetTableFieldInfo==null)
            {
                SkipLoad(fieldHandler, ctx);
                return true;
            }
            return false;
        }

        public static void SkipLoad(IFieldHandler fieldHandler, FieldHandlerLoad ctx)
        {
            fieldHandler.SkipLoad(new FieldHandlerSkipLoad { IlGenerator = ctx.IlGenerator, PushReader = ctx.PushReader });
        }
    }
}