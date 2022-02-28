using System;

namespace BTDB.ODBLayer;

/// Use this attribute on class stored in database when it had some IDictionary in previous versions but does not have anymore. It will force to remove any leaks on update.
[AttributeUsage(AttributeTargets.Class)]
public class RequireContentFreeAttribute : Attribute {}
