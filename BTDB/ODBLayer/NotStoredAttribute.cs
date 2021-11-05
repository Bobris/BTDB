using System;

namespace BTDB.ODBLayer;

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class NotStoredAttribute : Attribute
{
}
