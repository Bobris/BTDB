using System;

namespace BTDB;

[AttributeUsage(AttributeTargets.Interface | AttributeTargets.Class | AttributeTargets.Delegate)]
public class GenerateAttribute : Attribute
{
}
