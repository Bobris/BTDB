using System;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest;

/// The source generator's metadata and reflection must agree on the stored properties of every generated type, as
/// relations compare them and emit mode uses reflection: hidden, overridden and private base properties differed.
public class MetadataReflectionConsistencyTest
{
    [Fact]
    public void GeneratedMetadataMatchesReflectedProperties()
    {
        // Force registration of all generated metadata in this assembly.
        foreach (var t in typeof(MetadataReflectionConsistencyTest).Assembly.GetTypes())
            if (t.GetCustomAttribute<BTDB.GenerateAttribute>() != null && !t.ContainsGenericParameters)
                RuntimeHelpers.RunClassConstructor(t.TypeHandle);
        var mismatches = new System.Collections.Generic.List<string>();
        foreach (var m in ReflectionMetadata.All())
        {
            // Public fields are refused by reflection, as WithPublicField tests.
            if (m.Type.GetFields(BindingFlags.Public | BindingFlags.Instance).Length != 0) continue;
            var type = m.Type;
            // Constructed-only services (IOC), tuples and collections are not stored as objects with fields.
            if (type.IsInterface || type.IsAbstract || m.Fields.Length == 0 || type.Namespace == "System" ||
                typeof(System.Collections.IEnumerable).IsAssignableFrom(type)) continue;
            var reflected = type.GetVisibleInstanceProperties()
                .Where(p => !Attribute.IsDefined(p, typeof(NotStoredAttribute), true) && p.GetIndexParameters().Length == 0)
                .Where(p => p.CanWrite || p.GetCustomAttribute<CompilerGeneratedAttribute>() == null)
                .Select(p => p.GetCustomAttribute<BTDB.FieldHandler.PersistedNameAttribute>()?.Name ?? p.Name)
                .OrderBy(n => n, StringComparer.Ordinal).ToArray();
            var generated = m.Fields.Select(f => f.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();
            if (!reflected.SequenceEqual(generated))
                mismatches.Add(type.FullName + ": gen [" + string.Join(",", generated) + "] refl [" + string.Join(",", reflected) + "]");
        }

        Assert.True(mismatches.Count == 0, string.Join("\n", mismatches));
    }
}
